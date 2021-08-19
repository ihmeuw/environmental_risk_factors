rm(list=ls())

library("data.table")
library("matrixStats")


j <- FILEPATH



source("FILEPATH/get_outputs.R")
source("FILEPATH/get_draws.R")
source("FILEPATH/get_location_metadata.R")
source("FILEPATH/get_cause_metadata.R")
source("FILEPATH/csv2objects.R")


# set demographics for which to build redistribution table
arg <- commandArgs()[-(1:5)]  # First args are for unix use only
loc_id <- as.integer(arg[1])
cause_list <- arg[2]
acause_list <- arg[3]
outdir <- arg[4]
indir <- arg[5]
tmrel_min <- as.numeric(arg[6])
tmrel_max <- as.numeric(arg[7])
year_list <- arg[8]
config.file <- arg[9]
job.name <- arg[10]

cause_list <- as.integer(strsplit(cause_list, split = ",")[[1]])
acause_list <- strsplit(acause_list, split = ",")[[1]]
cause_link <- data.table(cause_id = cause_list, acause = acause_list)
year_list <- as.integer(strsplit(year_list, split = ",")[[1]])

csv2objects(config.file, exclude = ls())




### IMPORT COD ESTIMATES FOR SELECTED CAUSES ###

if (cod_source==("get_draws")) {
  cod <- get_draws("cause_id", cause_list, location_id = loc_id, metric_id = 1, measure_id = 1, age_group_id = 22, sex_id = 3, year_id = year_list,
                    source="codcorrect", version = codcorrect_version, gbd_round_id = gbd_round_id, decomp_step = codcorrect_step)

  cod <- cod[, c("age_group_id", "sex_id", "measure_id", "metric_id") := NULL]
  warning("CoD data loaded")

} else {
  cod <- fread(cod_source)[location_id==loc_id & year_id %in% year_list & acause %in% acause_list, ]
  cod <- merge(cod, cause_link, by = "acause", all.x = T)

  warning("CoD data loaded")
}

maxDraw <- length(grep("draw_", names(cod)))-1





# Convert deaths to weights
cod[, paste0("weight_", 0:maxDraw) := lapply(.SD, function(x) {x/sum(x, na.rm = T)}), by = .(location_id, year_id), .SDcols = paste0("draw_", 0:maxDraw)]
cod[, paste0("draw_", 0:maxDraw) := NULL]


warning("CoD data converted to weights")

### IMPORT RR DRAWS FROM MR-BRT MODELS ###
rr <- do.call(rbind, lapply(acause_list, function(acause) {
  cbind(fread(paste0(indir, "/", acause, "/", acause, "_curve_samples.csv")), acause)}))

setnames(rr, "annual_temperature", "meanTempCat")
setnames(rr, "daily_temperature", "dailyTempCat")
rr <- rr[, .SD, .SDcols = c("meanTempCat", "dailyTempCat", "acause", paste0("draw_", 0:maxDraw))]

warning("RR data loaded")

startSize <- nrow(rr)
rr <- rr[dailyTempCat>=tmrel_min & dailyTempCat<=tmrel_max & meanTempCat>=6, ]
endSize <- nrow(rr)

warning(paste0("RR data trimmed to temp limits, from ", startSize, " to ", endSize))

rr[, paste0("rr_", 0:maxDraw) := lapply(.SD, exp), .SDcols = c(paste0("draw_", 0:maxDraw))]
rr[, "rrMean" := apply(.SD, 1, mean), .SDcols=paste0("draw_", 0:maxDraw)]


## clean up (drop unnecessary varibles and rename others)
rr[, c(paste0("draw_", 0:maxDraw)) := NULL]
rr[, meanTempCat := as.integer(meanTempCat)]
rr[, acause := as.character(acause)]
rr[, dailyTempCat := as.integer(round(dailyTempCat*10))]

# merge in cause ids
rr <- merge(rr, cause_link, by = "acause", all.x = T)

warning("RRs merged to cause ids")

### MERGE COD & RR DATA ###
master <- merge(cod, rr, by = "cause_id", all = TRUE, allow.cartesian = TRUE)[is.na(location_id)==F, ]
warning("RRs merged to CoD Data")




### CALCULATE DEATH_WEIGHTED MEAN RRs ###
master[, id := .GRP, by = .(location_id, year_id, dailyTempCat, meanTempCat)]
setkey(master, id)

ids <- unique(master[, .(id, location_id, year_id, dailyTempCat, meanTempCat)])

rrWt <- master[, lapply(0:maxDraw, function(x) {sum(get(paste0("rr_", x)) * get(paste0("weight_", x)))}), by = .(id)]
setnames(rrWt, paste0("V", (0:maxDraw)+1), paste0("rr_", 0:maxDraw))


rrWt <- merge(ids, rrWt, by = "id", all = TRUE)
setkey(rrWt, id)

rrWt[, dailyTempCat := as.numeric(dailyTempCat/10)]

warning("Death weighted RRs calculated")


tmrel <- rrWt[, lapply(.SD, function(x) {sum(dailyTempCat*(x==min(x)))}), by = c("location_id", "year_id", "meanTempCat"), .SDcols = paste0("rr_", 0:maxDraw)]
names(tmrel) <- sub("rr_", "tmrel_", names(tmrel))

warning("TMRELs calculated")


write.csv(tmrel, file = paste0(outdir, "/", job.name, ".csv"), row.names = F)
warning("Draw file saved")


tmrel[, "tmrelMean" := apply(.SD, 1, mean), .SDcols=paste0("tmrel_", 0:maxDraw)]
tmrel[, "tmrelLower" := apply(.SD, 1, quantile, c(.025)), .SDcols=paste0("tmrel_", 0:maxDraw)]
tmrel[, "tmrelUpper" := apply(.SD, 1, quantile, c(.975)), .SDcols=paste0("tmrel_", 0:maxDraw)]
tmrel[, paste0("tmrel_", 0:maxDraw) := NULL]

write.csv(tmrel, file = paste0(outdir, "/", job.name, "_summaries.csv"), row.names = F)
warning("Summary file saved")


write("Complete", paste0(outdir, "/", job.name, "_complete.txt"))
