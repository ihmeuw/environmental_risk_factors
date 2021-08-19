
rm(list=ls())

library("dplyr") 
library("data.table")
library("feather")


source("FILEPATH/get_outputs.R")
source("FILEPATH/get_draws.R")
source("FILEPATH/get_location_metadata.R")
source("FILEPATH/get_population.R")
source("FILEPATH/get_cause_metadata.R")



### GET LIST OF INCLUDED CAUSES ###
arg <- commandArgs(trailingOnly = T)  # First args are for unix use only
causeList <- arg[1]
version <- arg[2]
popType <- arg[3]
tempZone <- arg[4]
rrDir <- arg[5]
tmrelDir <- arg[6]
description <- arg[7]

for (a in c("causeList", "version", "popType", "tempZone", "rrDir", "description")) {
  warning(paste0(a, ": ", eval(parse(text = a))))
}


gbd_round_id <- 7

yearList <- 1990:2021

outdir <- FILEPATH
tmreldir <- tmrelDir



### GET CAUSE AND LOCATION META-DATA ###
causeMeta <- get_cause_metadata(cause_set_id=4, gbd_round_id=gbd_round_id)
causeMeta <- causeMeta %>% filter(acause %in% causeList & level==3) %>% dplyr::select(cause_id, cause_name, acause, sort_order)

cause_list <- causeMeta$cause_id
acause_list <- causeMeta$acause
cause_link <- data.table(cause_id = cause_list, acause = acause_list)


### GET LOCATION METADATA ###
locMeta <- get_location_metadata(location_set_id = 35, gbd_round_id=gbd_round_id)



### IMPORT RR DRAWS FROM MR-BRT MODELS ###

rrRaw <- do.call(rbind, lapply(acause_list, function(acause) {
  cbind(fread(paste0(rrDir, "/", acause, "/", acause, "_curve_samples.csv")), acause)}))


setnames(rrRaw, "annual_temperature", "meanTempCat")

rrRaw <- rrRaw[meanTempCat==tempZone, ]
rrRaw <- rrRaw[acause %in% causeList,]

rrRaw[, dailyTempCat := as.integer(round(daily_temperature*10))][, daily_temperature := NULL]
rrRaw <- rrRaw[, lapply(.SD, mean), by = c("meanTempCat", "acause", "dailyTempCat"), .SDcols = paste0("draw_", 0:999)]

rrRaw[, paste0("draw_", 0:999) := lapply(.SD, exp), .SDcols = paste0("draw_", 0:999)]

rrRaw[, id := 1:.N]
rrRaw[, "rrMean"  := apply(.SD, 1, mean), .SDcols=paste0("draw_", 0:999)]
rrRaw[, "rrLower" := apply(.SD, 1, quantile, c(.025)), .SDcols=paste0("draw_", 0:999)]
rrRaw[, "rrUpper" := apply(.SD, 1, quantile, c(.975)), .SDcols=paste0("draw_", 0:999)]



rrClean <- copy(rrRaw)
rrClean[, paste0("draw_", 0:999) := NULL]


tempLimits <- unique(rrClean[, c("meanTempCat", "dailyTempCat")])
tempLimits <- unique(tempLimits[, `:=` (minTemp = min(dailyTempCat), maxTemp = max(dailyTempCat)), by = c("meanTempCat")][, dailyTempCat := NULL])


### IMPORT TMRELS ###
tmrel <- do.call(rbind, lapply(locMeta[most_detailed==1, location_id], function(loc_id) {fread(paste0(tmreldir, "/tmrel_", loc_id, "_summaries.csv"))}))
tmrel <- tmrel[year_id %in% yearList,]
tmrel <- tmrel[, tmrel := as.integer(round(tmrelMean*10))][, c("tmrelMean", "tmrelLower", "tmrelUpper") := NULL]


### MERGE RRs & TMRELs, and adjust RRs ###
rrClean <- merge(rrClean, tmrel, by = "meanTempCat", all.x = T, allow.cartesian = T)
rrClean[, refRR := sum(rrMean * (dailyTempCat==tmrel)), by = c("meanTempCat", "acause", "year_id", "location_id")]
rrClean[refRR==0, refRR := sum(rrMean * (dailyTempCat==(tmrel+1))), by = c("meanTempCat", "acause",  "year_id", "location_id")]

rrClean[, `:=` (rrMean = rrMean/refRR, rrLower = rrLower/refRR, rrUpper = rrUpper/refRR)]
rrClean[dailyTempCat < tmrel, risk := "cold"][dailyTempCat > tmrel, risk := "heat"]
rrClean[, c("tmrel", "refRR") := NULL]

temps <- do.call(rbind, lapply(yearList, function(y) {read_feather(paste0(FILEPATH, "/tempCollapsed_", y, "_", tempZone, ".feather"))}))
setDT(temps)
temps[, year_id := as.integer(year_id)]

temps <- merge(temps, tempLimits, by = "meanTempCat", all.x = T)
temps[dailyTempCat<minTemp, dailyTempCat := minTemp][dailyTempCat>maxTemp, dailyTempCat := maxTemp]
temps[, c("minTemp", "maxTemp") := NULL]


master <- merge(temps, rrClean, by = c("meanTempCat", "dailyTempCat", "year_id", "location_id"), all.x = T, allow.cartesian = T)

### FIND RRmax ###
master <- master[is.na(rrMean)==F, ]

masterBkup <- copy(master)
master <- copy(masterBkup)

master <- master[order(meanTempCat, acause, rrMean), ]
master[, pct := cumsum(population) / sum(population), by = .(meanTempCat, acause)]
master[, pctDif := abs(pct-0.99)]
master[, isRrMax := pctDif==min(pctDif), by = .(meanTempCat, acause)]

rrmax <- master[isRrMax==1, ][, .SD, .SDcols = c("meanTempCat", "acause", "id", "rrMean")][, risk := "all"]

master <- master[order(meanTempCat, acause, risk, rrMean), ]
master[, pct := cumsum(population) / sum(population), by = .(meanTempCat, acause, risk)]
master[, pctDif := abs(pct-0.99)]
master[, isRrMax := pctDif==min(pctDif), by = .(meanTempCat, acause, risk)]

rrmax <- rbind(rrmax, master[isRrMax==1 & is.na(risk)==F, ][, .SD, .SDcols = c("meanTempCat", "acause", "id", "rrMean", "risk")])
setnames(rrmax, "rrMean", "rrMaxMean")

rrmax[, index := 1:.N, by = c("meanTempCat", "acause", "risk")]
rrmax <- rrmax[index==1, ]

rrMaxRows <- merge(rrmax, rrRaw, by = c("id", "meanTempCat"), all.x =T)
rrMaxRows[, paste0("draw_", 0:999) := lapply(.SD, function(x) {x * rrMaxMean / rrMean}), .SDcols = paste0("draw_", 0:999)]
rrMaxRows <- rrMaxRows[, .SD, .SDcols = c("meanTempCat", "risk", "rrMaxMean", paste0("draw_", 0:999))]


names(rrMaxRows) <- gsub("^draw_", "rrMax_", names(rrMaxRows))

write.csv(rrMaxRows, file = paste0(outdir, "rrMax/rrMaxDraws_", causeList, "_zone", tempZone, ".csv"), row.names = F)


