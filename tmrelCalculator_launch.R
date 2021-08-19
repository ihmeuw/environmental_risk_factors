
#clear memory
rm(list=ls())

library("data.table")
library("ggplot2")
source("FILEPATH/get_location_metadata.R")
source("FILEPATH/get_cause_metadata.R")
source("FILEPATH/csv2objects/csv2objects.R")
source("FILEPATH/submitExperiment.R")




#define args:
tmrel_min <- 6.6
tmrel_max <- 34.6
lTrim <- 0.05
uTrim <- 0.95
yearList <- 1990:2100

config.path    <- "FILEPATH"
config.version <- "config20210706"

config.file <- file.path(config.path, paste0(config.version, ".csv"))

csv2objects(config.file, exclude = ls())

indir <- paste0(rrRootDir, rrVersion, "/")

runDate <- as.character(format(Sys.Date(), "%Y%m%d"))
description <- paste0(rrVersion, "_", project, "_", runDate)

outdir <- file.path(tmrelRootDir, paste0(config.version, "_", runDate))
dir.create(outdir, recursive = T)

update.config(update = rbind(data.frame(object = "description", value = description, format = "str"),
                             data.frame(object = "tmrelDir", value = outdir, format = "str"),
                             data.frame(object = "rrDir", value = indir, format = "str")),
              in.file = config.file, replace = T)


### GET CAUSE META-DATA ###
causeMeta <- get_cause_metadata(cause_set_id=4, gbd_round_id=gbd_round_id)
causeMeta <- causeMeta[acause %in% causeList & level==3, ][, .(cause_id, cause_name, acause)]

cause_list <- paste(causeMeta$cause_id, collapse = ",")
acause_list <- paste(causeMeta$acause, collapse = ",")

### GET LOCATION META-DATA ###

if (project=="gbd") {
  locMeta <- get_location_metadata(location_set_id = 35, gbd_round_id = gbd_round_id)
  loc_ids <- locMeta[is_estimate==1 | level<=3, location_id]
  loc_ids <- loc_ids[order(loc_ids)]

  yearList <- paste(yearList, collapse = ",")
  name.args <- "loc_id"
} else {
  cod <- fread(cod_source)
  loc_ids <- unique(cod$location_id)
  loc_ids <- loc_ids[order(loc_ids)]

  name.args <- c("loc_id", "year")
}



arg.list <- list(loc_id = loc_ids, cause_list = cause_list, acause_list = acause_list, outdir = outdir, indir = indir,
                 tmrel_min = tmrel_min, tmrel_max = tmrel_max, year = yearList, config = config.file)


name.stub <- "tmrel"


# set up run environment
project <- PROJECT
sge.output.dir <- paste0(" -o FILEPATH/", user, "/output -e FILEPATH/", user, "/errors ")
r.shell <- "FILEPATH"
save.script <- "FILEPATH/tmrelCalculator.R"


if (length(name.args)==1) {
  mem <- "250G"
  slots <- 8
} else {
  mem <- "25G"
  slots <- 4
}


arg.table <- launch.jobs(arg.list = arg.list, name.stub = name.stub, name.args = name.args, outfile.suffix = "_summaries.csv",
                         pause = 1, max.attempts = 1, return = T)



