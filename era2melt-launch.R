
#clear memory
rm(list=ls())

library("data.table")
library("ggplot2")
source("FILEPATH/get_location_metadata.R")
source("FILEPATH/get_cause_metadata.R")
source("FILEPATH/csv2objects/csv2objects.R")
source("FILEPATH/submitExperiment.R")




#define args:
yearList <- 1990:2020
outdir <- FILEPATH 
user <- USERNAME
gbd_round_id <- 7


### GET LOCATION META-DATA ###
locMeta <- get_location_metadata(location_set_id = 35, gbd_round_id = gbd_round_id)
loc_ids <- locMeta[(is_estimate==1 & most_detailed==1) | level==3 , location_id]
loc_ids <- loc_ids[order(loc_ids)]



arg.list <- list(loc_id = loc_ids, year = yearList, outdir = outdir)
name.stub <- "melt"
name.args <- c("loc_id", "year")

# set up run environment
project <- PROJECT
sge.output.dir <- paste0(" -o FILEPATH", user, "/output -e FILEPATH", user, "/errors ")
r.shell <- FILEPATH
save.script <- FILEPATH


mem <- "150G"
slots <- 8


arg.table <- launch.jobs(arg.list = arg.list, name.stub = name.stub, name.args = name.args, outfile.suffix = ".csv",
                         pause = 1, max.attempts = 1, return = T, relaunch = T)



status <- data.table()
for (loc in loc_ids) {
  print(loc)
  for (y in yearList) {
    cat(".")
    status <- rbind(status, 
                    data.table(location_id = loc, year_id = y, 
                               complete = file.exists(paste0(outdir, "/melt_", loc, "_", y, ".csv"))))
  }
}
