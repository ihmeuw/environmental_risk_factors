
#clear memory
rm(list=ls())

library("data.table")
library("dplyr")
source("FILEPATH/csv2objects/csv2objects.R")
source("FILEPATH/submitExperiment.R")


#define args:
config.path    <- FILEPATH
config.version <- "config20210706"
config.file <- file.path(config.path, paste0(config.version, ".csv"))

csv2objects(config.file, exclude = ls())

popType <- "annual" # "fixed" or "annual"

missingOnly <- T

user <- USERNAME

version <- config.version

# set up run environment
project <- PROJECT
sge.output.dir <- paste0(" -o FILEPATH", user, "/output -e FILEPATH", user, "/errors ")
r.shell <- FILEPATH
slots <- 8
mem <- "100G"
save.script <- paste0("-s ",  "FILEPATH/rrMaxCalc.R")

outdir <- paste0(FILEPATH, version)
dir.create(outdir)
dir.create(paste0(outdir, "/rrMax"))



for (cause in causeList) {
  for (zone in 6:28) {
    
    if (missingOnly==F | file.exists(paste0(FILEPATH, "/rrMaxDraws_", cause, "_zone", zone, ".csv"))==F) {
      args <- paste(cause, version, popType, zone, rrDir, tmrelDir, description)
      
      jname <- paste("rrmax", cause, zone, sep = "_")
    
      # Create submission call
      sys.sub <- paste0("qsub -l archive -l m_mem_free=", mem, " -l fthread=", slots, " -P ", project, " -q long.q", sge.output.dir, " -N ", jname)
      
      # Run
      system(paste(sys.sub, r.shell, save.script, args))
      Sys.sleep(0.01)
    }
  }
}



status <- data.table()
for (cause in causeList) {
  for (zone in 6:28) {
    status <- rbind(status, data.table(cause = cause, zone = zone, 
                                       status = file.exists(paste0(FILEPATH, "/rrMaxDraws_", cause, "_zone", zone, ".csv"))))
  }
}

table(status$zone, status$status)


