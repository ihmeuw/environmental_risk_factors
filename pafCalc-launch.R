
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

user <- USERNAME
gbd_round_id <- 7

config.path    <- FILEPATH
config.version <- "config20210706"
config.file <- file.path(config.path, paste0(config.version, ".csv"))
csv2objects(config.file, exclude = ls())

### GET LOCATION META-DATA ###
outRoot <- FILEPATH
outVersion <- "version_13.0"
outdir <- paste0(outRoot, outVersion)

locSet <- 35
locMeta <- get_location_metadata(location_set_id = locSet, gbd_round_id = gbd_round_id)
loc_ids <- locMeta[(is_estimate==1 & most_detailed==1) | level==3 | level==0, location_id]
loc_ids <- loc_ids[order(loc_ids)]

project <- PROJECT
calcSevs <- T

dir.create(outdir, recursive = T)
dir.create(paste0(outdir, "/heat"))
dir.create(paste0(outdir, "/cold"))
dir.create(paste0(outdir, "/sevs"))

arg.list <- list(loc_id = loc_ids, year = yearList, outdir = outdir, proj = proj, 
                 scenario = scenario, config.file = config.file, calcSevs = calcSevs)
name.stub <- "paf"
name.args <- c("loc_id", "year")


# set up run environment
sge.output.dir <- paste0(" -o FILEPATH", user, "/output -e FILEPATH", user, "/errors ")
r.shell <- FILEPATH
save.script <- FILEPATH


mem <- "50G"
slots <- 8

arg.table <- launch.jobs(arg.list = arg.list, name.stub = name.stub, name.args = name.args, outfile.suffix = ".csv",
                         pause = 1, max.attempts = 1, return = T, relaunch = F)


status <- data.table()
for (loc in loc_ids) {
  print(loc)
  for (y in yearList) {
    cat(".")
    status <- rbind(status,
                    data.table(location_id = loc, year_id = y,
                               heat = file.exists(paste0(outdir, "/heat/", loc, "_", y, ".csv")),
                               cold = file.exists(paste0(outdir, "/cold/", loc, "_", y, ".csv")),
                               sevs = file.exists(paste0(outdir, "/sevs/", loc, "_", y, ".csv"))))
  }
}

status <- merge(status, locMeta, by = "location_id", all.x = T)

table(status$heat, status$level)
table(status$year_id, status$complete)
table(status$location_id, status$complete)


paf.version    <- "13.0"
input_dir.heat <- paste0("FILEPATH/version_", paf.version, "/heat")
input_dir.cold <- paste0("FILEPATH/version_", paf.version, "/cold")
mei.heat       <- 20263
mei.cold       <- 20262
years          <- 1990:2020
gbd_round_id   <- 7
step           <- STEP

save_results_risk(input_dir = input_dir.heat,
                  input_file_pattern = "{location_id}_{year_id}.csv",
                  modelable_entity_id = mei.heat,
                  year_id = years,
                  n_draws=1000,
                  description = paste0("HEAT PAFs: ", paf.version),
                  risk_type = "paf",
                  measure = 4,
                  mark_best = T,
                  gbd_round_id = gbd_round_id,
                  decomp_step = step)

save_results_risk(input_dir = input_dir.cold,
                  input_file_pattern = "{location_id}_{year_id}.csv",
                  modelable_entity_id = mei.cold,
                  year_id = years,
                  n_draws=1000,
                  description = paste0("COLD PAFs: ", paf.version),
                  risk_type = "paf",
                  measure = 4,
                  mark_best = T,
                  gbd_round_id = gbd_round_id,
                  decomp_step = step)