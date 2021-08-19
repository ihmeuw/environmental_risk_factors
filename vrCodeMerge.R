rm(list = ls())

library("RODBC")
library("data.table")

if (Sys.info()["sysname"]=="Windows") {
  h <- "DRIVE"
  j <- "DRIVE"
  k <- "DRIVE"
} else {
  h <- "DRIVE"
  j <- "DRIVE"
  k <- "DRIVE"
}

ihme_loc_id <- ISO3

# pull in age/sex split VR data file produced by engine room code (python)
vr <- fread(paste0("FILEPATH/", ihme_loc_id, "_split_noCodes.csv"))

# pull in icd codes that correspond to code_id's for each code system in the vr data
engine <- odbcConnect(DATABASE)
codes <- sqlQuery(engine, paste("SELECT code_system_id, code_id, value FROM maps_code WHERE code_system_id IN (", paste(unique(vr$code_system_id), collapse = ","), ")"), stringsAsFactors = F)
setDT(codes)

# merge VR data and codes, and clean up icd codes
vr <- merge(vr, codes, by = c("code_system_id", "code_id"), all.x = T)



# convert age groups to old-style age varible (approx=stating age)
convert_age = function(age){
  if (age > 95){return(95)}
  if (age >= 5){return((age %/% 5) * 5)}
  if (age < .01){return(0)}
  if (age < .1){return(.01)}
  if (age < 1){return(.1)}
  return(1)
}

age_groups <- data.table(age_group_id = c(2:20,30:32, 235), 
                         age_group_years_start = c(0, 0.019, 0.077, 1, seq(5, 95, 5)), 
                         age_group_years_end = c(0.019, 0.077, 1, seq(5, 100, 5)))[, age_mid := (age_group_years_start + age_group_years_end)/2]

age_groups <- age_groups[, age := sapply(age_mid, convert_age)][, .(age_group_id, age)] 

vr <- merge(vr, age_groups, by = "age_group_id", all.x = T)

# clean up the file and export
vr <- vr[, .(location_id, year_id, sex_id, age, code_system_id, value, deaths)]

vr <- vr[, sum(deaths), by = .(location_id, year_id, sex_id, age, code_system_id, value)]
setnames(vr, "V1", "deaths")

write.csv(vr, paste0("FILEPATH/", ihme_loc_id, "_split_codes.csv"), row.names = F)


