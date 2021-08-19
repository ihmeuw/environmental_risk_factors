rm(list=ls())

library(raster)
library(data.table)
library(rgdal)
library(zoo)
library(parallel)
library(Rfast)




arg <- commandArgs(trailingOnly = T)  # First args are for unix use only
loc_id <- arg[1]
year <- arg[2]
outDir <- arg[3]
job.name <- arg[4]


maxDraw <- 999
debug <- F
gbd_round_id <- 6

warning(paste0("Location   = ", loc_id))
warning(paste0("Year       = ", year))
warning(paste0("Output dir = ", outDir))
warning(paste0("Job name   = ", job.name))
warning(paste0("Draws      = ", maxDraw))



### READ IN SHAPEFILE ###

start <- Sys.time()

if (gbd_round_id==7) {
  shapefile.dir <- file.path(FILEPATH)
  shapefile.version <- "GBD2020_analysis_final"
} else if (gbd_round_id==6) {
  shapefile.dir <- file.path(FILEPATH)
  shapefile.version <- "GBD2019_analysis_final"
}

shape <- readOGR(dsn = shapefile.dir, layer = shapefile.version)

end <- Sys.time()
(shpReadTime <- difftime(end, start))

warning("Shapefile loaded")

if (loc_id==1) {
  sub_shape <- shape
} else {
  sub_shape <- shape[shape@data$loc_id == loc_id,] ### choose location
}
warning("Shapefile subsetted")



### POPULATION AND TEMPERATURE DATA ARE NOT AVAILABLE FOR ALL YEARS -- SUB WITH CLOSEST YEAR IF NEEDED ###
if (year > 2020) {
  popYear <- 2018
  tempYear <- 2020
} else if (year >= 2019) {
  popYear <- 2018
  tempYear <- year
} else {
  popYear <- year
  tempYear <- year
}


### READ IN POP, TEMP, TEMP SD, & TEMP ZONE DATA ###

start <- Sys.time()

pop           <- fread(FILEPATH)
dailyTempCat  <- brick(FILEPATH, overwrite = TRUE, format = "raster")
sd            <- brick(FILEPATH, overwrite = TRUE, format = "raster")
meanTempCat   <- raster(FILEPATH)
warning("Rasters loaded")


# CONVERT POP FROM XY TO RASTER #
pop <- rotate(rasterFromXYZ(pop[,c("longitude","latitude","population")]))
warning("Population converted to raster")


# ENSURE ALL RASTERS HAVE SAME PROJECTION #
crs(pop)   <- "+proj=longlat +datum=WGS84 +no_defs +ellps=WGS84 +towgs84=0,0,0" 
crs(dailyTempCat)  <- "+proj=longlat +datum=WGS84 +no_defs +ellps=WGS84 +towgs84=0,0,0" 
crs(sd)    <- "+proj=longlat +datum=WGS84 +no_defs +ellps=WGS84 +towgs84=0,0,0" 
crs(meanTempCat) <- "+proj=longlat +datum=WGS84 +no_defs +ellps=WGS84 +towgs84=0,0,0" 
warning("Rasters reprojected")

# ENSURE ALL RASTERS HAVE SAME PIXEL SIZE AND ALIGNMENT #
pop <- raster::resample(pop, dailyTempCat, method = "bilinear")
sd <- raster::resample(sd, dailyTempCat, method = "bilinear")
meanTempCat <- raster::resample(meanTempCat, dailyTempCat, method = "bilinear")
warning("Rasters resampled")

end <- Sys.time()
(dataReadTime <- difftime(end, start))


### DEFINE FUNCTION TO EXTRACT THE DATA FROM PIXELS IN LOCATION AND FORMAT FOR MERGE ###
extract2shape <- function(x, shape, melt = FALSE, mask = TRUE, crop = TRUE) {
  if (crop==TRUE) {
    c <- crop(eval(parse(text = x)), shape)
  } else {
    c <- eval(parse(text = x))
  }
  
  if (mask==TRUE) {
    c <- raster::mask(c, shape)
  }
  
  dt <- as.data.table(rasterToPoints(c, spatial = TRUE))
  
  if (melt==TRUE) {
    dt <- melt(data = dt, id.vars = c("x","y"))
    setnames(dt, "variable", "layer")
    setnames(dt, "value", x)
  } else {
    names(dt)[1] <- x
  }
  
  dt[, `:=` (x = round(x, digits = 2), y = round(y, digits = 2))]
  
  return(dt)
}

warning("Extract function defined")  


### EXTRACT DATA FROM RASTERS AND CREATE DATA TABLE ###  
start <- Sys.time()

# FOR VERY SMALL LOCATIONS WE MAY LOSE ALL PIXELS IF WE MASK TO THE EXACT LOCATION POLYGON, FOR THESE LOCS WE'LL BUFFER THE POLYGON #
# TEST FOR THAT HERE
if (loc_id==1) {
  crop <- F
  mask <- F

} else {
  crop <- T
  mask <- T
  
  testPixels <- raster::mask(crop(pop, sub_shape), sub_shape)
  warning("Test pixels extracted")

  testVals <- as.numeric(values(testPixels))
  warning("Test values extracted")
  

  while (sum(testVals, na.rm = T)==0) {
    warning("Zero population found in the polygon. Buffering.")
    sub_shape <- buffer(sub_shape, 1)
    testPixels <- raster::mask(crop(pop, sub_shape), sub_shape)
    testVals <- as.numeric(values(testPixels))
  }

  rm(testVals, testPixels)
}

warning("Pixel test completed")



# EXTRACT DATA #  
for (x in c("pop", "dailyTempCat", "sd", "meanTempCat")) {
  if (x %in% c("dailyTempCat", "sd")) {
    melt = T
  } else {
    melt = F
  }
  assign(x, extract2shape(x, sub_shape, melt = melt, mask = mask))
}
warning("Rasters extracted to data tables")


### CLEAN AND FIND UNIQUE VALUES OF TEMPERATURE ZONE ###
meanTempCat[, meanTempCat  := as.integer(round(meanTempCat-273.15, digits = 0))]
zones <- unique(meanTempCat$meanTempCat)


### MERGE COMPONENTS ###
master <- merge(pop, meanTempCat,  by=c("x","y"), all = TRUE)
if (debug==F) rm(pop, meanTempCat)
master <- merge(master, dailyTempCat, by=c("x","y"), all = TRUE)
if (debug==F) rm(dailyTempCat)
master <- merge(master, sd, by=c("x", "y", "layer"), all = TRUE)
if (debug==F) rm(sd)

warning("Components merged")

### WARN IF THERE ARE MISSING VALUES OF ANY NEEDED VARIABLES ###    
if (sum(is.na(master$pop))>0) warning("Population values are missing for some pixels")
if (sum(is.na(master$dailyTempCat))>0) warning("Daily temperature values are missing for some pixels")
if (sum(is.na(master$meanTempCat))>0) warning("Temperature zone values are missing for some pixels")
if (sum(is.na(master$sd))>0) warning("Temperature SD values are missing for some pixels")


### ENSURE THAT POPULATION VALUES ARE NOT ALL NA OR ZERO (E.G. MISASLIGNMENT WITH SMALL ISLAND)
# If all values of population are missing replace with one (this should never happen, but is from an older code version and retained for insurance/neurosis)
if (sum(!is.na(master$pop))==0) {
  master[, pop := 1]
  
  # If all values of population sum to < 1, replace population with 1 to avoid floating point precision issues
} else if (sum(master$pop, na.rm=TRUE) < 1) {
  master[, pop := 1]
  
  # if there are missing population pixels fill them through interpolation  
} else if (sum(is.na(master$pop)) > 0) {
  master[, pop := na.approx(pop, na.rm=FALSE), by="layer"]
} 
warning("Missing and zero populations resolved")

# Drop any pixels with zero or missing populations #
master <- master[pop > 0 & is.na(pop)==F, ]

if (debug==T) masterBkup <- copy(master)




### CONVERT TEMP VARIABLES AND CREATE TEMPERATURE DRAWS ###
master[, dailyTempCat := 10 * (dailyTempCat - 273.1)]
master[, sd := 10 * sd]
master <- master[, .SD, .SDcols = c("meanTempCat", "dailyTempCat", "sd", "pop")]
setkeyv(master, c("meanTempCat", "dailyTempCat"))
  
wideStart <- Sys.time()

  
### CREATE DRAWS AND COLLAPSE POPULATION BY DAILY TEMP AND TEMP ZONE ###
  collapse.draws <- function(draw) { 
    longTemp <- copy(master)[, dailyTempCat := as.integer(round(sd * Rnorm(.N, 0, 1) + dailyTempCat))]
    
    longTemp <- longTemp[, lapply(.SD, sum), by = .(meanTempCat, dailyTempCat), .SDcols = "pop"] 
    longTemp[, draw := as.integer(draw)]
    
    return(longTemp)
    }

  long <- do.call(rbind, mclapply(0:maxDraw, collapse.draws))
  
  wideEnd <- Sys.time()
  difftime(wideEnd, wideStart)


warning("Reshape to long & collapse completed for all zones")

# Save the output #
write.csv(long, file = paste0(outDir, "/", job.name, ".csv"), row.names = F)

end <- Sys.time()
(locProcessTime <- difftime(end, start))
warning("File exported -- done")

shpReadTime
dataReadTime
locProcessTime




