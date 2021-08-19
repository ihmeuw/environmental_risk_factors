#----HEADER----------------------------------------------------------------------------------------------------------------------
# Author: Kate Causey
# Date: 4/21/2018
# Purpose: Launches save results for air_pm, air_hap and air, new proportional pafs method
# source("/homes/swozniak/air_pollution/air/save_results_parent.R", echo=T)
# qsub -N save_air -pe multi_slot 1 -P proj_custom_models -o /share/temp/sgeoutput/kcausey/output -e /share/temp/sgeoutput/kcausey/errors /ihme/singularity-images/health_fin/forecasting/shells/health_fin_forecasting_shell_singularity.sh /homes/kcausey/code/air/save_results_parent.R 
#*********************************************************************************************************************************

#----CONFIG----------------------------------------------------------------------------------------------------------------------
# clear memory
rm(list=ls())

user <- 'swozniak'

# runtime configuration
if (Sys.info()["sysname"] == "Linux") {
  j_root <- "/home/j/"
  h_root <- "~/"
  central_lib <- "/ihme/cc_resources/libraries/"
} else {
  j_root <- "J:/"
  h_root <- "H:/"
  central_lib <- "K:/libraries/"
}


pacman::p_load(data.table, magrittr)

project <- "-P proj_erf "
#project <- ""
sge.output.dir <- paste0(" -o /share/temp/sgeoutput/", user, "/output -e /share/temp/sgeoutput/", user, "/errors ")
#sge.output.dir <- paste0(" -o /home/j/temp/", user, "/output -e /home/j/temp/", user, "/errors ")

#sge.output.dir <- "" # toggle to run with no output files


save.script <- "-s /homes/swozniak/air_pollution/air/save_results.R"
r.shell <- "/ihme/singularity-images/rstudio/shells/execRscript.sh"
#r.shell <- "/ihme/singularity-images/health_fin/forecasting/shells/health_fin_forecasting_shell_singularity.sh"

paf.version <- 80
decomp <- "iterative"

#create datatable of unique jobs
risks <- data.table(risk=c("air_pmhap","air_pm","air_hap"), me_id=c(20260,8746,8747), rei_id=c(380,86,87))

save <- function(i){

  args <- paste(risks[i,risk],
                risks[i,me_id],
                risks[i,rei_id],
                paf.version,
                decomp)
  mem <- "-l m_mem_free=75G"
  fthread <- "-l fthread=10"
  runtime <- "-l h_rt=24:00:00"
  archive <- "-l archive=TRUE" 
  jname <- paste0("-N ","save_results_",risks[i,risk])
  
  system(paste("qsub",jname,mem,fthread,runtime,archive,project,"-q long.q",sge.output.dir,r.shell,save.script,args))
  
  
  
}

# save(2)
complete <- lapply(1:nrow(risks),save)

#FOR FAIR CLUSTER: jobs with 3 years & 10 slots used 20-25 G of memory and 20-35 min of runtime. Seeing random failures, so launching multiple jobs. Dumb



#********************************************************************************************************************************
