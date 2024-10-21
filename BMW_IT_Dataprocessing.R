
# README ------------------------------------------------------------------
# This script is tramsforms all the data for the specific car.
# Problem: Script takes very long, since the adstock/hyp tranformations
# for the non-car-specific products are done in each iteration of the loop
# CARE: This skript takes around 3-3.5h to run, is only the most important 
# models schould be transformed, then run v02


# Prerequisites -----------------------------------------------------------
library(readxl)
library(dplyr)
library(tidyr)
library(ammm)
library(stringr)
library(writexl)
library(data.table)



# Data Import -----------------------------------------------------------------
rm(list=ls())
graphics.off()


# Set paths ---------------------------------------------------------------
import_path <- "G:/MP_Insights/Kunden/Bestandskunden/TME_The_Marcom_Engine/04_NVWR_Prognosis/02_Data/02_Work"
export_path <- "G:/MP_Insights/Kunden/Bestandskunden/TME_The_Marcom_Engine/04_NVWR_Prognosis/03_Analysis/Italy/test"
local_import_path <-"C:/Local_Files/BMW_NVWR/datapreprocessing_import"
local_export_path <-"C:/Local_Files/BMW_NVWR/datapreprocessing_export"


# import preprocessed data 
setwd(local_import_path) # CHANGE TO LOCAL IF SERVER PROBLEMS OR HOMEOFFICE
df_dm <- read_excel(
  path= "Digital_Media_BMW.xlsx")
df_NVWR_IT<- read.csv(
  file= "BMW_IT_NVWR_by_vehicle_v01.csv",
  header=TRUE, sep=";")

# extract column names
cars <- names(df_NVWR_IT[-c(1,ncol(df_NVWR_IT))])

# rename the NVWR for every car according to the coding scheme
df_NVWR_IT <- df_NVWR_IT %>% 
  rename_with(~ paste0("AxBMWxITx", ., "xNVWR"), .cols = -1)

# cut off digital_media dataframe to match the length of the NVWR dataframe
df_dm <- df_dm[1:(nrow(df_dm)-2),]
df_dm = as.data.frame(df_dm) # if u r using ammm, please use data.frame class

# Non-Specific Products ---------------------------------------------------
non_specific_product <- c("BRAND","COMPETITIOR","CONCEPT","DEALER","ELECTRIC","GENERIC", "MULTIPLE")

# filter digital media data frame for italy and every non_specific product
vars <- names(df_dm)[str_detect(names(df_dm) , "xITx")]
vars <- vars[grepl(paste0("x(", paste(non_specific_product, collapse = "|"), ")x"), vars)] 

#make df that includes all the non-specific products ( e.g MULTIPLE, BRAND etc.)
#--> will be merged with the car-specific dfs to later test if the effects of
# media which is not directly targeted towards a specific car is relevant
df_dm_IT_nsp <- df_dm[,c("CW", vars)]
df_dm_IT_nsp[,1] <- as.Date(df_dm_IT_nsp[,1])
names(df_dm_IT_nsp)[1] <- "Date"
df_dm_IT_nsp <- day2week(df_dm_IT_nsp, dayVar = "Date", aggVars = names(df_dm_IT_nsp)[-1])


# Transformation Loop -----------------------------------------------------
setwd(local_export_path) # CHANGE TO LOKAL IF SERVER PROBLEMS OR HOMEOFFICE

no_digital_media <- c() #stores cars that do not have digital media data
start_time <- Sys.time()
for (car in cars){
  start_time2 <- Sys.time()
  cat("Current Car:",car, "\n")
  #create the filter for car model in digital media df
  vars <- names(df_dm)[str_detect(names(df_dm) , "xITx")]
  vars <- vars[grepl(paste0("x", car, "x"), vars)] 
  
  if ( length(vars) > 0){
    df_dm_IT_car <- df_dm[,c("CW", vars)]
    df_dm_IT_car[,1] <- as.Date(df_dm_IT_car[,1])
    names(df_dm_IT_car)[1] <- "Date"
    df_dm_IT_car <- day2week(df_dm_IT_car, dayVar = "Date", aggVars = names(df_dm_IT_car)[-1])     #use "day2week"-funtion to change to monday-date if needed and append KW and Jahr
  }else{
    no_digital_media <- c(no_digital_media,car)
    next
  }
  #analoguous filtering for the Italian NVWR data frame
  vars <- names(df_NVWR_IT)[str_detect(names(df_NVWR_IT) , "xITx")]
  vars <- vars[grepl(paste0("x", car, "x"), vars)] 
  
  if (length(vars) > 0){
    #filter NVWR dataframe for current car
    df_NVWR_IT_car <- df_NVWR_IT[,c("Week" , vars)]
    df_NVWR_IT_car[,1] <- as.Date(df_NVWR_IT_car[,1])
    df_NVWR_IT_car <- day2week(df_NVWR_IT_car, dayVar = "Week", aggVars = names(df_NVWR_IT_car)[-1])
  }else{
      next
  }
  #merge dfs and export non-transformed
  modeldata_car <- weekly_merge(list(df_NVWR_IT_car, df_dm_IT_car, df_dm_IT_nsp))
  name <- paste0("modeldata_BMW_IT_",car,".csv")
  fwrite(modeldata_car, name) # "fwrite" a lot faster than write.csv
  
  #transform merged df and export seperately
  #ACTIVATE THIS PART AFTER THE REST RUNS SMOOTHLY
  vars <- names(modeldata_car)[-c(1:4)]
  modeldata_car_transformed <- adstockHyp_transform(data = modeldata_car,
                                                    vars= vars)
  name <- paste0("modeldata_BMW_IT_",car,"_transformed.csv")
  fwrite(modeldata_car_transformed, name)
  
  
  index <- match(car, cars)
  cat("Overall progress:", index, "out of ", length(cars), "cars","\n")
  end_time2 <- Sys.time()
  print(end_time2 - start_time2)
  cat("\n")
}
end_time <- Sys.time()
end_time - start_time
cat("No digital media data available for: \n", no_digital_media)

##### works until here










# F40 (redundent) --------------------------------------------------------------------
# rm(list=ls())
# graphics.off()
# 
# 
# # Read-In Preprocessed Data 
# 
# df_dm <- read_excel(path= "G:/MP_Insights/Kunden/Bestandskunden/TME_The_Marcom_Engine/04_NVWR_Prognosis/02_Data/02_Work/Digital_Media_BMW.xlsx")
# 
# df_NVWR_IT<- read.csv(
#   file= "G:/MP_Insights/Kunden/Bestandskunden/TME_The_Marcom_Engine/04_NVWR_Prognosis/02_Data/02_Work/BMW_IT_NVWR_by_vehicle_v01.csv",
#   header=TRUE, sep=";")
# 
# 
# # rename the NVWR for every car according to the coding scheme
# df_NVWR_IT <- df_NVWR_IT %>% 
#   rename_with(~ paste0("AxBMWxITx", ., "xNVWR"), .cols = -1)
# 
# 
# #cut off digital_media dataframe to match the length of the NVWR dataframe
# df_dm <- df_dm[1:(nrow(df_dm)-2),]
# 
# df_dm = as.data.frame(df_dm) # if u r using ammm, please use data.frame class
# 
# 
# 
# #create variable that filters the digital media data frame for Italy and F40
# vars <- names(df_dm)[str_detect(names(df_dm) , "xITx")]
# vars <- vars[str_detect( vars, "xF40x")]
# 
# # "dm" stands for digital media -> filter digitam media dataframa for IT and F40
# df_dm_IT_F40 <- df_dm[,c("CW", vars)]
# df_dm_IT_F40$CW <- as.Date(df_dm_IT_F40$CW)
# names(df_dm_IT_F40)[1] <- "Date"
# 
# # the "day2week" function checks if the date is the monday-date and appends
# # Jahr and KW 
# df_dm_IT_F40 <- day2week(df_dm_IT_F40, dayVar = "Date", aggVars = names(df_dm_IT_F40)[-1])
# 
# 
# vars = names(df_NVWR_IT)[str_detect(names(df_NVWR_IT) , "xITx")]
# vars = vars[str_detect( vars, "xF40x")]
# 
# df_NVWR_IT2 = df_NVWR_IT[,c("Week" , vars)]
# class(df_NVWR_IT2$Week)
# df_NVWR_IT2$Week = as.Date(df_NVWR_IT2$Week)
#       
#       
# df_NVWR_IT3 = day2week(df_NVWR_IT2, dayVar = "Week", aggVars = names(df_NVWR_IT2)[-1])
# 
# 
# modeldata = weekly_merge(list(df_NVWR_IT3, df_dm_IT_F40))
# 
# 
# 
# #adstock transformation 
# 
# vars = names(modeldata)[-c(1:4)]
# modeldata_transformed = adstockHyp_transform(data = modeldata,
#                                              vars = vars)
# getwd()
# setwd("G:/MP_Insights/Kunden/Bestandskunden/TME_The_Marcom_Engine/04_NVWR_Prognosis/03_Analysis/Italy")
# save(modeldata_transformed, file = "modeldata_transformed_IT_F40.RData")


# U11(redundent) ---------------------------------------------------------------------
# 
# rm(list=ls())
# graphics.off()
# 
# 
# # Read-In Preprocessed Data 
# 
# df_dm <- read_excel(path= "G:/MP_Insights/Kunden/Bestandskunden/TME_The_Marcom_Engine/04_NVWR_Prognosis/02_Data/02_Work/Digital_Media_BMW.xlsx")
# 
# df_NVWR_IT<- read.csv(
#   file= "G:/MP_Insights/Kunden/Bestandskunden/TME_The_Marcom_Engine/04_NVWR_Prognosis/02_Data/02_Work/BMW_IT_NVWR_by_vehicle_v01.csv",
#   header=TRUE, sep=";")
# 
# 
# # rename the NVWR for every car according to the coding scheme
# df_NVWR_IT <- df_NVWR_IT %>% 
#   rename_with(~ paste0("AxBMWxITx", ., "xNVWR"), .cols = -1)
# 
# 
# #cut off digital_media dataframe to match the length of the NVWR dataframe
# df_dm <- df_dm[1:(nrow(df_dm)-2),]
# 
# df_dm = as.data.frame(df_dm) # if u r using ammm, please use data.frame class
# 
# 
# 
# #create variable that filters the digital media data frame for Italy and F40
# vars <- names(df_dm)[str_detect(names(df_dm) , "xITx")]
# vars <- vars[str_detect( vars, "xU11x")] # !!!!!!!!!!!!!!!!!
# 
# # "dm" stands for digital media -> filter digitam media dataframa for IT and F40
# df_dm_IT_car <- df_dm[,c("CW", vars)]
# df_dm_IT_car$CW <- as.Date(df_dm_IT_car$CW)
# names(df_dm_IT_car)[1] <- "Date"
# 
# # the "day2week" function checks if the date is the monday-date and appends
# # Jahr and KW 
# df_dm_IT_car <- day2week(df_dm_IT_car, dayVar = "Date", aggVars = names(df_dm_IT_car)[-1])
# 
# 
# vars = names(df_NVWR_IT)[str_detect(names(df_NVWR_IT) , "xITx")]
# vars = vars[str_detect( vars, "xU11x")] # !!!!!!!!!!!!!!!!!
# 
# df_NVWR_IT2 = df_NVWR_IT[,c("Week" , vars)]
# class(df_NVWR_IT2$Week)
# df_NVWR_IT2$Week = as.Date(df_NVWR_IT2$Week)
# 
# 
# df_NVWR_IT3 = day2week(df_NVWR_IT2, dayVar = "Week", aggVars = names(df_NVWR_IT2)[-1])
# 
# 
# modeldata = weekly_merge(list(df_NVWR_IT3, df_dm_IT_car))
# 
# 
# 
# #adstock transformation 
# 
# vars = names(modeldata)[-c(1:4)]
# modeldata_transformed = adstockHyp_transform(data = modeldata,
#                                              vars = vars)
# getwd()
# setwd("G:/MP_Insights/Kunden/Bestandskunden/TME_The_Marcom_Engine/04_NVWR_Prognosis/03_Analysis/Italy")
# save(modeldata_transformed, file = "modeldata_transformed_IT_U11.RData") #!!!!!!!!!!!!!!!!!







