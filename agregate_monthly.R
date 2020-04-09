library(lubridate)
library(dplyr)
library(data.table)
library(tidyverse)
library(scales)
library(plyr)

# get current year
rm(list = ls())
data_path <- "/yogyaapp/data/sales/"


###############33 change ever year #####################
# cur_year <- as.numeric(format(Sys.Date(), "%y"))
# prev_year <- as.numeric(format(Sys.Date(), "%y")) - 1
# cur_full_year  <- paste0(format(Sys.Date(), "%Y-%m"),"-")
# prev_full_year <- paste0(as.numeric(format(Sys.Date(), "%Y"))-1, substring(cur_full_year,5,7),"-")
cur_year <- 20
prev_year <- 19
cur_full_year  <- "2020-01-"
prev_full_year <- "2019-01"

cur_full_date  <- paste0(format(Sys.Date(), "%Y-%m-%d"))
date <- seq(from = as.Date("2020-01-01"), to = as.Date(cur_full_date), by = "month")
#date <- seq(from = as.Date("2019-01-01"), to = as.Date("2019-12-31"), by = "month")

print(date)
loop_date <- data.frame(date = date)

# Loop over loop_date
for (row in 1:nrow(loop_date)) {
  
    # Tanggal Sesuai Loop ( ex: 2019-05-01 )
    date_year_cur  <- loop_date[row, "date"]
    
    # #date_year_cur  <- loop_date[row, "date"]
    month_filename  <- paste0( format(loop_date[row, "date"],"%Y-%m"))
    # print(month_filename)
    month_row       <- paste0( format(loop_date[row, "date"],"%Y-%m"),"-")
    # #full month -> Januari February etc
    cur_full_month  <- paste0( format(loop_date[row, "date"], "%B"))
    # print(month_row)
    #print(month_row)
    
    ##############################################################
    # Load file per Month
    # Note : Current year and Previouse Year is already in file
    ##############################################################
   
    the_pattern <- paste("sales-mtd-",month_row,"**",".RDS",sep="")
    get_files <- dir(data_path, pattern= glob2rx(the_pattern) ) # get file names
    #print(get_files)

    # cukup ambil satu file saja dari bulan terakhir, karena di dalam satu bulan itu sudah akumulasi 
    # get last file from this file -> sales-mtd-2019-01-01.RDS
    last_file <-  max(substring(get_files,11,20))
    the_last_file_pattern <- paste("sales-mtd-",last_file,".RDS",sep="")
    #last_file <- substring(get_files,11,20) 
    print(the_last_file_pattern)

    #print(get_files)
    print('Load last  montly file rds')
    data_monthly <- the_last_file_pattern %>%
      map_dfr(function(x) { 
        readRDS(file.path(data_path, x)) %>% mutate(the_last_file_pattern=x) } )
    
    print('agregate last monthly file rds')
    data_monthly_agg <- data_monthly %>%
    group_by(store_code,store_desc,directorate_code,directorate_name,
             div,divdesc,cat, catdesc, subcat, subcatdesc, system) %>%
      select( store_code,store_desc,directorate_code,directorate_name,
              div,divdesc,cat, catdesc, subcat, subcatdesc, system,
              paste0("qty",prev_year),       
              paste0("qty",cur_year),        
              paste0("netsales",prev_year),       
              paste0("netsales",cur_year),        
              paste0("profit",prev_year),         
              paste0("profit",cur_year)           
      ) %>%
      summarise_all(sum) %>%
      ungroup()
    
    data_monthly_agg <- data_monthly_agg %>% 
      mutate(month = month_filename,
             month_name = cur_full_month)
    
    saveRDS(data_monthly_agg, paste0(data_path,"sales-month-",month_filename,".RDS"))
}


#saveRDS(data_monthly_agg, paste0("clean/sales-month-",month_filename,".RDS"))



