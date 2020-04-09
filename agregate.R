library(lubridate)
library(dplyr)
library(data.table)
library(tidyverse)
library(scales)
library(plyr)
library(curl)

# get current year
rm(list = ls())
data_sales <- "/yogyaapp/Rscript/data/clean/sales-harian-article-csv/"
data_save <- "/yogyaapp/data/sales/"
data_path <- "/yogyaapp/Rscript/data/clean/sales-harian"

##### Change ever year ############################
cur_year <- as.numeric(format(Sys.Date(), "%y"))
prev_year <- as.numeric(format(Sys.Date(), "%y")) - 1
# cur_year <- 20
# prev_year <- 19

################ end of change every year##########################

##### Global Parameter ############################
max_date_cur_year <- Sys.Date()-1
#min_date_cur_year <- max_date_cur_year - 4
min_date_cur_year <- as.Date("2020-03-10")

# max_date_cur_year <- as.Date("2019-01-01")
# min_date_cur_year <- as.Date("2019-01-01")

##### Global Parameter ############################

library(sparklyr)

################################################################
## Spark Cluster 
## spark client version harus sama dengan versio spark cluster
################################################################

# Set memory allocation for whole local Spark instance
Sys.setenv("SPARK_MEM" = "50g")
config <- spark_config()
#User Memory
config[["sparklyr.shell.driver-memory"]] <- "100G"
config[["sparklyr.shell.executor-memory"]] <- "100G"
config[["maximizeResourceAllocation"]] <- "TRUE"
config[["spark.default.parallelism"]] <- 32
config[["spark.sql.catalogImplementation"]] <- "in-memory"
config[["spark.driver.memoryOverhead"]] = "100g" 
config[["sparklyr.gateway.start.timeout"]] <- 10

config[["sparklyr.shell.conf"]] <- "spark.driver.extraJavaOptions=-XX:MaxHeapSize=50G"
config[["spark.driver.maxResultSize"]] <- "30G"
config[["spark.dynamicAllocation.enable"]] <- "TRUE"
config[["spark.sql.shuffle.partitions"]] <- 400 
#config[["sparklyr.gateway.address"]] <- "127.0.0.1"

config[["spark.driver.extraJavaOptions"]] <- "-Xmx6G -XX:MaxPermSize=20G -XX:+UseCompressedOops -XX:UseGCOverheadLimit=50G" 

#spark_disconnect(sc)
sc <- spark_connect(master = "spark://xxx.xxx.xxx.xx:7078",
                    version = "2.4.3",
                    config = config,
                    spark_home = "/opt/spark-2.4.3-bin-hadoop2.7")

# =====================================================
# Load Sales Current Year
# =====================================================
cur_year_pattern <- paste0(data_sales,"sales_article_",cur_year,"**",".csv")
current_year_sales_sc <- spark_read_csv(sc, "current_year_sales_sc",
                                        path = cur_year_pattern, 
                                        header=T, 
                                        overwrite=T,
                                        memory=F,
                                        infer_schema = FALSE
                                        )

print('agregate current year file rds')

##############################################################
# Previos Year
##############################################################
prev_year_pattern <- paste0(data_sales,"sales_article_",prev_year,"**",".csv")
# do not cache the initial data set to save memory; only cache the final result
prev_year_sales_sc <- spark_read_csv(sc, name = "prev_year", 
                                        path = prev_year_pattern, 
                                        #sep="/"
                                        header=T, 
                                        overwrite=T,
                                        memory=F,
                                        infer_schema = FALSE
                                     )

print('agregate prev year file rds')

date <- seq(from = as.Date(min_date_cur_year), to = as.Date(max_date_cur_year), by = "days")
loop_date <- data.frame(date = date)
loop_date

# Loop over loop_date
for (row in 1:nrow(loop_date)) {
    #Tanggal Sesuai Loop ( ex: 2019-05-01 )
    date_year_cur  <- loop_date[row, "date"]
    cat(paste("Processing ",date_year_cur, collapse = "\n"), "\n", sep = "")

    #date_year_cur  <- as.Date("2019-11-15")
    # Tanggal sebelum di tahun lalu dari date_year_cur ( ex: 2018-05-01 )
    date_year_prev <- paste0(as.numeric(substring(date_year_cur,1,4))-1,substring(date_year_cur,5,11))
    
    # Tanggal Awal Tahun (ex: 01/01/2019)
    first_year_cur <- paste0(substring(date_year_cur,1,4),"-01-01")
    # Tanggal tahun Pertama sebelumnya ex: 01/01/2018
    first_year_prev <- paste0(as.numeric(substring(date_year_cur,1,4))-1,"-01-01")

    # Tanggal pertama di bulan current
    first_month_cur <- floor_date(date_year_cur, "month")
    first_month_prev <- paste0(as.numeric(substring(first_month_cur,1,4))-1,substring(first_month_cur,5,10))
    
    #####################################################################
    #                    PROSES SALES YTD 
    #####################################################################
    
    #####################################################################
    # Filter YTD CURRENT YEAR
    # dari tanggal current sampai awal tahun currunt
    # Misal Tanngal current             : 20/05/2019
    # Filter untuk YTD di tahun current : 20/05/2019 s/d 01/01/2019 (First Year)
    #####################################################################
    
    #glimpse(current_year_sales_sc)
    ytd_cur <- current_year_sales_sc %>% 
      filter(between(tanggal, as.Date(first_year_cur),as.Date(date_year_cur))) %>% 
      group_by(store_code,store_desc,directorate_code,directorate_name,div,
               divdesc,cat, catdesc,subcat, subcatdesc,system) %>% 
      select(store_code,store_desc,directorate_code,directorate_name,div,
               divdesc,cat,catdesc,subcat, subcatdesc,system,
               netsales,qty,sales,cogs,ppn,profit) %>% 
        summarise_all(sum) %>% 
        ungroup() %>% 
        collect()
  

    ## Change column name
    ytd_cur <- plyr::rename(ytd_cur,setNames(paste0('netsales',cur_year),"netsales" ))
    ytd_cur <- plyr::rename(ytd_cur,setNames(paste0('qty',cur_year),"qty" ))
    ytd_cur <- plyr::rename(ytd_cur,setNames(paste0('sales',cur_year),"sales" ))
    ytd_cur <- plyr::rename(ytd_cur,setNames(paste0('cogs',cur_year),"cogs" ))
    ytd_cur <- plyr::rename(ytd_cur,setNames(paste0('ppn',cur_year),"ppn" ))
    ytd_cur <- plyr::rename(ytd_cur,setNames(paste0('profit',cur_year),"profit" ))

    #####################################################################
    # Filter YTD PREVIOUS YEAR
    # dari tanggal current sampai awal tahun currunt
    # Misal Tanngal LALU                : 20/05/2018
    # Filter untuk YTD di tahun current : 20/05/2018 s/d 01/01/2018 (First Year)
    #####################################################################
    
    # previous Year
    ytd_prev <- prev_year_sales_sc %>% 
      filter(between(tanggal, as.Date(first_year_prev),as.Date(date_year_prev))) %>% 
      group_by(store_code,store_desc,directorate_code,directorate_name,div,
               divdesc,cat, catdesc,subcat, subcatdesc,system) %>% 
      select(store_code,store_desc,directorate_code,directorate_name,div,
             divdesc,cat,catdesc,subcat, subcatdesc,system,
             netsales,qty,sales,cogs,ppn,profit) %>% 
      summarise_all(sum) %>% 
      ungroup() %>% 
      collect()
    
    ## Change column name
    ytd_prev <- plyr::rename(ytd_prev,setNames(paste0('netsales',prev_year),"netsales" ))
    ytd_prev <- plyr::rename(ytd_prev,setNames(paste0('qty',prev_year),"qty" ))
    ytd_prev <- plyr::rename(ytd_prev,setNames(paste0('sales',prev_year),"sales" ))
    ytd_prev <- plyr::rename(ytd_prev,setNames(paste0('cogs',prev_year),"cogs" ))
    ytd_prev <- plyr::rename(ytd_prev,setNames(paste0('ppn',prev_year),"ppn" ))
    ytd_prev <- plyr::rename(ytd_prev,setNames(paste0('profit',prev_year),"profit" ))
    
    # ##############################################################
    # # Join df
    # ##############################################################
    #print('merge current year and prev year')
    #glimpse(ytd_prev)
    df_join_ytd <- merge(ytd_prev,ytd_cur,
                     by =  c("store_code","store_desc","directorate_code","directorate_name","div",
                             "divdesc","cat","catdesc","subcat", "subcatdesc","system"
                     ),
                     all=TRUE  # meaning  keep all rows from both data frames
    ) %>% 
      select (
        store_code,
        store_desc,
        directorate_code,
        directorate_name,
        div,
        divdesc,
        cat,    
        catdesc,
        subcat, 
        subcatdesc,
        system,
        paste0('netsales',cur_year), paste0('netsales',prev_year),
        paste0('qty',cur_year),      paste0('qty',prev_year),
        paste0('sales',cur_year),    paste0('sales',prev_year),
        paste0('cogs',cur_year),     paste0('cogs',prev_year),
        paste0('ppn',cur_year),      paste0('ppn',prev_year),
        paste0('profit',cur_year),   paste0('profit',prev_year)
      )
    
    filename_rds <-  paste0(data_save,"sales-ytd-",date_year_cur,".RDS")

    if  (file.exists(filename_rds)) {
      file.remove(filename_rds)    
    }
    saveRDS(df_join_ytd,filename_rds)


    #####################################################################
    #                    PROSES SALES MTD
    #####################################################################

    mtd_cur <- current_year_sales_sc %>% 
      filter(between(tanggal, as.Date(first_month_cur),as.Date(date_year_cur))) %>% 
      group_by(store_code,store_desc,directorate_code,directorate_name,div,
               divdesc,cat, catdesc,subcat, subcatdesc,system) %>% 
      select( store_code,store_desc,directorate_code,directorate_name,div,
              divdesc,cat,catdesc,subcat, subcatdesc,system,
              netsales,qty,sales,cogs,ppn,profit) %>% 
      summarise_all(sum) %>% 
      ungroup() %>% 
      collect()
    
    ## Change column name
    mtd_cur <- plyr::rename(mtd_cur,setNames(paste0('netsales',cur_year),"netsales" ))
    mtd_cur <- plyr::rename(mtd_cur,setNames(paste0('qty',cur_year),"qty" ))
    mtd_cur <- plyr::rename(mtd_cur,setNames(paste0('sales',cur_year),"sales" ))
    mtd_cur <- plyr::rename(mtd_cur,setNames(paste0('cogs',cur_year),"cogs" ))
    mtd_cur <- plyr::rename(mtd_cur,setNames(paste0('ppn',cur_year),"ppn" ))
    mtd_cur <- plyr::rename(mtd_cur,setNames(paste0('profit',cur_year),"profit" ))
    
    #####################################################################
    # Filter MTD PREVIOUS YEAR
    # dari tanggal current sampai awal tahun currunt
    # Misal Tanngal LALU                : 20/05/2018
    # Filter untuk YTD di tahun current : 20/05/2018 s/d 01/01/2018 (First Mont)
    #####################################################################
    
    mtd_prev <- prev_year_sales_sc %>% 
    filter(between(tanggal, as.Date(first_month_prev),as.Date(date_year_prev))) %>% 
    group_by(store_code,store_desc,directorate_code,directorate_name,div,
               divdesc,cat, catdesc,subcat, subcatdesc,system) %>% 
      select( store_code,store_desc,directorate_code,directorate_name,div,
              divdesc,cat,catdesc,subcat, subcatdesc,system,
              netsales,qty,sales,cogs,ppn,profit) %>% 
      summarise_all(sum) %>% 
      ungroup() %>% 
      collect()
    
    ## Change column name
    mtd_prev <- plyr::rename(mtd_prev,setNames(paste0('netsales',prev_year),"netsales" ))
    mtd_prev <- plyr::rename(mtd_prev,setNames(paste0('qty',prev_year),"qty" ))
    mtd_prev <- plyr::rename(mtd_prev,setNames(paste0('sales',prev_year),"sales" ))
    mtd_prev <- plyr::rename(mtd_prev,setNames(paste0('cogs',prev_year),"cogs" ))
    mtd_prev <- plyr::rename(mtd_prev,setNames(paste0('ppn',prev_year),"ppn" ))
    mtd_prev <- plyr::rename(mtd_prev,setNames(paste0('profit',prev_year),"profit" ))
    
    # ##############################################################
    # # Join df
    # ##############################################################
    # print('merge current month and prev month')
    df_join_mtd <- merge(mtd_prev,mtd_cur,
                         by = c("store_code","store_desc","directorate_code","directorate_name","div",
                                  "divdesc","cat","catdesc","subcat", "subcatdesc","system"
                         ),
                         all=TRUE # meaning  keep all rows from both data frames
    ) %>% 
      select (
        store_code,
        store_desc,
        directorate_code,
        directorate_name,
        div,
        divdesc,
        cat,    
        catdesc,
        subcat, 
        subcatdesc,
        system,
        paste0('netsales',cur_year), paste0('netsales',prev_year),
        paste0('qty',cur_year),      paste0('qty',prev_year),
        paste0('sales',cur_year),    paste0('sales',prev_year),
        paste0('cogs',cur_year),     paste0('cogs',prev_year),
        paste0('ppn',cur_year),      paste0('ppn',prev_year),
        paste0('profit',cur_year),   paste0('profit',prev_year)
      )
    
    filename_rds <-  paste0(data_save,"sales-mtd-",date_year_cur,".RDS")

    if  (file.exists(filename_rds)) {
      file.remove(filename_rds)    
    }
    saveRDS(df_join_mtd,filename_rds)
    
}

spark_disconnect(sc)

