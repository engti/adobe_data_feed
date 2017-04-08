## load library
  library(dplyr)
  library(sparklyr)

## load data
  setwd("btData")

#Get list of lookup files from tarball
  files_tar <- untar("btcom_2016-12-02-lookup_data.tar.gz", list = TRUE)

#Extract files to _temp directory. Directory will be created if it doesn't exist
  untar("btcom_2016-12-02-lookup_data.tar.gz", exdir = "_temp")

#Read each file into a data frame
#If coding like this in R offends you, keep it to yourself...
for (file in files_tar) {
  df_name <- unlist(strsplit(file, split = ".tsv", fixed = TRUE))
  temp_df <- read.delim(paste("_temp", file, sep = "/"), header=FALSE, stringsAsFactors=FALSE)
  #column_headers not used as lookup table
  if (df_name != "column_headers"){
    names(temp_df) <- c("id", df_name)
  }
  assign(df_name, temp_df)
  rm(temp_df)
}  

#gz files can be read directly into dataframes from base R
#Could also use `readr` library for performance
servercall_data <- read.delim("01-wgil-coles-prod-responsive_2017-01-31.tsv.gz",
                              header=FALSE, stringsAsFactors=FALSE)

#Use column_headers to label servercall_data data frame using first row of data
names(servercall_data) <- column_headers[1,1:1005]

tmp <- gsub(".tsv","",files_tar)  
for(file in files_tar){
  df1 <-  servercall_data %>% select(matches(paste0("^",tmp[1],"$"))) %>% names()
}  

tmp <- servercall_data

tmpdf <- servercall_data %>% 
  left_join(browser,by = c("browser" = "id")) %>% 
  select(-browser,browser = browser.y)

#If coding like this in R offends you, keep it to yourself...
for (file in files_tar) {
  df_name <- unlist(strsplit(file, split = ".tsv", fixed = TRUE))
  temp_df <- read.delim(paste("_temp", file, sep = "/"), header=FALSE, stringsAsFactors=FALSE)
  #column_headers not used as lookup table
  if (df_name != "column_headers"){
    names(temp_df) <- c("id", df_name)
  }
  assign(df_name, temp_df)
  rm(temp_df)
  tmpdf <- tmpdf %>% 
    left_join(`paste0(df_name)`,by = c(`paste0(df_name)` = "id")) %>% 
    select(-`paste0(df_name)`,`paste0(df_name)` = `paste0(df_name,".y")`)
}  


#### spark stuff ####
# Create Spark connection and read data
sc = spark_connect(master="local", version="2.0.2")
