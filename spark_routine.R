## load library
  library(dplyr)
  library(sparklyr)
  
## set up spark
  spark_home_set("D:\\Spark")
  spark_available_versions()
  spark_install_dir()
  spark_install(version = "2.1.0", hadoop_version = "2.7")
  
  
  spark_home_dir()
  spark_installed_versions()
  
  # Create Spark connection and read data
  sc = spark_connect(master="local", version="2.1.0")
  sc <- spark_connect(master = "local", spark_home=spark_home_dir(version = "2.1.0"))
  
  # read data
  data_feed_local <- spark_read_csv(sc = sc,
                                   name = "data_feed",
                                   path = "data/01-report.suite_2014-*.tsv",
                                   header = FALSE,
                                   delimiter = "\t")