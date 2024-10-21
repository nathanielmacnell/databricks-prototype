# Databricks notebook source
# MAGIC %md
# MAGIC Install packages with Linux dependencies

# COMMAND ----------

# MAGIC %md
# MAGIC ### Driver Init Script
# MAGIC The terra and prism packages require non-standard linux dependencies. The rest of the R packages can be installed using the driver UI (compute -> select cluster resource -> libraries then select the CRAN packages). To get these to persist, we'll need to put them into an init script. Currently, this takes about 15 minutes to compile.

# COMMAND ----------

# gdal dependencies
system("sudo apt-get update")
system("sudo apt-get install -y gdal-bin proj-bin libgdal-dev libproj-dev")

# terra setup (needs gdal)
install.packages('terra')
install.packages('prism')

# COMMAND ----------

### load required packages
library(dplyr)     # sql-like data management using pipes
library(stringr)   # string and regex helpers

library(lubridate) # datetime objects and functions
library(ggplot2)   # plots
library(sparklyr)  # interact with spark

library(tictoc)    # basic timings
library(arrow)     # read/write parquet
library(purrr)     # function mapping

### load libraries with linix dependencies
library(prism)     # prism data api
library(terra)     # raster data objects and functions

# COMMAND ----------

# note that this is ephemeral storage
prism_set_dl_dir("~/data/prism")

# COMMAND ----------

list.files('~/data/prism', full.names=TRUE)

# COMMAND ----------

getwd()

# COMMAND ----------

# Download Mean daily temperature (2020)
get_prism_dailys(
  type = "tmean", 
  minDate = "2020-01-01", 
  maxDate = "2020-12-31", 
  keepZip = FALSE
)

# COMMAND ----------

# Copy to workspace
system("cp -r /root/data/prism /Workspace/Users/nathaniel.macnell@dlhcorp.com/databricks-prototype/data/prism")


# COMMAND ----------

# Download Mean daily dew point temperature (2020)
get_prism_dailys(
  type = "tdmean", 
  minDate = "2020-01-01", 
  maxDate = "2020-12-31", 
  keepZip = FALSE
)

# COMMAND ----------

# verify archive contents
prism_archive_ls() |>
  str_extract('[0-9]{8}') |>
  as_date() |>
  data.frame() |>  # cast the vector as a data frame so we can plot it
  setNames('date') |>
  mutate(
    month = month(date),
    day = mday(date)
  ) |>
  ggplot(
    aes(x=month, y=day, label=date)  # set graph parameters
  ) +
  geom_text(size=2, alpha=0.5)  # black text indicates there are 2 values

# COMMAND ----------

library(prism)
temperature_files = prism_archive_subset("tmean", "daily", years = 2020) |>
  pd_to_file()
raw = terra::rast(temperature_files)

# COMMAND ----------

# get list of just the temperature and humidity values for loading




# prepare a reader function to import the temperature data
#prism_reader = function(x) {

 result = as.data.frame(raw, xy=TRUE)
#  rownames(result) <- NULL
#  
  # extract values of interest
  date = stringr::str_extract(names(result)[3], '[0-9]{8}')


  variable_names = names(result)
  
  
   = str_extract(names(result), 'PRISM_[a-z]+_stable') |>
    str_remove('PRISM_') |>
    str_remove('_stable')
  names(result) <- variable_name  
  # round to avoid floating point errors in the representation of grid cells
  result$x = round(result$x, digits=5)
  result$y = round(result$y, digits=5)
  return(data.frame(result))
}

# write temperature data to workspace
temperature = prism_reader(temperature_files)

# COMMAND ----------



# COMMAND ----------

names(temperature)

# COMMAND ----------

list.files('/Workspace/Users/nathaniel.macnell@dlhcorp.com/databricks-prototype/data/prism/tmean.csv')

# COMMAND ----------

# write to parquet file
write_csv(temperature, file="/dbfs/tmp/tmean.csv")

# COMMAND ----------

display(temperature)

# COMMAND ----------

# load into spark
library(SparkR)
temperature_df <- read.df('/dbfs/root/data/tmean.csv',source="csv")


# COMMAND ----------

# get list of just the temperature and humidity values for loading
temperature_files = prism_archive_subset("tmean", "daily", years = 2020) |>
  pd_to_file()
dewpoint_files = prism_archive_subset("tdmean", "daily", years = 2020) |>
  pd_to_file()

print(temperature_files)
print(dewpoint_files)

# COMMAND ----------

# MAGIC %md
# MAGIC ### File Reader and Writer
# MAGIC
# MAGIC Define a reader for the raw PRISM data
# MAGIC - Takes a file url as input, returns an R data frame in long format

# COMMAND ----------

prism_reader = function(x) {
  raw = terra::rast(x)
  result = as.data.frame(raw, xy=TRUE)
  rownames(result) <- NULL
  result$date = stringr::str_extract(names(result)[3], '[0-9]{8}')
  variable_name = str_extract(names(result)[3], 'PRISM_[a-z]+_stable') |>
    str_remove('PRISM_') |>
    str_remove('_stable')
  names(result)[3] <- variable_name  
  # round to avoid floating point errors in the representation of grid cells
  result$x = round(result$x, digits=5)
  result$y = round(result$y, digits=5)
  return(data.frame(result))
}

### define an ingest function for input files
# Spark is slow doing operations directly on the data because the format
# is awful, so we'll dump the files into a better format first
# we'll give them all the same schema (defined by prism_reader above)
prism_ingest = function(temperature_file, dewpoint_file) {
  
  temperature = prism_reader(temperature_file)
  humidity = prism_reader(dewpoint_file)
  
  # join data segments
  result = left_join(temperature, humidity, by=c('x','y','date'))
  
  # get file label
  file_name = unique(temperature$date)
  output_path = file.path('~/data/parquet',file_name)
  
  # write
  write_parquet(result, sink = output_path, compression = 'UNCOMPRESSED')
}

# COMMAND ----------

### Dump downloaded files into a parquet database
# by walking ingest function across pairs of temperature, dewpoint references
walk2(temperature_files,  #TODO: Turn this into a spark job
     dewpoint_files,
     prism_ingest,
     .progress='Ingesting raw PRISM data to parquet')

# COMMAND ----------

dbutils.fs.ls("/Volumes/my_catalog/my_schema/my_volume/") %fs ls /Volumes/my_catalog/my_schema/my_volume/

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW VOLUMES

# COMMAND ----------

library('SparkR')
df <- sql("SELECT * FROM prism.default.temperature_1_day")

# COMMAND ----------

display(df)
