### prism_example.R
# Example script showing use case for pre-processing public geospatial data
#
# We are aiming to calculate a year's worth of daily heat index values for 
# 4km grid cells covering the contiguous United States

# the prism data is accessed through an API that caches the files on disk for 
# later processing

### Requirements
# Java 8 JRE (for local spark to run)

##### Setup #####

### install R packages if needed (needs access to cran.r-project.org)
# install.packages("dplyr")
# install.packages("prism")
# install.packages("stringr")
# install.packages("lubridate")
# install.packages("ggplot2")
# install.packages("sparklyr")
# install.packages("terra")
# install.packages("tictoc")
# install.packages("arrow")
# install.packages("purrr")

### load required packages
library(dplyr)     # sql-like data management using pipes
library(stringr)   # string and regex helpers
library(prism)     # prism data api
library(lubridate) # datetime objects and functions
library(ggplot2)   # plots
library(sparklyr)  # interact with spark
library(terra)     # raster data objects and functions
library(tictoc)    # basic timings
library(arrow)     # read/write parquet
library(purrr)     # function mapping

### Install local spark (if not already done) for the local test
# In the real deployment we'll be talking to a spark server somewhere else
# spark_install()

### Set up spark connection
# This version uses a local spark
sc <- spark_connect(master = "local")

# For the real deployment, point to the spark resource instead
# sc <- spark_connect(master = "spark://hostname:port")

### Set data cache directory
# I'm just using my local disk but this could be pointed to any storage resource
dir.create('~/prism')
prism_set_dl_dir("~/prism")
# also create a directory to dump the parquet files
dir.create('~/parquet')

##### Copy data to cache #####
# To increase the workload, increase the time range
# The full data ranges from '1981-01-01' to '2024-02-28'
# This example only uses one year of data (2020)
# The API will skip downloading if the files already exist in cache

# mean temperature
get_prism_dailys(
  type = "tmean", 
  minDate = "2020-01-01", 
  maxDate = "2020-12-31", 
  keepZip = FALSE
)

# dew point temperature
get_prism_dailys(
  type = "tdmean", 
  minDate = "2020-01-01", 
  maxDate = "2020-12-31", 
  keepZip = FALSE
)

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

# get list of just the temperature and humidity values for loading
temperature_files = prism_archive_subset("tmean", "daily", years = 2020) |>
  pd_to_file()
dewpoint_files = prism_archive_subset("tdmean", "daily", years = 2020) |>
  pd_to_file()

##### Define reader and writer #####

### define a reader for raw data
# takes the file uri as input, returns an R data frame in long format
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

test = read_parquet('~/parquet/20200110')

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
  output_path = file.path('~/parquet',file_name)
  
  # write
  write_parquet(result, sink = output_path, compression = 'UNCOMPRESSED')
}

### Dump cache into parquet database
# by walking ingest function across pairs of temperature, dewpoint references
walk2(temperature_files,  #TODO: Turn this into a spark job
     dewpoint_files,
     prism_ingest,
     .progress='Ingesting raw PRISM data to parquet')



##### Spark Operations #####

### Import data
# Timings are for an Apple M2 Max chip with one 12-core executor on local spark
# wrap code in tic() toc() to get basic timings

# load data references (13 sec)
tic()
climate = spark_read_parquet(sc, 
                         path = '~/parquet')
toc()

### Inspect data



# inspect schema
climate
class(climate)
sdf_schema(climate)


# count number of rows (176M for one year of data)
sdf_nrow(climate)

# inspect averages
climate |>
  summarize(
    mean_tmean = mean(tmean, na.rm=TRUE),
    mean_tdmean = mean(tdmean, na.rm=TRUE))


### Transform data

# define humidity formula
get_humidity = function(t_c, td_c) {
  100*(exp((17.625*td_c)/(243.04+td_c)) /exp((17.625*t_c)/(243.04+t_c)))
}

# define NOAA heat index transformation 
get_heat_index = function(tf, rh) {
  
  # first, calculate heat index
  heat_index_simple = 0.5 * (tf + 61.0 + ((tf-68.0)*1.2) + (rh*0.094))
  # next, average simple heat index with temperature; if below 80 return simple
  threshold = (heat_index_simple + tf) / 2
  if(threshold < 80) return(heat_index_simple)
  
  # otherwise, use polynomial formula
  heat_index = -42.379 + 2.04901523*tf + 10.14333127*rh - .22475541*tf*rh - .00683783*tf*tf - .05481717*rh*rh + .00122874*tf*tf*rh + .00085282*T*rh*rh - .00000199*tf*tf*rh*rh
  
  # apply adjustment 1
  if( (rh < 13) & (tf>80) & (tf<112) ) {
    heat_index = heat_index - ((13-rh)/4)*sqrt((17-abs(t_f-95))/17)
  }
  
  return(heat_index)
  
}

Get_heat_index = Vectorize(get_heat_index)

# prepare output dataset with derived statistics (heat index)
# we'll structure this as one spark job, using dplyr verbs (sql stand-ins)
# TODO: Things need to be wrapped in spark_apply() to run custom R code...
# heat_index = climate |>
#  mutate(
#    # magnus equation for relative humidity (based on Celsius values)
#    relative_humidity = get_humidity(tmean, tdmean),
#    # convert temperature to Fahrenheit
#    tmean_f = tmean*(9/5) + 32,
#    # ccalculate heat index
#    heat_index = Get_heat_index(tmean_f, relative_humidity)
    
#  ) 

# calculate relative humidity (Celcius)



