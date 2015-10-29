#' Imports and returns a cleaned data table of the ISSDA's CER Smart Meter Data. The returned object can be huge (up to 8 gb of RAM). The package comes with default data tables but if they are missing, source the ISSDA_CER_Smart_Metering_Data folder from Dropbox.
#'
#' @param cer_dir path to folder of CER Smart Meter Data and support files.
#' @param only_kwh trigger to import consumption data with assignment and timeseries data only.
#' @param yr specify years.
#' @param mo specify months.
#' @param hr specify hour.
#' @return A data table of CER Smart Meter data.
#' @examples
#' # get 2009 data, kwh data only (much smaller but still large)
#' get_cer(cer_dir="~/Dropbox/ISSDA_CER_Smart_Metering_Data", only_kwh=TRUE, yr = 2009)
get_cer <- function(cer_dir="~/Dropbox/ISSDA_CER_Smart_Metering_Data/",
                    only_kwh=TRUE,
                    yr = NULL,
                    mo = NULL,
                    hr = NULL) {

  data_dir <- file.path(cer_dir, "data")
  extdata <- system.file("extdata", "cer_kwh.csv.gz", package = "cersmartmeter")

  # IMPORT DATA ---------
  ## consumption data
  import <- function(...) {
    message("importing consumption data...")
    if(dir.exists(data_dir)) {
      files <- list.files(data_dir, pattern = "^File.*txt$", full.names = T)
      dts <- lapply(files, fread, sep = " ") # loop through each path and run 'fread' (data.tables import)
      DT <- rbindlist(dts) # stack data
      rm('dts')
      setnames(DT, names(DT), c('id', 'date_cer', 'kw')) # rename data
      setkey(DT, id, date_cer) # set key to 'id'
    } else {
      if(file.exists(extdata)) {
        cmd <- paste('zcat <', extdata)
        DT <- fread(input = cmd)
      } else {
        stop('No CER residential consumption data source')
      }
    }
    return(DT)
  }

  DT <- import()

  # CREATE NEW VARIABLES ---------------
  message("creating new variables...")
  DT[, kwh := kw*.5] # assuming data is in kw, this creates kwh

  ## assignment data
  message("importing assignment data and time data...")
  try(if(!exists('cer_assign')) dt_assign <- get_assign(cer_dir) else dt_assign <- cer_assign)
  try(if(!exists('cer_ts')) dt_ts <- get_ts(cer_dir) else dt_ts <- cer_ts)

  # REDUCE DATATABLE SIZE --------
  ## pass option to only keep certain years, months, hours
  message("reducing datatable size...")
  if(!is.null(yr)) dt_ts <- dt_ts[year %in% yr]
  if(!is.null(mo)) dt_ts <- dt_ts[month %in% mo]
  if(!is.null(hr)) dt_ts <- dt_ts[hour %in% hr]

  # check for date reductions of any kind and update
  if(!all(is.null(yr), is.null(mo), is.null(hr))) {
    keep <- unique(dt_ts$date_cer)
    DT <- DT[date_cer %in% keep]
  }

  if(!only_kwh) {
    # MERGE DATA ------------------
    ## merge assignments
    DT <- merge(DT, dt_assign, by = "id")
    DT[, code:=NULL]

    ## merge time series data
    DT <- merge(DT, dt_ts, by = "date_cer")

    message("merging weather and survey data...")
    # WEATHER AND SURVEY DATA ----------------
    try(if(!exists('cer_weather')) dt_weather <- get_weather(cer_dir) else dt_weather <- cer_weather)
    if(!is.null(yr)) dt_weather <- dt_weather[year %in% yr] # only want certain year

    try(if(!exists('cer_survey')) dt_srvy <- get_srvy(cer_dir) else dt_srvy <- cer_survey)

    # MERGE SURVEY AND WEATHER DATA ---------------
    DT = merge(DT, dt_weather, by = c('year', 'month', 'day', 'hour', 'tz'))
    DT = merge(DT, dt_srvy, by = 'id', all.x=TRUE)
  }

  message("...done.")
  return(DT)

}


get_survey <- function(cer_dir = "~/Dropbox/ISSDA_CER_Smart_Metering_Data/") {

  data_dir <- file.path(cer_dir, "data")

  try(if(!file.exists(file.path(data_dir, "cer_pretrial_survey_redux.csv"))) {
    stop("dt_pretrial_survey_redux.csv does not exists. run 'gen_survey_data.py'\n
         (requires python)")
  })

  nas <- c("NA", "", ".")
  files <- list.files(data_dir, pattern = "cer_pretrial.*.csv", full.names = T)
  srvy <- fread(files, sep = ",", header = TRUE, na.strings = nas)
  nms = sapply(names(srvy), str_replace, "\\.0", "") # remove the ".0" in names
  nms = tolower(nms)
  setnames(srvy, names(srvy), nms)

  return(srvy)
  }

get_weather <- function(cer_dir = "~/Dropbox/ISSDA_CER_Smart_Metering_Data/") {

  data_dir <- file.path(cer_dir, "weather")

  files <- list.files(data_dir, pattern = "hl*", full.names = T)
  weather <- lapply(files, fread, sep = ",", header = TRUE)
  weather <- rbindlist(weather, fill=TRUE) # stack weather options
  setnames(weather, "Date (utc)", "date")

  # reformat date variable
  utc <- ymd_hms(strptime(weather$date, "%d-%b-%Y %H:%M", tz="utc"))
  weather[, date:=NULL] # delete old date
  dublin <- format(utc, tz='Europe/Dublin')
  tzone <- format(as.POSIXct(dublin, tz="Europe/Dublin"), "%Z")
  weather[, `:=`(date = dublin,
                 year = year(dublin),
                 month = month(dublin),
                 week = week(dublin),
                 day = day(dublin),
                 hour = hour(dublin),
                 min = minute(dublin),
                 tz = tzone)]

  weather <- weather[, list(temp = mean(temp), dewpt = mean(dewpt), rhum = mean(rhum)),
                     by = c('year', 'month', 'day', 'hour', 'tz')]
  # add squared values
  weather[, `:=`(temp2 = temp^2, dewpt2 = dewpt^2, rhum2 = rhum^2)]
  weather[, `:=`(temp3 = temp^3, dewpt3 = dewpt^3, rhum3 = rhum^3)]

  return(weather)
}

get_assign <- function(cer_dir = "~/Dropbox/ISSDA_CER_Smart_Metering_Data/") {

  data_dir <- file.path(cer_dir, "data")
  nas <- c("NA", "", ".")
  assignments <- list.files(data_dir, pattern = "^SME.*csv$", full.names = T)
  dt_assign <- fread(assignments, sep = ',', select = c(1:4), na.strings = nas)
  setnames(dt_assign, names(dt_assign), c('id', 'code', 'tariff', 'stimulus')) # change
  setkey(dt_assign, tariff)
  dt_assign["b", tariff:="B"] # fix lowercase b's
  setkey(dt_assign, id)
  dt_assign <- dt_assign[code == 1] # subset the data to residential only
  dt_assign[, tar_stim := paste0(tariff, stimulus)]
  dt_assign[, `:=`(tariff=NULL, stimulus=NULL)] # drop redundant vars

  return(dt_assign)
}

get_ts <- function(cer_dir = "~/Dropbox/ISSDA_CER_Smart_Metering_Data/") {

  data_dir <- file.path(cer_dir, "data")

  ## time series correction
  ts <- list.files(data_dir, pattern = "^dst.*csv$", full.names = T)
  dt_ts <- fread(ts, sep = ',')
  dt_ts[, ts:=NULL]
  dt_ts <- dt_ts[day_cer > 194]
  dt_ts[, date_cer:=day_cer*100 + hour_cer]
  setkey(dt_ts, date_cer)

  # ADD DAY OF WEEK ------------------------------------------------
  weeks_T <- unique(dt_ts[, .(date, year)])[, `:=`(week=week(date),
                                                dow=wday(as.Date(date, "%Y-%m-%d")))]
  weeks_T[, weekday:= 0 + !(dow == 1 | dow ==7)] # sunday = 1
  setkey(weeks_T, year, week)
  weeks_T2 <- unique(weeks_T[, .(week, year)])[, T_wk:=seq_along(week)]
  weeks_T <- merge(weeks_T, weeks_T2, by = c("week", "year"))
  weeks_T[, year:=NULL]
  setkey(dt_ts, date)
  setkey(weeks_T, date)
  dt_ts <- dt_ts[weeks_T]
  setkey(dt_ts, hour, weekday)
  dt_ts[, peak:=0]
  dt_ts[.(c(5,6,7), 1), peak:=1]
  setkey(dt_ts, date_cer)
  return(dt_ts)
}


