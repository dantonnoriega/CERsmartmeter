#' creates a balanced panel of CER data.
#'
#' @param DT_KWH (data.table) table with CER consumption data
#' @param drop_dst (logical) drop Daylight savings observations (data will NOT be perfectly balance if FALSE)
balance_kwh <- function(DT_KWH, drop_dst=TRUE) {

  message('balancing data...')
  # modal value of 30 min interval counts
  # removing duplicated values
  DT_KWH <- DT_KWH[!duplicated(DT_KWH, by = c('id', 'date_cer'), fromLast=TRUE)]
  un <- DT_KWH[, .(N = uniqueN(date_cer)), by = 'id']
  modal <- which.max(tabulate(un$N))
  modal_ids <- unique(un[N==modal]$id)
  # drop ids with 90% of modal obs missing or greater than mode
  drop_ids <- unique(DT_KWH[, .N, by = 'id'][N < modal*.9 | N > modal]$id)
  DT_KWH <- DT_KWH[!(DT_KWH$id %in% drop_ids)]
  # get dates
  full_dates <- unique(DT_KWH[id==modal_ids[1]]$date_cer)
  # get ids
  all_ids <- unique(DT_KWH$id)
  # create balanced data.table
  NT <- rbindlist(lapply(all_ids, function(x) data.table(cbind(x, full_dates))))
  setnames(NT, names(NT), c("id", "date_cer"))
  setkey(NT, id, date_cer)
  setkey(DT_KWH, id, date_cer)
  DT1 <- merge(NT, DT_KWH, all.x=TRUE, by = c("id", "date_cer"))
  message('removing any ids with more than 3 days of missing data...')
  # drop IDs with a lot of missing data
  DT_na <- DT1[, .(N=sum(is.na(kwh))), by = "id"]
  DT_na[, na_days:=N/48]
  # 3 day cut off
  keep_ids <- unique(DT_na[na_days <=3]$id)
  DT_KWH <- DT1[(DT1$id %in% keep_ids)]
  message(length(drop_ids), ' ids removed.')
  return(DT_KWH)

}

#' drop ids with long sequential zero readings or missing values.
#' @param tol tolerable sequence of zeros and missing values
#' @param drop drops ids with long sequence of zeroes
find_seq_zeros_nas <- function(DT_KWH, tol = 10, drop = TRUE) {
  message('searching for long strings of zeros or nas...')
  indx <- DT_KWH[is.na(kwh) | kwh==0]
  indx[, hour_cer := date_cer %%100]
  indx[, day_cer  := (date_cer - hour_cer)/100]
  sq <- indx[, .(ddate=c(11111,diff(date_cer))), by = 'id']
  sq1 <- indx[, .(dday=c(11111,diff(day_cer))), by = 'id']
  sq[, dday:=sq1$dday]
  sq[, s:=0 + (ddate>1)]
  sq[ddate==53 & dday==1, s:=0] # find sequences through diff days
  sq[, s:=cumsum(s)]
  sq <- sq[, .N, by =c('id', 's')]
  if(drop) {
    drop_ids <- unique(sq[N > tol]$id)
    DT1 <- DT_KWH[!(DT_KWH$id %in% drop_ids)]
    message('dropped ', length(drop_ids), ' ids.')
    sq <- sq[N <= tol]
    n <- uniqueN(DT1$id)
    message(n, ' ids remain with sequential zeros or nas less than ', tol, ' in length.')
  } else {
    n <- uniqueN(sq$id)
    message(n, ' ids found with sequential zeros.')
  }
  return(list(DT1, sq))
}

#' Find surges in kwh and smooth them out. Pon (2015) finds that average KWH consumption per household is about 11-12 kwh. It is assumed that a surge, therefore, would be anything about 6 kwh per half hour (12 kwh).
#' @param kwh_surge set value of what is considered a kwh surge
smooth_residential_kwh <- function(DT_KWH, kwh_surge = 6) {

  # check to see if residential only data
  if(!"tar_stim" %in% names(DT_KWH)) {
    DT1 <- merge(DT_KWH, cer_assign, by = "id")
  }
  # find index of high values
  setkey(DT1, id, date_cer)
  s <- DT1[, .(indx=which(kwh>kwh_surge))]
  f <- s[, .(indx=indx+1)]
  b <- s[, .(indx=indx-1)]
  diff1 <- DT1[s$indx]$kwh - DT1[b$indx]$kwh > 1/2*kwh_surge # sudden surge
  diff2 <- DT1[s$indx]$kwh - DT1[f$indx]$kwh > 1/2*kwh_surge # sudden drop
  diff <- diff1 & diff2
  avg <- (DT1[b[diff]$indx]$kwh + DT1[f[diff]$indx]$kwh)/2
  smooth <- s[diff]$indx
  message(length(smooth), ' surge values found and corrected.')
  DT1[smooth, kwh:=avg]
  return(DT1)

}

#' IN TESTING
#' impute residential kwh zeros and missing data along time. imputation can take a LONG time.
impute_residential_kwh_time <- function(DT_KWH) {

  # check to see if residential only data
  if(!"tar_stim" %in% names(DT_KWH)) {
    setkey(cer_assign, id)
    setkey(DT_KWH, id)
    DT1 <- merge(DT_KWH, cer_assign, by = "id")
  }
  # balance data
  DT1 <- balance_kwh(DT1)[, .(id, date_cer, kw, kwh)]
  setkey(DT1, id, date_cer)
  weekdays <- cer_ts[weekday > 0, .(date_cer, weekday)]
  weekends <- cer_ts[weekday < 1, .(date_cer, weekday)]

# IMPUT WEEKDAYS and WEEKENDS ----------------------------------------------------------
  ## data table to search
  DT_wkdy <- merge(DT1, weekdays, by = 'date_cer') # weekday data
  DT_wknd <- merge(DT1, weekends, by = 'date_cer') # weekend data
  # loop
  DTS <- lapply(list(DT_wknd, DT_wkdy), function(DT0) {
    DT0[, hour_cer := date_cer %%100]
    DT0[, day_cer  := (date_cer - hour_cer)/100]
    ## create weekday indx
    setkey(DT0, id, date_cer) # crucial that we do sequentially
    ids <- unique(DT0$id)
    dates <- unique(DT0$date_cer)
    days <- unique((dates - dates%%100)/100)
    N <- length(ids)
    T <- length(dates)
    NT <- matrix(DT0$kwh,nrow=T,ncol=N) # rows = T, cols = N
    indx <- which(is.na(NT) | NT==0, arr.ind = TRUE) # find r,c index of missing vals
    indx <- as.data.table(indx)
    indx[, id := ids[col]]
    indx[, date_cer := dates[row]] # only works if data is sequential!
    indx[, hour_cer := date_cer %%100]
    indx[, day_cer  := (date_cer - hour_cer)/100]
    # find sequence of zeros
    sq <- indx[, .(d=c(2,diff(date_cer))), by = 'id']
    sq[, s:=0 + (d>1)]
    sq[, s:=cumsum(s)]
    # set keys for quick search and column binding
    setkey(DT0, id, hour_cer, day_cer)
    setkey(indx, id, hour_cer, day_cer)
    # parallel search
    if(require(parallel) & .Platform$OS.type == "unix") {
      message('parallel package found on Mac/Linux system. using mcmapply.')
      CORES <- detectCores() - 2
      imputs <- mcmapply(function(x, y, z) { # use mcmapply
        j <- which(days %in% z) # match to cer_days vector
        past_days <- days[(j-1):(j-10)] # sequence back 5 weekdays
        past_kwh <- vapply(past_days, function(i) DT0[.(x,y,i)]$kwh, numeric(1))
        avg10 <- mean(past_kwh, na.rm=TRUE) # avg of past 10 readings at given hour
        return(avg10)
      }, x=indx$id, y=indx$hour_cer, z=indx$day_cer, mc.cores=CORES)
    } else {
      message('parallel NOT package found. NOT using mcmapply.')
      imputs <- mapply(function(x, y, z) { # use mcmapply
        r <- which(days %in% z) # match to cer_days vector
        past_days <- days[(j-1):(j-10)] # sequence back 10 weekdays
        past_kwh <- vapply(past_days, function(i) DT0[.(x,y,i)]$kwh, numeric(1))
        avg10 <- mean(past_kwh, na.rm=TRUE) # avg of past 10 readings at given hour
        return(avg10)
      }, x=indx$id, y=indx$hour_cer, z=indx$day_cer)
    }
    DT0[is.na(kwh) | kwh == 0, kwh:=imputs]
    DT0[is.na(kw)| kwh == 0, kw:=kwh*2]
    return(DT0)
  })

# END IMPUT ------------------------------------------------------------
  DT1 <- rbindlist(DTS)
  setkey(DT1, id, date_cer)
  return(DT1)
}

