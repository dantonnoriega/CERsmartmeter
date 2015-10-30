#' Useful functions for cleaning problems with the CER data. Used AFTER one has imported
#' the raw data using get_cer(). It will first fill in any
#'
#' @param DT_KWH (data.table) table with CER consumption data
balance_kwh <- function(DT_KWH) {

  message('balancing data...')
  # modal value of 30 min interval counts
  un <- DT_KWH[, .(N = uniqueN(date_cer)), by = 'id']
  modal <- which.max(tabulate(un$N))
  modal_ids <- unique(un[N==modal]$id)
  # get dates
  full_dates <- unique(DT_KWH[id==modal_ids[1]]$date_cer)
  # get ids
  all_ids <- unique(DT_KWH$id)
  # create balanced data.table
  NT <- rbindlist(lapply(all_ids, function(x) data.table(cbind(x, full_dates))))
  setnames(NT, names(NT), c("id", "date_cer"))
  setkey(NT, id, date_cer)
  setkey(DT_KWH, id, date_cer)
  DT_bal <- merge(NT, DT_KWH, by = c("id", "date_cer"), all.x=TRUE)
  message('removing any ids with more than 5 days of missing data...')
  # drop IDs with a lot of missing data
  DT_na <- DT_bal[, .(N=sum(is.na(kwh))), by = "id"]
  DT_na <- DT_na[, days:=N/48]
  # 5 day cut off
  keep_ids <- unique(DT_na[days<=5]$id)
  drop_ids <- unique(DT_na[days>5]$id)
  DT2 <- DT_bal[!id %in% drop_ids]
  message(length(drop_ids), ' ids removed.')
  return(DT2)

}

#' clean residential kwh data.
clean_residential_kwh <- function(DT_KWH) {

  CORES <- detectCores() - 1

  # check to see if residential only data
  if(!"tar_stim" %in% names(DT_KWH)) {
    DT1 <- merge(DT_KWH, cer_assign, by = "id")
  }
  # balance data
  DT2 <- balance_kwh(DT1)[, .(id, date_cer, kw, kwh)]
  setkey(DT2, id, date_cer)
  weekdays <- cer_ts[weekday>0, .(date_cer, weekday)]
  weekends <- cer_ts[weekday<1, .(date_cer, weekday)]

  # imput weekdays
  ## data table to search
  DT_wkdy <- merge(DT2, weekdays, by = 'date_cer')
  DT_wkdy[, hour_cer := date_cer %%100]
  DT_wkdy[, day_cer  := (date_cer - hour_cer)/100]
  setkey(DT_wkdy, id, date_cer)

  ## create weekday indx
  ids <- unique(DT_wkdy$id)
  dates <- unique(DT_wkdy$date_cer)
  days <- unique((dates - dates%%100)/100)
  N <- length(ids)
  T <- length(dates)
  NT <- matrix(DT_wkdy$kwh,nrow=T,ncol=N) # rows = T, cols = N
  indx <- which(is.na(NT), arr.ind = TRUE) # find r,c index of missing vals
  indx <- as.data.table(indx)
  indx[, id := ids[col]]
  indx[, date_cer := dates[row]]
  indx[, hour_cer := date_cer %%100]
  indx[, day_cer  := (date_cer - hour_cer)/100]
  setkey(DT_wkdy, id, hour_cer, day_cer)
  setkey(indx, id, hour_cer, day_cer)
  imputs <- mapply(function(x, y, z) {
    j <- which(days %in% z) # match to cer_days vector
    past_days <- days[(j-1):(j-10)] # sequence back 5 weekdays
    past_kwh <- vapply(past_days, function(i) DT_wkdy[.(x,y,i)]$kwh, numeric(1))
    avg10 <- mean(past_kwh, na.rm=TRUE) # avg of past 10 readings at given hour
    DT_wkdy[.(x,y,z), kwh:=avg10]
  }, x=indx$id, y=indx$hour_cer, z=indx$day_cer)



}

#' find zeros or missing kwh data.
imput_kwh_nas <- function(DT_KWH) {

  # check to see if residential only data
  if(!"tar_stim" %in% names(DT_KWH)) {
    DT1 <- merge(DT_KWH, cer_assign, by = "id")
  }

  # find index of zeros
  setkey(DT1, id, date_cer)
  zs  <- DT1[, .(indx=which(kwh==0))]
  zs1 <- zs[, .(indx=indx+1)]
  zs_1<- zs[, .(indx=indx-1)]
  Z <- unique(rbindlist(list(zs, zs1, zs_1)))
  setkey(Z, indx)
  Z <- Z$indx
  h <- hist(DT1[Z][kwh>0]$kwh) # get histogram object of nonzero values
  nms <- paste(h$breaks[-length(h$breaks)], h$breaks[-1], sep="-")
  counts <- h$counts
  names(counts) <- nms
}

#' find zeros or missing kwh data.
find_zeros_or_nas <- function(DT_KWH) {

  # check to see if residential only data
  if(!"tar_stim" %in% names(DT_KWH)) {
    DT1 <- merge(DT_KWH, cer_assign, by = "id")
  }

  # find index of zeros
  setkey(DT1, id, date_cer)
  zs  <- DT1[, .(indx=which(kwh==0))]
  zs1 <- zs[, .(indx=indx+1)]
  zs_1<- zs[, .(indx=indx-1)]
  Z <- unique(rbindlist(list(zs, zs1, zs_1)))
  setkey(Z, indx)
  Z <- Z$indx
  h <- hist(DT1[Z][kwh>0]$kwh) # get histogram object of nonzero values
  nms <- paste(h$breaks[-length(h$breaks)], h$breaks[-1], sep="-")
  counts <- h$counts
  names(counts) <- nms
}


