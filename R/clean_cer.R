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

  # check to see if residential only data
  if(!"tar_stim" %in% names(DT_KWH)) {
    DT1 <- merge(DT_KWH, cer_assign, by = "id")
  }
  # balance data
  DT2 <- balance_kwh(DT1)[, .(id, date_cer, kw, kwh)]
  N <- uniqueN(DT2$id)
  T <- uniqueN(DT2$date_cer)
  NT <- matrix(DT2$kwh,nrow=T,ncol=N) # rows = T, cols = N
  indx <- which(is.na(NT), arr.ind = TRUE) # find r,c index of missing vals
  indx_f <- cbind(indx[,1] + 1, indx[,2])
  indx_b <- cbind(indx[,1] - 1, indx[,2])
  a <- indx_f[,1]
  b <- indx_b[,1]

  diff1 <- indx_f[!which(indx_f[,1] %in% indx[,1]),]
  diff2 <- indx_b[!which(indx_b[,1] %in% indx_f[,1]),] # find differences
  indx <- do.call(rbind, list(indx_b, indx_f))
  NT[indx]
  # imput any missing data
#   DT2[, hour_cer:=date_cer%%100]
#   DT2[, day_cer:=(date_cer - date_cer%%100)/100]
  # imput avgs
  ns  <- DT2[, .(indx=which(is.na(kwh)))]
  ns1 <- ns[, .(indx=indx+1)]
  ns_1<- ns[, .(indx=indx-1)]
  ns <- unique(rbindlist(list(ns, ns1, ns_1)))
  setkey(ns, indx)
  ns <- ns$indx
  DT2[ns]
  NAs <- DT2[is.na(kwh)]
  NAs[, c(paste0('day_cer_',c(1:5))):=list(day_cer-1,
    day_cer-2, day_cer-3, day_cer-4, day_cer-5)]

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


