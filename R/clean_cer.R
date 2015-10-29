#' Useful functions for cleaning problems with the CER data. Used AFTER one has imported
#' the raw data using get_cer(). It will first fill in any
#'
#' @param DT_KWH (data.table) table with CER consumption data
balance_kwh <- function(DT_KWH) {

  message('balancing data...')
  # get ids
  all_ids <- unique(DT_KWH$id)
  # get dates
  full_dates <- unique(DT_KWH[id==keep_ids[1]]$date_cer)
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
  message('mode of observations per id = ', modal)

  DT2 <- DT_KWH[!id %in% drop_ids]
  message(length(drop_ids), ' ids removed.')
  return(DT2)

}

#' @param DT_KWH (data.table) table with CER consumption data
clean_residential_kwh <- function(DT_KWH) {

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
