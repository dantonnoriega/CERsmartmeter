#' Run Two Stage Least Squares
#'
#' @param d endogenous variable (vector)
#' @param z instrument (vector or matrix)
#' @param x exogenous variable (vector or matrix)
#' @param y dependent variable (vector)
#' @return Returns a linear regression model `lm` where the endogenous variable has been replaced by the predicted values of the first stage. That is, the results of a two stage lease squares regression.
tsls <- function(d, z, x, y){
  dhat <- lm(d ~ z + x)$fitted.values
  R <- lm(y ~ dhat + x)
  return(R)
}

mlm.fit <- function (x,y,s,theta)
{
  xm <- PQ(x,s)$Ph
  ym <-tapply(y,s,mean)[s]
  xt <- x - (1 - theta)*xm
  yt <- matrix(y - (1 - theta)*ym)
  lm(yt ~ xt)
}

fetsls <- function (d,z,x,y,s)
{
  xm <- PQ(x,s)$Ph
  ym <-tapply(y,s,mean)[s]
  zm <-tapply(z,s,mean)[s]
  dm <-tapply(d,s,mean)[s]
  xt <- x - xm
  yt <- y - ym
  zt <- z - zm
  dt <- d - dm
  dhat <- lm(dt ~ zt + xt)$fitted.values
  R <- lm(yt ~ dhat + xt)
  return(R)
}

PQ <- function(h, id){
  if(is.vector(h))
    h <- matrix(h, ncol = 1)
  Ph <- unique(id)
  Ph <- cbind(Ph, table(id))
  for(i in 1:ncol(h))
    Ph <- cbind(Ph, tapply(h[, i], id, mean))
  is <- tapply(id, id)
  Ph <- Ph[is, - (1:2)]
  Qh <- h - Ph
  list(Ph=as.matrix(Ph), Qh=as.matrix(Qh), is=is)
}

