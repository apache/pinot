#
# This file contains the R implmentation to remove seasonality using stl.
#

removeSeasonality <- function(time.series.data, seasonal)
{
    ts.data <- ts(time.series.data, frequency = seasonal)

  # seach for the smallest positive correlation (these bounds are somewhat arbitrary)
  rangeVec = seq(2, min(length(ts.data) / 10, 25))
  acfVec = NULL
  for (twin in rangeVec)
  {
      stl.obj = try(stl(ts.data, s.window = "periodic", robust = TRUE, na.action = na.fail, t.window=twin))
      stl.residual = stl.obj$time.series[,"remainder"]
      acfVec = append(acfVec, acf(stl.residual, plot=FALSE)$acf[2])
  }
  optimalTwin = rangeVec[which(acfVec > 0)[1]]

  # run again with optimal params
  stl.obj <- try(stl(ts.data, s.window="periodic", robust=TRUE, na.action=na.fail, t.window=optimalTwin))

    stl.seasonal.factor <- stl.obj$time.series[,"seasonal"]
    stl.trend <- stl.obj$time.series[,"trend"]
    stl.residual <- stl.obj$time.series[,"remainder"]

    # return the series with seasonality removed
    return (stl.trend + stl.residual)
}

# Read command line args
args <- commandArgs(trailingOnly=TRUE)
seasonality <- as.integer(args[1])

# Read the data in from stdin
data <- read.csv(file='stdin', header=TRUE)

ts.data <- ts(data$Series, frequency=seasonality)

stlResult <- removeSeasonality(data$Series, seasonality)

# write the series with seasonality removed to stdout
write.table(stlResult, file='/dev/stdout', sep=',', row.names=FALSE, col.names=FALSE)