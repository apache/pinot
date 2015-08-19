thirdeye.query <- function(host, port, sql) {
  # 
  # Queries ThirdEye back-end to generate aggregate time series table.
  #
  # Args:
  #   host: The machine name (e.g. "localhost")
  #   port: The machine port (e.g. 10000)
  #   sql: A SQL statement to be executed by ThirdEye, e.g.
  #       "SELECT AGGREGATE_1_HOURS(m1,m2) FROM collection
  #           WHERE time BETWEEN '2015-04-01T07:00:00Z' AND '2015-04-08T07:00:00Z'
  #           AND d1 = 'd1v1'
  #           GROUP BY d2"
  #
  # Returns:
  #   A table whose columns are ["dimensions"], "time", ["metrics"].
  #
  # Author:
  #   Greg Brandt (gbrandt@linkedin.com)
  #
  require('jsonlite')
  require('utils')

  # Construct URI
  uri <- paste('http://', host, ':', port, '/query/', URLencode(sql), sep='')

  # Do query
  res <- stream_in(url(uri))

  # Compute total rows / columns
  total.cols <- 1 + length(res$metrics[[1]]) + length(res$dimensions[[1]])
  total.rows <- 0
  for (combination in ls(res$data)) {
    total.rows <- total.rows + length(res$data[[combination]])
  }

  # Allocate data frame
  series <- as.data.frame(matrix(nrow=total.rows, ncol=total.cols))
  names(series) <- c(res$dimensions[[1]], "time", res$metrics[[1]])

  # Compose data frame
  i <- 1
  for (combination in ls(res$data)) {
    dimensions <- fromJSON(combination)

    for (time in ls(res$data[[combination]])) {
      j <- 1

      # Dimensions
      for (dimension in dimensions) {
        series[i, j] <- dimension
        j <- j + 1
      }

      # Time
      series[i, j] <- as.numeric(time)
      j <- j + 1

      # Metrics
      for (metric in res$data[[combination]][[time]][[1]]) {
        series[i, j] <- metric
        j <- j + 1
      }

      i <- i + 1
    }
  }

  return(series)
}

thirdeye.schema <- function(host, port, collection) {
  #
  # Queries ThirdEye back-end to generate aggregate time series table.
  #
  # Args:
  #   host: The machine name (e.g. "localhost")
  #   port: The machine port (e.g. 10000)
  #   collection: The collection name (e.g. "myCollection")
  #
  # Returns:
  #   A schema object. To see metrics, `schema$metrics`; dimensions, `schema$dimensions`, etc.
  #
  # Author:
  #   Greg Brandt (gbrandt@linkedin.com)
  #
  require('jsonlite')
  require('utils')

  # Construct URI
  uri <- paste('http://', host, ':', port, '/collections/', URLencode(collection), sep='')

  # Do query
  res <- stream_in(url(uri))

  return(res)
}
