package com.linkedin.thirdeye.anomaly.utils;

/**
 * Data source request performance log entry
 */
final class RequestLogEntry {
  final String datasource;
  final String dataset;
  final String metric;
  final boolean success;
  final long start;
  final long end;
  final Exception exception;

  public RequestLogEntry(String datasource, String dataset, String metric, boolean success, long start, long end,
      Exception exception) {
    this.datasource = datasource;
    this.dataset = dataset;
    this.metric = metric;
    this.success = success;
    this.start = start;
    this.end = end;
    this.exception = exception;
  }

  public String getDatasource() {
    return datasource;
  }

  public String getDataset() {
    return dataset;
  }

  public String getMetric() {
    return metric;
  }

  public boolean isSuccess() {
    return success;
  }

  public long getStart() {
    return start;
  }

  public long getEnd() {
    return end;
  }

  public Exception getException() {
    return exception;
  }
}
