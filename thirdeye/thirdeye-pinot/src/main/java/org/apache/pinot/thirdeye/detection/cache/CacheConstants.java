package org.apache.pinot.thirdeye.detection.cache;

/*
  Constants used for centralized-cache related code.
 */
public class CacheConstants {
  public static final String TIMESTAMP = "timestamp";

  // couchbase document field names
  public static final String BUCKET = "bucket";
  public static final String TIME = "time";
  public static final String METRIC_ID = "metricId";
  public static final String START = "start";
  public static final String END = "end";

  // refers to hashed metricURN which is used as the key to data for a dimension combination.
  public static final String DIMENSION_KEY = "dimensionKey";

}
