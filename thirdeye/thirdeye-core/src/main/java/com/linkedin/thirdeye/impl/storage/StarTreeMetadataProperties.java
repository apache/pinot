package com.linkedin.thirdeye.impl.storage;

public class StarTreeMetadataProperties {

  // Minimum data time in the segment in collection time unit
  public static String MIN_DATA_TIME = "min.data.time";
  // Maximum data time in the segment in collection time unit
  public static String MAX_DATA_TIME = "max.data.time";
  // Minimum data time in the segment in milliseconds
  public static String MIN_DATA_TIME_MILLIS = "min.data.time.millis";
  // Maximum data time in the segment in milliseconds
  public static String MAX_DATA_TIME_MILLIS = "max.data.time.millis";
  // Schedule start time in collection time unit
  public static String START_TIME = "start.time";
  // Schedule end time in collection time unit
  public static String END_TIME = "end.time";
  // Schedule start time in milliseconds
  public static String START_TIME_MILLIS = "start.time.millis";
  // Schedule end time in milliseconds
  public static String END_TIME_MILLIS = "end.time.millis";
  // Hadoop workflow schedule : HOURLY, DAILY, MONTHLY
  public static String TIME_GRANULARITY = "time.granularity";
  // Bucket size of the collection data : HOURS, MINUTES, SECONDS
  public static String AGGREGATION_GRANULARITY = "aggregation.granularity";
  // Bucket unit of the collection data : 1, 10
  public static String BUCKET_SIZE = "bucket.size";
  //Data storage format, FIXED or VARIABLE SIZE
  public static final String INDEX_FORMAT = "index.format";


}
