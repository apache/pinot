package com.linkedin.thirdeye.impl.storage;

import java.util.Properties;

public class IndexMetadata {

  /**
   * Min/max Time from the data
   */
  private Long minDataTime;
  private Long maxDataTime;
  private Long minDataTimeMillis;
  private Long maxDataTimeMillis;
  /**
   * Wall clock time of the data, Note wall clock time is non overlapping across two indexes of the
   * same granularity (hourly, daily, weekly, monthly)
   */
  private Long startTime;
  private Long startTimeMillis;
  private Long endTime;
  private Long endTimeMillis;

  // The schedule at which the segment was generated (e.g. MONTHLY, DAILY)
  private String timeGranularity;
  // The unit at which data is aggregated in the segment
  private String aggregationGranularity;
  // The number of aggregationGranularity units (e.g. 10 MINUTES)
  private int bucketSize;
  
  private IndexFormat indexFormat;

  public IndexMetadata(Long minDataTime, Long maxDataTime, Long minDataTimeMillis,
      Long maxDataTimeMillis, String aggregationGranularity, int bucketSize, 
      IndexFormat indexFormat) {
    this.minDataTime = minDataTime;
    this.maxDataTime = maxDataTime;
    this.minDataTimeMillis = minDataTimeMillis;
    this.maxDataTimeMillis = maxDataTimeMillis;
    this.aggregationGranularity = aggregationGranularity;
    this.bucketSize = bucketSize;
    this.indexFormat = indexFormat;
  }

  public IndexMetadata() {

  }

  public IndexMetadata(Long minDataTime, Long maxDataTime, Long minDataTimeMillis,
      Long maxDataTimeMillis, Long startTime, Long endTime, Long startTimeMillis,
      Long endTimeMillis, String timeGranularity, String aggregationGranularity, int bucketSize, 
      IndexFormat indexFormat) {
    this.minDataTime = minDataTime;
    this.maxDataTime = maxDataTime;
    this.minDataTimeMillis = minDataTimeMillis;
    this.maxDataTimeMillis = maxDataTimeMillis;
    this.startTime = startTime;
    this.endTime = endTime;
    this.startTimeMillis = startTimeMillis;
    this.endTimeMillis = endTimeMillis;
    this.timeGranularity = timeGranularity;
    this.aggregationGranularity = aggregationGranularity;
    this.bucketSize = bucketSize;
    this.indexFormat = indexFormat;
  }

  public Long getStartTime() {
    return startTime;
  }

  public Long getEndTime() {
    return endTime;
  }

  public void setEndTime(Long endTime) {
    this.endTime = endTime;
  }

  public String getTimeGranularity() {
    return timeGranularity;
  }

  public void setTimeGranularity(String timeGranularity) {
    this.timeGranularity = timeGranularity;
  }

  public void setStartTime(Long startTime) {
    this.startTime = startTime;
  }

  public Long getMinDataTime() {
    return minDataTime;
  }

  public void setMinDataTime(Long minDataTime) {
    this.minDataTime = minDataTime;
  }

  public Long getMaxDataTime() {
    return maxDataTime;
  }

  public void setMaxDataTime(Long maxDataTime) {
    this.maxDataTime = maxDataTime;
  }

  public Long getMinDataTimeMillis() {
    return minDataTimeMillis;
  }

  public void setMinDataTimeMillis(Long minDataTimeMillis) {
    this.minDataTimeMillis = minDataTimeMillis;
  }

  public Long getMaxDataTimeMillis() {
    return maxDataTimeMillis;
  }

  public void setMaxDataTimeMillis(Long maxDataTimeMillis) {
    this.maxDataTimeMillis = maxDataTimeMillis;
  }

  public Long getStartTimeMillis() {
    return startTimeMillis;
  }

  public void setStartTimeMillis(Long startTimeMillis) {
    this.startTimeMillis = startTimeMillis;
  }

  public Long getEndTimeMillis() {
    return endTimeMillis;
  }

  public void setEndTimeMillis(Long endTimeMillis) {
    this.endTimeMillis = endTimeMillis;
  }

  public String getAggregationGranularity() {
    return aggregationGranularity;
  }

  public void setAggregationGranularity(String aggregationGranularity) {
    this.aggregationGranularity = aggregationGranularity;
  }

  public int getBucketSize() {
    return bucketSize;
  }

  public void setBucketSize(int bucketSize) {
    this.bucketSize = bucketSize;
  }

  public IndexFormat getIndexFormat() {
    return indexFormat;
  }

  public void setIndexFormat(IndexFormat indexFormat) {
    this.indexFormat = indexFormat;
  }

  public Properties toPropertiesBootstrap() {
    Properties properties = new Properties();
    properties.setProperty(StarTreeMetadataProperties.MIN_DATA_TIME, Long.toString(minDataTime));
    properties.setProperty(StarTreeMetadataProperties.MAX_DATA_TIME, Long.toString(maxDataTime));
    properties.setProperty(StarTreeMetadataProperties.MIN_DATA_TIME_MILLIS,
        Long.toString(minDataTimeMillis));
    properties.setProperty(StarTreeMetadataProperties.MAX_DATA_TIME_MILLIS,
        Long.toString(maxDataTimeMillis));
    properties.setProperty(StarTreeMetadataProperties.AGGREGATION_GRANULARITY,
        aggregationGranularity);
    properties.setProperty(StarTreeMetadataProperties.BUCKET_SIZE, Integer.toString(bucketSize));
    properties.setProperty(StarTreeMetadataProperties.INDEX_FORMAT, indexFormat.name());

    return properties;

  }

  public Properties toProperties() {
    Properties properties = new Properties();
    properties.setProperty(StarTreeMetadataProperties.MIN_DATA_TIME, Long.toString(minDataTime));
    properties.setProperty(StarTreeMetadataProperties.MAX_DATA_TIME, Long.toString(maxDataTime));
    properties.setProperty(StarTreeMetadataProperties.MIN_DATA_TIME_MILLIS,
        Long.toString(minDataTimeMillis));
    properties.setProperty(StarTreeMetadataProperties.MAX_DATA_TIME_MILLIS,
        Long.toString(maxDataTimeMillis));
    properties.setProperty(StarTreeMetadataProperties.START_TIME, Long.toString(startTime));
    properties.setProperty(StarTreeMetadataProperties.END_TIME, Long.toString(endTime));
    properties.setProperty(StarTreeMetadataProperties.START_TIME_MILLIS,
        Long.toString(startTimeMillis));
    properties
        .setProperty(StarTreeMetadataProperties.END_TIME_MILLIS, Long.toString(endTimeMillis));
    properties.setProperty(StarTreeMetadataProperties.TIME_GRANULARITY, timeGranularity);
    properties.setProperty(StarTreeMetadataProperties.AGGREGATION_GRANULARITY,
        aggregationGranularity);
    properties.setProperty(StarTreeMetadataProperties.BUCKET_SIZE, Integer.toString(bucketSize));
    properties.setProperty(StarTreeMetadataProperties.INDEX_FORMAT, indexFormat.name());

    return properties;
  }

  public static IndexMetadata fromPropertiesBootstrap(Properties properties) {
    IndexMetadata indexMetadata = new IndexMetadata();

    String minDataTime = properties.getProperty(StarTreeMetadataProperties.MIN_DATA_TIME);
    String maxDataTime = properties.getProperty(StarTreeMetadataProperties.MAX_DATA_TIME);
    String minDataTimeMillis =
        properties.getProperty(StarTreeMetadataProperties.MIN_DATA_TIME_MILLIS);
    String maxDataTimeMillis =
        properties.getProperty(StarTreeMetadataProperties.MAX_DATA_TIME_MILLIS);
    String aggregationGranularity =
        properties.getProperty(StarTreeMetadataProperties.AGGREGATION_GRANULARITY);
    String bucketSize = properties.getProperty(StarTreeMetadataProperties.BUCKET_SIZE);
    IndexFormat indexFormat = IndexFormat.valueOf(properties.getProperty(StarTreeMetadataProperties.INDEX_FORMAT, IndexFormat.FIXED_SIZE.name()));
    if (minDataTime == null || maxDataTime == null || minDataTimeMillis == null
        || maxDataTimeMillis == null || aggregationGranularity == null || bucketSize == null) {
      throw new IllegalStateException("Cannot find required metadata properties");
    }
    indexMetadata.setMinDataTime(Long.valueOf(minDataTime));
    indexMetadata.setMaxDataTime(Long.valueOf(maxDataTime));
    indexMetadata.setMinDataTimeMillis(Long.valueOf(minDataTimeMillis));
    indexMetadata.setMaxDataTimeMillis(Long.valueOf(maxDataTimeMillis));
    indexMetadata.setAggregationGranularity(aggregationGranularity);
    indexMetadata.setBucketSize(Integer.valueOf(bucketSize));
    indexMetadata.setIndexFormat(indexFormat);
    return indexMetadata;

  }

  private static String getAndCheck(Properties properties, String name) {
    String value = properties.getProperty(name);
    if (value == null) {
      throw new IllegalStateException("Cannot find required metadata property: " + name);
    }
    return value;
  }

  public static IndexMetadata fromProperties(Properties properties) {
    IndexMetadata indexMetadata = new IndexMetadata();

    String minDataTime = getAndCheck(properties, StarTreeMetadataProperties.MIN_DATA_TIME);
    String maxDataTime = getAndCheck(properties, StarTreeMetadataProperties.MAX_DATA_TIME);
    String minDataTimeMillis =
        getAndCheck(properties, StarTreeMetadataProperties.MIN_DATA_TIME_MILLIS);
    String maxDataTimeMillis =
        getAndCheck(properties, StarTreeMetadataProperties.MAX_DATA_TIME_MILLIS);
    String startTime = getAndCheck(properties, StarTreeMetadataProperties.START_TIME);
    String endTime = getAndCheck(properties, StarTreeMetadataProperties.END_TIME);
    String startTimeMillis = getAndCheck(properties, StarTreeMetadataProperties.START_TIME_MILLIS);
    String endTimeMillis = getAndCheck(properties, StarTreeMetadataProperties.END_TIME_MILLIS);
    String timeGranularity = getAndCheck(properties, StarTreeMetadataProperties.TIME_GRANULARITY);
    String aggregationGranularity =
        getAndCheck(properties, StarTreeMetadataProperties.AGGREGATION_GRANULARITY);
    String bucketSize = getAndCheck(properties, StarTreeMetadataProperties.BUCKET_SIZE);
    IndexFormat indexFormat = IndexFormat.valueOf(properties.getProperty(StarTreeMetadataProperties.INDEX_FORMAT, IndexFormat.FIXED_SIZE.name()));

    indexMetadata.setMinDataTime(Long.valueOf(minDataTime));
    indexMetadata.setMaxDataTime(Long.valueOf(maxDataTime));
    indexMetadata.setMinDataTimeMillis(Long.valueOf(minDataTimeMillis));
    indexMetadata.setMaxDataTimeMillis(Long.valueOf(maxDataTimeMillis));
    indexMetadata.setStartTime(Long.valueOf(startTime));
    indexMetadata.setEndTime(Long.valueOf(endTime));
    indexMetadata.setStartTimeMillis(Long.valueOf(startTimeMillis));
    indexMetadata.setEndTimeMillis(Long.valueOf(endTimeMillis));
    indexMetadata.setTimeGranularity(timeGranularity);
    indexMetadata.setAggregationGranularity(aggregationGranularity);
    indexMetadata.setBucketSize(Integer.valueOf(bucketSize));
    indexMetadata.setIndexFormat(indexFormat);

    return indexMetadata;

  }

  @Override
  public String toString() {
    return String.format(
        "minDataTime:%s, maxDataTime:%s, startTime:%s, endTime:%s, timeGranularity:%s, ",
        minDataTime, maxDataTime, startTime, endTime, timeGranularity);
  }

}
