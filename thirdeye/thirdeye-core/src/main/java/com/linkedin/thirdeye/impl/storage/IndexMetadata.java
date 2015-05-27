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
   * Wall clock time of the data, Note wall clock time is non overlapping across two indexes of the same granularity (hourly, daily, weekly, monthly)
   */
  private Long startTime;
  private Long startTimeMillis;
  private Long endTime;
  private Long endTimeMillis;

  private String timeGranularity;

  private String aggregationGranularity;
  private int bucketSize;

  public IndexMetadata(Long minDataTime, Long maxDataTime, Long minDataTimeMillis, Long maxDataTimeMillis,
      String aggregationGranularity, int bucketSize) {
    this.minDataTime = minDataTime;
    this.maxDataTime = maxDataTime;
    this.minDataTimeMillis = minDataTimeMillis;
    this.maxDataTimeMillis = maxDataTimeMillis;
    this.aggregationGranularity = aggregationGranularity;
    this.bucketSize = bucketSize;
  }

  public IndexMetadata() {

  }

  public IndexMetadata(Long minDataTime, Long maxDataTime, Long minDataTimeMillis, Long maxDataTimeMillis,
      Long startTime, Long endTime, Long startTimeMillis, Long endTimeMillis,
      String timeGranularity, String aggregationGranularity, int bucketSize) {
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


  public Properties toPropertiesBootstrap()
  {
    Properties properties = new Properties();
    properties.setProperty(StarTreeMetadataProperties.MIN_DATA_TIME, Long.toString(minDataTime));
    properties.setProperty(StarTreeMetadataProperties.MAX_DATA_TIME, Long.toString(maxDataTime));
    properties.setProperty(StarTreeMetadataProperties.MIN_DATA_TIME_MILLIS, Long.toString(minDataTimeMillis));
    properties.setProperty(StarTreeMetadataProperties.MAX_DATA_TIME_MILLIS, Long.toString(maxDataTimeMillis));
    properties.setProperty(StarTreeMetadataProperties.AGGREGATION_GRANULARITY, aggregationGranularity);
    properties.setProperty(StarTreeMetadataProperties.BUCKET_SIZE, Integer.toString(bucketSize));

    return properties;

  }

  public Properties toProperties()
  {
    Properties properties = new Properties();
    properties.setProperty(StarTreeMetadataProperties.MIN_DATA_TIME, Long.toString(minDataTime));
    properties.setProperty(StarTreeMetadataProperties.MAX_DATA_TIME, Long.toString(maxDataTime));
    properties.setProperty(StarTreeMetadataProperties.MIN_DATA_TIME_MILLIS, Long.toString(minDataTimeMillis));
    properties.setProperty(StarTreeMetadataProperties.MAX_DATA_TIME_MILLIS, Long.toString(maxDataTimeMillis));
    properties.setProperty(StarTreeMetadataProperties.START_TIME, Long.toString(startTime));
    properties.setProperty(StarTreeMetadataProperties.END_TIME, Long.toString(endTime));
    properties.setProperty(StarTreeMetadataProperties.START_TIME_MILLIS, Long.toString(startTimeMillis));
    properties.setProperty(StarTreeMetadataProperties.END_TIME_MILLIS, Long.toString(endTimeMillis));
    properties.setProperty(StarTreeMetadataProperties.TIME_GRANULARITY, timeGranularity);
    properties.setProperty(StarTreeMetadataProperties.AGGREGATION_GRANULARITY, aggregationGranularity);
    properties.setProperty(StarTreeMetadataProperties.BUCKET_SIZE, Integer.toString(bucketSize));

    return properties;
  }

  public static IndexMetadata fromPropertiesBootstrap(Properties properties)
  {
    IndexMetadata indexMetadata = new IndexMetadata();

    String minDataTime = properties.getProperty(StarTreeMetadataProperties.MIN_DATA_TIME);
    String maxDataTime = properties.getProperty(StarTreeMetadataProperties.MAX_DATA_TIME);
    String minDataTimeMillis = properties.getProperty(StarTreeMetadataProperties.MIN_DATA_TIME_MILLIS);
    String maxDataTimeMillis = properties.getProperty(StarTreeMetadataProperties.MAX_DATA_TIME_MILLIS);
    String aggregationGranularity = properties.getProperty(StarTreeMetadataProperties.AGGREGATION_GRANULARITY);
    String bucketSize = properties.getProperty(StarTreeMetadataProperties.BUCKET_SIZE);

    if (minDataTime == null || maxDataTime == null || minDataTimeMillis == null || maxDataTimeMillis == null
        || aggregationGranularity == null || bucketSize == null)    {
      throw new IllegalStateException("Cannot find required metadata properties");
    }
    indexMetadata.setMinDataTime(Long.valueOf(minDataTime));
    indexMetadata.setMaxDataTime(Long.valueOf(maxDataTime));
    indexMetadata.setMinDataTimeMillis(Long.valueOf(minDataTimeMillis));
    indexMetadata.setMaxDataTimeMillis(Long.valueOf(maxDataTimeMillis));
    indexMetadata.setAggregationGranularity(aggregationGranularity);
    indexMetadata.setBucketSize(Integer.valueOf(bucketSize));

    return indexMetadata;

  }


  public static IndexMetadata fromProperties(Properties properties)
  {
    IndexMetadata indexMetadata = new IndexMetadata();

    String minDataTime = properties.getProperty(StarTreeMetadataProperties.MIN_DATA_TIME);
    String maxDataTime = properties.getProperty(StarTreeMetadataProperties.MAX_DATA_TIME);
    String minDataTimeMillis = properties.getProperty(StarTreeMetadataProperties.MIN_DATA_TIME_MILLIS);
    String maxDataTimeMillis = properties.getProperty(StarTreeMetadataProperties.MAX_DATA_TIME_MILLIS);
    String startTime = properties.getProperty(StarTreeMetadataProperties.START_TIME);
    String endTime = properties.getProperty(StarTreeMetadataProperties.END_TIME);
    String startTimeMillis = properties.getProperty(StarTreeMetadataProperties.START_TIME_MILLIS);
    String endTimeMillis = properties.getProperty(StarTreeMetadataProperties.END_TIME_MILLIS);
    String timeGranularity = properties.getProperty(StarTreeMetadataProperties.TIME_GRANULARITY);
    String aggregationGranularity = properties.getProperty(StarTreeMetadataProperties.AGGREGATION_GRANULARITY);
    String bucketSize = properties.getProperty(StarTreeMetadataProperties.BUCKET_SIZE);

    if (minDataTime == null || maxDataTime == null || minDataTimeMillis == null || maxDataTimeMillis == null
        || startTime == null || endTime == null ||  startTimeMillis == null || endTimeMillis == null
        || timeGranularity == null|| aggregationGranularity == null || bucketSize == null)    {
      throw new IllegalStateException("Cannot find required metadata properties");
    }
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

    return indexMetadata;

  }

  @Override
  public String toString() {
    return String.format(
        "minDataTime:%s, maxDataTime:%s, startTime:%s, endTime:%s, timeGranularity:%s, ",
        minDataTime, maxDataTime, startTime, endTime, timeGranularity);
  }

}
