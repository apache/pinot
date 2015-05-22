package com.linkedin.thirdeye.impl.storage;

import java.util.Properties;

public class IndexMetadata {
  /**
   * Min/max Time from the data
   */
  private Long minDataTime;
  private Long maxDataTime;
  /**
   * Wall clock time of the data, Note wall clock time is non overlapping across two indexes of the same granularity (hourly, daily, weekly, monthly)
   */
  private Long startTime;

  private Long endTime;

  private String timeGranularity;

  public IndexMetadata(Long minDataTime, Long maxDataTime) {
    this.minDataTime = minDataTime;
    this.maxDataTime = maxDataTime;
  }

  public IndexMetadata() {

  }



  public IndexMetadata(Long minDataTime, Long maxDataTime, Long startTime, Long endTime, String timeGranularity) {
    super();
    this.minDataTime = minDataTime;
    this.maxDataTime = maxDataTime;
    this.startTime = startTime;
    this.endTime = endTime;
    this.timeGranularity = timeGranularity;
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

  public Properties toProperties()
  {
    Properties properties = new Properties();
    properties.setProperty(StarTreeMetadataProperties.MIN_DATA_TIME, Long.toString(minDataTime));
    properties.setProperty(StarTreeMetadataProperties.MAX_DATA_TIME, Long.toString(maxDataTime));
    properties.setProperty(StarTreeMetadataProperties.START_TIME, Long.toString(startTime));
    properties.setProperty(StarTreeMetadataProperties.END_TIME, Long.toString(endTime));
    properties.setProperty(StarTreeMetadataProperties.TIME_GRANULARITY, timeGranularity);

    return properties;

  }

  public static IndexMetadata fromProperties(Properties properties)
  {
    IndexMetadata indexMetadata = new IndexMetadata();

    String minDataTime = properties.getProperty(StarTreeMetadataProperties.MIN_DATA_TIME);
    String maxDataTime = properties.getProperty(StarTreeMetadataProperties.MAX_DATA_TIME);
    String startTime = properties.getProperty(StarTreeMetadataProperties.START_TIME);
    String endTime = properties.getProperty(StarTreeMetadataProperties.END_TIME);
    String timeGranularity = properties.getProperty(StarTreeMetadataProperties.TIME_GRANULARITY);

    if (minDataTime == null || maxDataTime == null || startTime == null || endTime == null
        || timeGranularity == null)    {
      throw new IllegalStateException("Cannot find required metadata properties");
    }
    indexMetadata.setMinDataTime(Long.valueOf(minDataTime));
    indexMetadata.setMaxDataTime(Long.valueOf(maxDataTime));
    indexMetadata.setStartTime(Long.valueOf(startTime));
    indexMetadata.setEndTime(Long.valueOf(endTime));
    indexMetadata.setTimeGranularity(timeGranularity);
    return indexMetadata;

  }

}
