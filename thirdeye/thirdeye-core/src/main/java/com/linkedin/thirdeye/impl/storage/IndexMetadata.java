package com.linkedin.thirdeye.impl.storage;

import java.util.Properties;

public class IndexMetadata {

  private Long minDataTime;
  private Long maxDataTime;


  public IndexMetadata(Long minDataTime, Long maxDataTime) {
    this.minDataTime = minDataTime;
    this.maxDataTime = maxDataTime;
  }

  public IndexMetadata() {

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

    return properties;

  }

  public static IndexMetadata fromProperties(Properties properties)
  {
    IndexMetadata indexMetadata = new IndexMetadata();

    String minDataTime = properties.getProperty(StarTreeMetadataProperties.MIN_DATA_TIME);
    String maxDataTime = properties.getProperty(StarTreeMetadataProperties.MAX_DATA_TIME);

    if (minDataTime == null || maxDataTime == null)
    {
      throw new IllegalStateException("Cannot find required metadata properties");
    }
    indexMetadata.setMinDataTime(Long.valueOf(minDataTime));
    indexMetadata.setMaxDataTime(Long.valueOf(maxDataTime));
    return indexMetadata;

  }




}
