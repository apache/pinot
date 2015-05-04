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
    indexMetadata.minDataTime = properties.getProperty(StarTreeMetadataProperties.MIN_DATA_TIME) == null ?
        null
        : Long.parseLong(properties.getProperty(StarTreeMetadataProperties.MIN_DATA_TIME));
    indexMetadata.maxDataTime = properties.getProperty(StarTreeMetadataProperties.MAX_DATA_TIME) == null ?
        null
        :Long.parseLong(properties.getProperty(StarTreeMetadataProperties.MAX_DATA_TIME));

    return indexMetadata;

  }




}
