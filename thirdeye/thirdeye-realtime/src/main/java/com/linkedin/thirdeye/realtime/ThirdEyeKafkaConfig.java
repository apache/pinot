package com.linkedin.thirdeye.realtime;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class ThirdEyeKafkaConfig
{
  private static final String DEFAULT_DECODER_CLASS = ThirdEyeKafkaDecoderAvroImpl.class.getCanonicalName();
  private static final long DEFAULT_PERSIST_INTERVAL_MILLIS = TimeUnit.MILLISECONDS.convert(5, TimeUnit.MINUTES);
  private static final String DEFAULT_GROUP_ID = "THIRDEYE";
  private static final long DEFAULT_START_TIME_MILLIS = 0; // i.e. all time

  private String zkAddress;
  private String topicName;
  private String groupId = DEFAULT_GROUP_ID;

  private String decoderClass = DEFAULT_DECODER_CLASS;
  private long persistIntervalMillis = DEFAULT_PERSIST_INTERVAL_MILLIS;
  private long startTimeMillis = DEFAULT_START_TIME_MILLIS;

  private Properties decoderConfig = new Properties();
  private Properties consumerConfig = new Properties();

  public ThirdEyeKafkaConfig() {}

  @JsonProperty
  public String getDecoderClass()
  {
    return decoderClass;
  }

  @JsonProperty
  public void setDecoderClass(String decoderClass)
  {
    this.decoderClass = decoderClass;
  }

  @JsonProperty
  public Properties getDecoderConfig()
  {
    return decoderConfig;
  }

  @JsonProperty
  public void setDecoderConfig(Properties decoderConfig)
  {
    this.decoderConfig = decoderConfig;
  }

  @JsonProperty
  public String getZkAddress()
  {
    return zkAddress;
  }

  @JsonProperty
  public void setZkAddress(String zkAddress)
  {
    this.zkAddress = zkAddress;
  }

  @JsonProperty
  public Properties getConsumerConfig()
  {
    return consumerConfig;
  }

  @JsonProperty
  public void setConsumerConfig(Properties consumerConfig)
  {
    this.consumerConfig = consumerConfig;
  }

  @JsonProperty
  public String getTopicName()
  {
    return topicName;
  }

  @JsonProperty
  public void setTopicName(String topicName)
  {
    this.topicName = topicName;
  }

  @JsonProperty
  public String getGroupId()
  {
    return groupId;
  }

  @JsonProperty
  public void setGroupId(String groupId)
  {
    this.groupId = groupId;
  }

  @JsonProperty
  public long getPersistIntervalMillis()
  {
    return persistIntervalMillis;
  }

  @JsonProperty
  public void setPersistIntervalMillis(long persistIntervalMillis)
  {
    this.persistIntervalMillis = persistIntervalMillis;
  }

  @JsonProperty
  public long getStartTimeMillis()
  {
    return startTimeMillis;
  }

  @JsonProperty
  public void setStartTimeMillis(long startTimeMillis)
  {
    this.startTimeMillis = startTimeMillis;
  }
}
