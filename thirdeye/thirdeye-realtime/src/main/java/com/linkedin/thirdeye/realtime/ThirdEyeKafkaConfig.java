package com.linkedin.thirdeye.realtime;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.linkedin.thirdeye.api.TimeGranularity;
import org.joda.time.DateTime;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class ThirdEyeKafkaConfig {
  private static final String DEFAULT_DECODER_CLASS =
      ThirdEyeKafkaDecoderAvroImpl.class.getCanonicalName();
  private static final TimeGranularity DEFAULT_PERSIST_INTERVAL =
      new TimeGranularity(15, TimeUnit.MINUTES);
  private static final String DEFAULT_GROUP_ID = "THIRDEYE";
  private static final DateTime DEFAULT_START_TIME = new DateTime(0);

  private String zkAddress;
  private String topicName;
  private String groupId = DEFAULT_GROUP_ID;

  private String decoderClass = DEFAULT_DECODER_CLASS;
  private TimeGranularity persistInterval = DEFAULT_PERSIST_INTERVAL;
  private DateTime startTime = DEFAULT_START_TIME;

  private Properties decoderConfig = new Properties();
  private Properties consumerConfig = new Properties();

  public ThirdEyeKafkaConfig() {
  }

  @JsonProperty
  public String getDecoderClass() {
    return decoderClass;
  }

  @JsonProperty
  public void setDecoderClass(String decoderClass) {
    this.decoderClass = decoderClass;
  }

  @JsonProperty
  public Properties getDecoderConfig() {
    return decoderConfig;
  }

  @JsonProperty
  public void setDecoderConfig(Properties decoderConfig) {
    this.decoderConfig = decoderConfig;
  }

  @JsonProperty
  public String getZkAddress() {
    return zkAddress;
  }

  @JsonProperty
  public void setZkAddress(String zkAddress) {
    this.zkAddress = zkAddress;
  }

  @JsonProperty
  public Properties getConsumerConfig() {
    return consumerConfig;
  }

  @JsonProperty
  public void setConsumerConfig(Properties consumerConfig) {
    this.consumerConfig = consumerConfig;
  }

  @JsonProperty
  public String getTopicName() {
    return topicName;
  }

  @JsonProperty
  public void setTopicName(String topicName) {
    this.topicName = topicName;
  }

  @JsonProperty
  public String getGroupId() {
    return groupId;
  }

  @JsonProperty
  public void setGroupId(String groupId) {
    this.groupId = groupId;
  }

  @JsonProperty
  public TimeGranularity getPersistInterval() {
    return persistInterval;
  }

  @JsonProperty
  public void setPersistInterval(TimeGranularity persistInterval) {
    this.persistInterval = persistInterval;
  }

  @JsonProperty
  public DateTime getStartTime() {
    return startTime;
  }

  @JsonProperty
  public void setStartTime(DateTime startTime) {
    this.startTime = startTime;
  }
}
