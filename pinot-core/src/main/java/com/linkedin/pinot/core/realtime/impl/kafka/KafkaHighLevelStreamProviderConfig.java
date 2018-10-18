/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.core.realtime.impl.kafka;

import com.linkedin.pinot.common.config.TableConfig;
import com.linkedin.pinot.common.data.Schema;
import com.linkedin.pinot.common.metadata.instance.InstanceZKMetadata;
import com.linkedin.pinot.core.realtime.StreamProviderConfig;
import com.linkedin.pinot.core.realtime.stream.StreamConfig;
import com.linkedin.pinot.core.realtime.stream.StreamConfigProperties;
import com.linkedin.pinot.core.realtime.stream.StreamMessageDecoder;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import kafka.consumer.ConsumerConfig;
import org.joda.time.Duration;
import org.joda.time.Period;
import org.joda.time.format.PeriodFormatter;
import org.joda.time.format.PeriodFormatterBuilder;

import static com.linkedin.pinot.common.utils.EqualityUtils.*;


public class KafkaHighLevelStreamProviderConfig implements StreamProviderConfig {
  private static final Map<String, String> defaultProps;

  private static final int DEFAULT_MAX_REALTIME_ROWS_COUNT = 5000000;
  private final static long ONE_MINUTE_IN_MILLSEC = 1000 * 60;
  public static final long ONE_HOUR = ONE_MINUTE_IN_MILLSEC * 60;

  private final static PeriodFormatter PERIOD_FORMATTER;

  static {
    defaultProps = new HashMap<String, String>();
    /*defaultProps.put("zookeeper.connect", zookeeper);
    defaultProps.put("group.id", groupId);*/
    defaultProps.put(KafkaStreamConfigProperties.HighLevelConsumer.ZK_SESSION_TIMEOUT_MS, "30000");
    defaultProps.put(KafkaStreamConfigProperties.HighLevelConsumer.ZK_CONNECTION_TIMEOUT_MS, "10000");
    defaultProps.put(KafkaStreamConfigProperties.HighLevelConsumer.ZK_SYNC_TIME_MS, "2000");

    // Rebalance retries will take up to 1 mins to fail.
    defaultProps.put(KafkaStreamConfigProperties.HighLevelConsumer.REBALANCE_MAX_RETRIES, "30");
    defaultProps.put(KafkaStreamConfigProperties.HighLevelConsumer.REBALANCE_BACKOFF_MS, "2000");

    defaultProps.put(KafkaStreamConfigProperties.HighLevelConsumer.AUTO_COMMIT_ENABLE, "false");
    defaultProps.put(KafkaStreamConfigProperties.HighLevelConsumer.AUTO_OFFSET_RESET, "largest");

    // A formatter for time specification that allows time to be specified in days, hours and minutes
    // e.g. 1d2h3m, or 6h5m or simply 5h
    PERIOD_FORMATTER =  new PeriodFormatterBuilder()
        .appendDays().appendSuffix("d")
        .appendHours().appendSuffix("h")
        .appendMinutes().appendSuffix("m")
        .toFormatter();
  }

  public static int getDefaultMaxRealtimeRowsCount() {
    return DEFAULT_MAX_REALTIME_ROWS_COUNT;
  }

  private String kafkaTopicName;
  private String zkString;
  private String groupId;
  private StreamMessageDecoder decoder;
  private String decodeKlass;
  private Schema indexingSchema;
  private Map<String, String> decoderProps;
  private Map<String, String> kafkaConsumerProps;
  private long segmentTimeInMillis = ONE_HOUR;
  private int realtimeRecordsThreshold = DEFAULT_MAX_REALTIME_ROWS_COUNT;

  public KafkaHighLevelStreamProviderConfig() {

  }

  @Override
  public Schema getSchema() {
    return indexingSchema;
  }

  public String getTopicName() {
    return this.kafkaTopicName;
  }

  public Map<String, Integer> getTopicMap(int numThreads) {
    Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
    topicCountMap.put(kafkaTopicName, numThreads);
    return topicCountMap;
  }

  public ConsumerConfig getKafkaConsumerConfig() {
    Properties props = new Properties();
    for (String key : defaultProps.keySet()) {
      props.put(key, defaultProps.get(key));
    }

    for (String key : kafkaConsumerProps.keySet()) {
      props.put(key, kafkaConsumerProps.get(key));
    }

    props.put("group.id", groupId);
    props.put("zookeeper.connect", zkString);
    return new ConsumerConfig(props);
  }

  public StreamMessageDecoder getDecoder() throws Exception {
    return getDecoder(decodeKlass);
  }

  public StreamMessageDecoder getDecoder(String decodeClass) throws Exception {
    StreamMessageDecoder ret = (StreamMessageDecoder) Class.forName(decodeClass).newInstance();
    ret.init(decoderProps, indexingSchema, kafkaTopicName);
    return ret;
  }

  @Override
  public void init(TableConfig tableConfig, InstanceZKMetadata instanceMetadata, Schema schema) {
    this.indexingSchema = schema;
    if (instanceMetadata != null) {
      // For LL segments, instanceZkMetadata will be null
      this.groupId = instanceMetadata.getGroupId(tableConfig.getTableName());
    }
    StreamConfig kafkaMetadata = new StreamConfig(tableConfig.getIndexingConfig().getStreamConfigs());
    this.kafkaTopicName = kafkaMetadata.getTopicName();
    this.decodeKlass = kafkaMetadata.getDecoderClass();
    this.decoderProps = kafkaMetadata.getDecoderProperties();
    this.kafkaConsumerProps = kafkaMetadata.getKafkaConsumerProperties();
    this.zkString = kafkaMetadata.getZkBrokerUrl();

    String flushThresholdRowsProperty = StreamConfigProperties.SEGMENT_FLUSH_THRESHOLD_ROWS;
    if (tableConfig.getIndexingConfig().getStreamConfigs().containsKey(flushThresholdRowsProperty)) {
      realtimeRecordsThreshold =
          Integer.parseInt(tableConfig.getIndexingConfig().getStreamConfigs().get(flushThresholdRowsProperty));
    }

    String flushThresholdTimeProperty = StreamConfigProperties.SEGMENT_FLUSH_THRESHOLD_TIME;
    if (tableConfig.getIndexingConfig().getStreamConfigs().containsKey(flushThresholdTimeProperty)) {
      segmentTimeInMillis =
          convertToMs(tableConfig.getIndexingConfig().getStreamConfigs().get(flushThresholdTimeProperty));
    }
  }

  @Override
  public String getStreamName() {
    return getTopicName();
  }

  @Override
  public int getSizeThresholdToFlushSegment() {
    return realtimeRecordsThreshold;
  }

  @Override
  public long getTimeThresholdToFlushSegment() {
    return segmentTimeInMillis;
  }

  String getGroupId() {
    return groupId;
  }

  String getZkString() {
    return zkString;
  }

  protected long convertToMs(String timeStr) {
    long ms = -1;
    try {
      ms = Long.valueOf(timeStr);
    } catch (NumberFormatException e1) {
      try {
        Period p = PERIOD_FORMATTER.parsePeriod(timeStr);
        Duration d = p.toStandardDuration();
        ms = d.getStandardSeconds() * 1000L;
      } catch (Exception e2) {
        throw new RuntimeException("Invalid time spec '" + timeStr + "' (Valid examples: '3h', '4h30m')", e2);
      }
    }
    return ms;
  }

  @Override
  public boolean equals(Object o) {
    if (isSameReference(this, o)) {
      return true;
    }

    if (isNullOrNotSameClass(this, o)) {
      return false;
    }

    KafkaHighLevelStreamProviderConfig that = (KafkaHighLevelStreamProviderConfig) o;

    return isEqual(segmentTimeInMillis, that.segmentTimeInMillis) &&
        isEqual(realtimeRecordsThreshold, that.realtimeRecordsThreshold) &&
        isEqual(kafkaTopicName, that.kafkaTopicName) &&
        isEqual(zkString, that.zkString) &&
        isEqual(groupId, that.groupId) &&
        isEqual(decoder, that.decoder) &&
        isEqual(decodeKlass, that.decodeKlass) &&
        isEqual(indexingSchema, that.indexingSchema) &&
        isEqual(decoderProps, that.decoderProps) &&
        isEqual(kafkaConsumerProps, that.kafkaConsumerProps);
  }

  @Override
  public int hashCode() {
    int result = hashCodeOf(kafkaTopicName);
    result = hashCodeOf(result, zkString);
    result = hashCodeOf(result, groupId);
    result = hashCodeOf(result, decoder);
    result = hashCodeOf(result, decodeKlass);
    result = hashCodeOf(result, indexingSchema);
    result = hashCodeOf(result, decoderProps);
    result = hashCodeOf(result, kafkaConsumerProps);
    result = hashCodeOf(result, segmentTimeInMillis);
    result = hashCodeOf(result, realtimeRecordsThreshold);
    return result;
  }
}
