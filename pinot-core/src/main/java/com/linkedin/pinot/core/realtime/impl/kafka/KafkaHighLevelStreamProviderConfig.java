/**
 * Copyright (C) 2014-2015 LinkedIn Corp. (pinot-core@linkedin.com)
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

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import kafka.consumer.ConsumerConfig;

import com.linkedin.pinot.common.data.Schema;
import com.linkedin.pinot.common.metadata.instance.InstanceZKMetadata;
import com.linkedin.pinot.common.metadata.resource.RealtimeDataResourceZKMetadata;
import com.linkedin.pinot.common.metadata.stream.KafkaStreamMetadata;
import com.linkedin.pinot.common.utils.CommonConstants.Helix;
import com.linkedin.pinot.core.realtime.StreamProviderConfig;


public class KafkaHighLevelStreamProviderConfig implements StreamProviderConfig {
  private static final Map<String, String> defaultProps;

  static {
    defaultProps = new HashMap<String, String>();
    /*defaultProps.put("zookeeper.connect", zookeeper);
    defaultProps.put("group.id", groupId);*/
    defaultProps.put("zookeeper.session.timeout.ms", "30000");
    defaultProps.put("zookeeper.sync.time.ms", "200");
    defaultProps.put("auto.commit.enable", "false");
    defaultProps.put("auto.offset.reset", "largest");
  }

  private String kafkaTopicName;
  private String zkString;
  private String groupId;
  private KafkaMessageDecoder decoder;
  private String decodeKlass;
  private Schema indexingSchema;
  private Map<String, String> decoderProps;

  /*
   * kafka.hlc.zk.connect.string : comma separated list of hosts
   * kafka.hlc.broker.port : broker port
   * kafka.hlc.group.id : group id
   * kafka.decoder.class.name : the absolute path of the decoder class name
   * kafka.decoder.props1 : every property that is prefixed with kafka.decoder.
   * */

  public void init(Map<String, String> properties, Schema schema) {
    decoderProps = new HashMap<String, String>();

    this.indexingSchema = schema;
    if (properties.containsKey(Helix.DataSource.Realtime.Kafka.HighLevelConsumer.GROUP_ID)) {
      this.groupId = properties.get(Helix.DataSource.Realtime.Kafka.HighLevelConsumer.GROUP_ID);
    }

    if (properties.containsKey(Helix.DataSource.Realtime.Kafka.HighLevelConsumer.ZK_CONNECTION_STRING)) {
      this.zkString = properties.get(Helix.DataSource.Realtime.Kafka.HighLevelConsumer.ZK_CONNECTION_STRING);
    }

    if (properties.containsKey(Helix.DataSource.Realtime.Kafka.TOPIC_NAME)) {
      this.kafkaTopicName = properties.get(Helix.DataSource.Realtime.Kafka.TOPIC_NAME);
    }

    if (properties.containsKey(Helix.DataSource.Realtime.Kafka.DECODER_CLASS)) {
      this.decodeKlass = properties.get(Helix.DataSource.Realtime.Kafka.DECODER_CLASS);
    }

    if (groupId == null || zkString == null || kafkaTopicName == null || this.decodeKlass == null) {
      throw new RuntimeException("Cannot initialize KafkaHighLevelStreamProviderConfig as: "
          + "groupId = " + groupId + ", zkString = " + zkString + ", kafkaTopicName = " + kafkaTopicName
          + ", decodeKlass = " + decodeKlass);
    }

    for (String key : properties.keySet()) {
      if (key.startsWith(Helix.DataSource.Realtime.Kafka.DECODER_PROPS_PREFIX)) {
        decoderProps.put(Helix.DataSource.Realtime.Kafka.getDecoderPropertyKey(key), properties.get(key));
      }
    }
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
    topicCountMap.put(kafkaTopicName, new Integer(numThreads));
    return topicCountMap;
  }

  public ConsumerConfig getKafkaConsumerConfig() {
    Properties props = new Properties();
    for (String key : defaultProps.keySet()) {
      props.put(key, defaultProps.get(key));
    }
    props.put("group.id", groupId);
    props.put("zookeeper.connect", zkString);
    return new ConsumerConfig(props);
  }

  public KafkaMessageDecoder getDecoder() throws Exception {
    KafkaMessageDecoder ret = (KafkaMessageDecoder) Class.forName(decodeKlass).newInstance();
    ret.init(decoderProps, indexingSchema, kafkaTopicName);
    return ret;
  }

  @Override
  public String getStreamProviderClass() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public void init(RealtimeDataResourceZKMetadata resourceMetdata, InstanceZKMetadata instanceMetadata) {
    this.indexingSchema = resourceMetdata.getDataSchema();
    this.groupId = instanceMetadata.getGroupId(resourceMetdata.getResourceName());
    KafkaStreamMetadata kafkaMetadata = (KafkaStreamMetadata) resourceMetdata.getStreamMetadata();
    this.kafkaTopicName = kafkaMetadata.getKafkaTopicName();
    this.decodeKlass = kafkaMetadata.getDecoderClass();
    this.decoderProps = kafkaMetadata.getDecoderProperties();
  }
}
