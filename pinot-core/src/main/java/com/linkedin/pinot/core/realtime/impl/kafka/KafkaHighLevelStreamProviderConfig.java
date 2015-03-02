package com.linkedin.pinot.core.realtime.impl.kafka;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import kafka.consumer.ConsumerConfig;

import com.linkedin.pinot.common.data.Schema;
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

  @Override
  public void init(Map<String, String> properties, Schema schema) throws Exception {
    decoderProps = new HashMap<String, String>();

    this.indexingSchema = schema;
    if (properties.containsKey(KafkaProperties.HighLevelConsumer.GROUP_ID)) {
      this.groupId = properties.get(KafkaProperties.HighLevelConsumer.GROUP_ID);
    }

    if (properties.containsKey(KafkaProperties.HighLevelConsumer.ZK_CONNECTION_STRING)) {
      this.zkString = properties.get(KafkaProperties.HighLevelConsumer.ZK_CONNECTION_STRING);
    }

    if (properties.containsKey(KafkaProperties.TOPIC_NAME)) {
      this.kafkaTopicName = properties.get(KafkaProperties.TOPIC_NAME);
    }

    if (properties.containsKey(KafkaProperties.DECODER_CLASS)) {
      this.decodeKlass = properties.get(KafkaProperties.DECODER_CLASS);
    }

    if (groupId == null || zkString == null || kafkaTopicName == null || this.decodeKlass == null) {
      throw new Exception();
    }

    for (String key : properties.keySet()) {
      if (key.startsWith(KafkaProperties.DECODER_PROPS_PREFIX)) {
        decoderProps.put(KafkaProperties.getDecoderPropertyKey(key), properties.get(key));
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
}
