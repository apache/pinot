package com.linkedin.pinot.core.realtime.impl.kafka;

import com.google.common.collect.ImmutableMap;
import com.linkedin.pinot.core.realtime.stream.StreamConfig;
import com.linkedin.pinot.core.realtime.stream.StreamConfigProperties;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.Map;

import static org.testng.Assert.*;
import static com.linkedin.pinot.core.realtime.impl.kafka.KafkaStreamConfigProperties.LowLevelConsumer.*;

public class KafkaLowLevelStreamConfigTest {

  private KafkaLowLevelStreamConfig getStreamConfig(String topic, String bootstrapHosts, String buffer, String socketTimeout) {
    Map<String, String> streamConfigMap = new HashMap<>();
    String streamType = "kafka";
    String consumerType = StreamConfig.ConsumerType.LOWLEVEL.toString();
    String consumerFactoryClassName = KafkaConsumerFactory.class.getName();
    String decoderClass = KafkaAvroMessageDecoder.class.getName();
    streamConfigMap.put(StreamConfigProperties.STREAM_TYPE, streamType);
    streamConfigMap.put(
        StreamConfigProperties.constructStreamProperty(streamType, StreamConfigProperties.STREAM_TOPIC_NAME), topic);
    streamConfigMap.put(
        StreamConfigProperties.constructStreamProperty(streamType, StreamConfigProperties.STREAM_CONSUMER_TYPES),
        consumerType);
    streamConfigMap.put(StreamConfigProperties.constructStreamProperty(streamType,
        StreamConfigProperties.STREAM_CONSUMER_FACTORY_CLASS), consumerFactoryClassName);
    streamConfigMap.put(
        StreamConfigProperties.constructStreamProperty(streamType, StreamConfigProperties.STREAM_DECODER_CLASS),
        decoderClass);
    streamConfigMap.put("stream.kafka.broker.list", bootstrapHosts);
    if (buffer != null) {
      streamConfigMap.put("stream.kafka.buffer.size", buffer);
    }
    if (socketTimeout != null) {
      streamConfigMap.put("stream.kafka.socket.timeout", String.valueOf(socketTimeout));
    }
    return new KafkaLowLevelStreamConfig(new StreamConfig(streamConfigMap));
  }

  @Test
  public void testGetKafkaTopicName() {
    KafkaLowLevelStreamConfig config = getStreamConfig("topic", "", "", "");
    Assert.assertEquals("topic", config.getKafkaTopicName());
  }

  @Test
  public void testGetBootstrapHosts() {
    KafkaLowLevelStreamConfig config = getStreamConfig("topic", "host1", "", "");
    Assert.assertEquals("host1", config.getBootstrapHosts());
  }

  @Test
  public void testGetKafkaBufferSize() {
    // test default
    KafkaLowLevelStreamConfig config = getStreamConfig("topic", "host1", null, "");
    Assert.assertEquals(KafkaStreamConfigProperties.LowLevelConsumer.KAFKA_BUFFER_SIZE_DEFAULT, config.getKafkaBufferSize());

    config = getStreamConfig("topic", "host1", "", "");
    Assert.assertEquals(KafkaStreamConfigProperties.LowLevelConsumer.KAFKA_BUFFER_SIZE_DEFAULT, config.getKafkaBufferSize());

    config = getStreamConfig("topic", "host1", "bad value", "");
    Assert.assertEquals(KafkaStreamConfigProperties.LowLevelConsumer.KAFKA_BUFFER_SIZE_DEFAULT, config.getKafkaBufferSize());

    // correct config
    config = getStreamConfig("topic", "host1", "100", "");
    Assert.assertEquals(100, config.getKafkaBufferSize());
  }

  @Test
  public void testGetKafkaSocketTimeout() {
    // test default
    KafkaLowLevelStreamConfig config = getStreamConfig("topic", "host1", "",null);
    Assert.assertEquals(KafkaStreamConfigProperties.LowLevelConsumer.KAFKA_SOCKET_TIMEOUT_DEFAULT, config.getKafkaSocketTimeout());

    config = getStreamConfig("topic", "host1", "","");
    Assert.assertEquals(KafkaStreamConfigProperties.LowLevelConsumer.KAFKA_SOCKET_TIMEOUT_DEFAULT, config.getKafkaSocketTimeout());

    config = getStreamConfig("topic", "host1", "","bad value");
    Assert.assertEquals(KafkaStreamConfigProperties.LowLevelConsumer.KAFKA_SOCKET_TIMEOUT_DEFAULT, config.getKafkaSocketTimeout());

    // correct config
    config = getStreamConfig("topic", "host1", "", "100");
    Assert.assertEquals(100, config.getKafkaSocketTimeout());
  }
}