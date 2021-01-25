package org.apache.pinot.plugin.stream.kinesis;

import java.util.HashMap;
import java.util.Map;
import org.apache.pinot.spi.stream.StreamConfig;
import org.apache.pinot.spi.stream.StreamConfigProperties;
import software.amazon.awssdk.services.kinesis.model.ShardIteratorType;


public class TestUtils {
  private static final String STREAM_NAME = "kinesis-test";
  private static final String AWS_REGION = "us-west-2";

  public static StreamConfig getStreamConfig(){
    Map<String, String> props = new HashMap<>();
    props.put(KinesisConfig.STREAM, STREAM_NAME);
    props.put(KinesisConfig.AWS_REGION, AWS_REGION);
    props.put(KinesisConfig.MAX_RECORDS_TO_FETCH, "10");
    props.put(KinesisConfig.SHARD_ITERATOR_TYPE, ShardIteratorType.AT_SEQUENCE_NUMBER.toString());
    props.put(StreamConfigProperties.STREAM_TYPE, "kinesis");
    props.put("stream.kinesis.consumer.type", "lowLevel");
    props.put("stream.kinesis.topic.name", STREAM_NAME);
    props.put("stream.kinesis.decoder.class.name", "ABCD");
    props.put("stream.kinesis.consumer.factory.class.name", "org.apache.pinot.plugin.stream.kinesis.KinesisConsumerFactory");
    return new StreamConfig("", props);

  }
}
