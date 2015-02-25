package com.linkedin.pinot.core.realtime.kafka;

import java.util.List;

import com.linkedin.pinot.common.data.Schema;
import com.linkedin.pinot.core.realtime.StreamProviderConfig;

public class KafkaStreamProviderConfig implements StreamProviderConfig{
  
  String kafkaZookeeperAddress;
  
  String topicName;
  
  String numPartitions;
  
  List<String> partitionNames;

  private Schema schema;

  public KafkaStreamProviderConfig(Schema schema){
    this.schema = schema;
  }
  @Override
  public String getStreamProviderClass() {
    return KafkaStreamProvider.class.getName();
  }

  @Override
  public Schema getSchema() {
    return schema;
  }
}
