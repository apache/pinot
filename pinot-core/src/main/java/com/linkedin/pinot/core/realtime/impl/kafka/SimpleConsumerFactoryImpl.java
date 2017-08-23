package com.linkedin.pinot.core.realtime.impl.kafka;

public class SimpleConsumerFactoryImpl implements KafkaConsumerFactory {
  public IPinotKafkaConsumer buildConsumerWrapper(String bootstrapNodes, String clientId, String topic,
      int partition, long connectTimeoutMillis) {
    return new SimpleConsumerWrapper(bootstrapNodes, clientId, topic, partition, connectTimeoutMillis);
  }
}
