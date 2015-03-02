package com.linkedin.pinot.core.realtime.impl.kafka;

import java.util.Map;

import com.linkedin.pinot.common.data.Schema;
import com.linkedin.pinot.core.data.GenericRow;


public interface KafkaMessageDecoder {

  /**
   *
   * @param props
   * @throws Exception
   */
  public void init(Map<String, String> props, Schema indexingSchema, String kafkaTopicName) throws Exception;

  /**
   *
   * @param payload
   * @return
   */
  public GenericRow decode(byte[] payload);

}
