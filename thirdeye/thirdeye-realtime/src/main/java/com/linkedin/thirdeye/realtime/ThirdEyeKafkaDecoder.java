package com.linkedin.thirdeye.realtime;

import com.linkedin.thirdeye.api.StarTreeConfig;
import com.linkedin.thirdeye.api.StarTreeRecord;

import java.io.IOException;

public interface ThirdEyeKafkaDecoder {
  void init(StarTreeConfig starTreeConfig, ThirdEyeKafkaConfig kafkaConfig) throws Exception;

  StarTreeRecord decode(byte[] bytes) throws IOException;
}
