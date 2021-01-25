/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.plugin.stream.kinesis;

import java.util.HashMap;
import java.util.Map;
import org.apache.pinot.spi.stream.StreamConfig;
import org.apache.pinot.spi.stream.StreamConfigProperties;
import software.amazon.awssdk.services.kinesis.model.ShardIteratorType;


public class TestUtils {
  private static final String STREAM_NAME = "kinesis-test";
  private static final String AWS_REGION = "us-west-2";

  public static StreamConfig getStreamConfig() {
    Map<String, String> props = new HashMap<>();
    props.put(KinesisConfig.STREAM, STREAM_NAME);
    props.put(KinesisConfig.AWS_REGION, AWS_REGION);
    props.put(KinesisConfig.MAX_RECORDS_TO_FETCH, "10");
    props.put(KinesisConfig.SHARD_ITERATOR_TYPE, ShardIteratorType.AT_SEQUENCE_NUMBER.toString());
    props.put(StreamConfigProperties.STREAM_TYPE, "kinesis");
    props.put("stream.kinesis.consumer.type", "lowLevel");
    props.put("stream.kinesis.topic.name", STREAM_NAME);
    props.put("stream.kinesis.decoder.class.name", "ABCD");
    props.put("stream.kinesis.consumer.factory.class.name",
        "org.apache.pinot.plugin.stream.kinesis.KinesisConsumerFactory");
    return new StreamConfig("", props);
  }

  public static KinesisConfig getKinesisConfig() {
    Map<String, String> props = new HashMap<>();
    props.put(KinesisConfig.STREAM, STREAM_NAME);
    props.put(KinesisConfig.AWS_REGION, AWS_REGION);
    props.put(KinesisConfig.MAX_RECORDS_TO_FETCH, "10");
    props.put(KinesisConfig.SHARD_ITERATOR_TYPE, ShardIteratorType.AT_SEQUENCE_NUMBER.toString());
    return new KinesisConfig(props);
  }
}
