/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
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

import com.linkedin.pinot.core.realtime.stream.PinotStreamConsumerFactory;
import com.linkedin.pinot.core.realtime.stream.StreamMessageDecoder;
import com.linkedin.pinot.core.realtime.stream.StreamMetadata;
import com.linkedin.pinot.core.realtime.stream.StreamProviderConfig;
import javax.annotation.Nonnull;


public class SimpleConsumerFactory extends PinotStreamConsumerFactory {
  public PinotKafkaConsumer buildConsumer(String clientId, int partition, StreamMetadata streamMetadata) {
    KafkaSimpleConsumerFactoryImpl kafkaSimpleConsumerFactory = new KafkaSimpleConsumerFactoryImpl();
    KafkaStreamMetadata kafkaStreamMetadata = (KafkaStreamMetadata) streamMetadata;
    return new SimpleConsumerWrapper(kafkaSimpleConsumerFactory, kafkaStreamMetadata.getBootstrapHosts(),
        clientId, kafkaStreamMetadata.getStreamName(), partition, kafkaStreamMetadata.getConnectionTimeoutMillis());
  }

  public PinotKafkaConsumer buildMetadataFetcher(@Nonnull String clientId, StreamMetadata streamMetadata) {
    KafkaSimpleConsumerFactoryImpl kafkaSimpleConsumerFactory = new KafkaSimpleConsumerFactoryImpl();
    KafkaStreamMetadata kafkaStreamMetadata = (KafkaStreamMetadata) streamMetadata;
    return new SimpleConsumerWrapper(kafkaSimpleConsumerFactory, kafkaStreamMetadata.getBootstrapHosts(),
        clientId, kafkaStreamMetadata.getConnectionTimeoutMillis());
  }

  @Override
  public StreamMessageDecoder getDecoder(StreamProviderConfig streamProviderConfig) throws Exception {
    KafkaLowLevelStreamProviderConfig kafkaLowLevelStreamProviderConfig =
        (KafkaLowLevelStreamProviderConfig) streamProviderConfig;
    return kafkaLowLevelStreamProviderConfig.getDecoder();
  }
}
