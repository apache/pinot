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

import com.linkedin.pinot.common.metadata.stream.KafkaStreamMetadata;
import javax.annotation.Nonnull;


public class SimpleConsumerFactory extends PinotKafkaConsumerFactory {
  public PinotKafkaConsumer buildConsumer(String clientId, int partition, KafkaStreamMetadata kafkaStreamMetadata) {
    KafkaSimpleConsumerFactoryImpl kafkaSimpleConsumerFactory = new KafkaSimpleConsumerFactoryImpl();
    return new SimpleConsumerWrapper(kafkaSimpleConsumerFactory, kafkaStreamMetadata.getBootstrapHosts(),
        clientId, kafkaStreamMetadata.getKafkaTopicName(), partition, kafkaStreamMetadata.getKafkaConnectionTimeoutMillis());
  }

  public PinotKafkaConsumer buildMetadataFetcher(@Nonnull String clientId, KafkaStreamMetadata kafkaStreamMetadata) {
    KafkaSimpleConsumerFactoryImpl kafkaSimpleConsumerFactory = new KafkaSimpleConsumerFactoryImpl();
    return new SimpleConsumerWrapper(kafkaSimpleConsumerFactory, kafkaStreamMetadata.getBootstrapHosts(),
        clientId, kafkaStreamMetadata.getKafkaConnectionTimeoutMillis());
  }
}
