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

package com.linkedin.pinot.core.realtime.stream;

import com.linkedin.pinot.common.Utils;
import com.linkedin.pinot.core.realtime.impl.kafka.KafkaLowLevelStreamProviderConfig;
import javax.annotation.Nonnull;


public abstract class PinotStreamConsumerFactory {
  public static PinotStreamConsumerFactory create(StreamMetadata streamMetadata) {
    PinotStreamConsumerFactory factory = null;
    try {
      factory = (PinotStreamConsumerFactory) Class.forName(streamMetadata.getConsumerFactoryName()).newInstance();
    } catch (Exception e) {
      Utils.rethrowException(e);
    }
    return factory;
  }

  public abstract PinotStreamConsumer buildConsumer(@Nonnull String clientId, int partition, StreamMetadata streamMetadata);

  public abstract PinotStreamConsumer buildMetadataFetcher(@Nonnull String clientId, StreamMetadata streamMetadata);

  // TODO First split KafkaLowLevelStreamProviderConfig to be kafka agnostic and kafka-specific and then rename.
  public StreamMessageDecoder getDecoder(KafkaLowLevelStreamProviderConfig kafkaStreamProviderConfig) throws Exception {
    return kafkaStreamProviderConfig.getDecoder();
  }
}
