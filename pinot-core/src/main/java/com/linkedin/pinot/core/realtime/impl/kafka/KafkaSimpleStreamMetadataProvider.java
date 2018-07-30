/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
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

import com.linkedin.pinot.common.utils.NetUtil;
import com.linkedin.pinot.core.realtime.stream.StreamMetadata;
import com.linkedin.pinot.core.realtime.stream.StreamMetadataProvider;
import java.io.IOException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Implementation of a stream metadata provider for a kafka simple stream
 */
public class KafkaSimpleStreamMetadataProvider extends KafkaConnectionHandler implements StreamMetadataProvider {
  private static final Logger LOGGER = LoggerFactory.getLogger(KafkaSimpleStreamMetadataProvider.class);

  public KafkaSimpleStreamMetadataProvider(StreamMetadata streamMetadata, int partition) {
    super(new KafkaSimpleConsumerFactoryImpl(), streamMetadata.getBootstrapHosts(),
        partition + "-" + NetUtil.getHostnameOrAddress(), streamMetadata.getKafkaTopicName(), partition,
        streamMetadata.getKafkaConnectionTimeoutMillis());
  }

  public KafkaSimpleStreamMetadataProvider(StreamMetadata streamMetadata) {
    super(new KafkaSimpleConsumerFactoryImpl(), streamMetadata.getBootstrapHosts(),
        KafkaSimpleStreamMetadataProvider.class.getName() + "-" + streamMetadata.getKafkaTopicName(),
        streamMetadata.getKafkaTopicName(), streamMetadata.getKafkaConnectionTimeoutMillis());
  }

  // TODO: this will replace metadata related methods called from SimpleConsumerWrapper
  // TODO: make sure this code is equivalent to the methods in SimpleConsumerWrapper before starting to use this
  @Override
  public synchronized int fetchPartitionCount(String topic, long timeoutMillis) {
    throw new UnsupportedOperationException();
  }

  /**
   * Fetches the numeric Kafka offset for this partition for a symbolic name ("largest" or "smallest").
   *
   * @param requestedOffset Either "largest" or "smallest"
   * @param timeoutMillis Timeout in milliseconds
   * @throws java.util.concurrent.TimeoutException If the operation could not be completed within {@code timeoutMillis}
   * milliseconds
   * @return An offset
   */
  @Override
  public synchronized long fetchPartitionOffset(String requestedOffset, int timeoutMillis)
      throws java.util.concurrent.TimeoutException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void close() throws IOException {
    super.close();
  }
}
