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

import java.io.Closeable;
import java.io.IOException;
import javax.annotation.Nonnull;


/**
 * Interface that allows us to plugin different Kafka consumers. The default implementation is SimpleConsumerWrapper.
 */
public interface PinotKafkaConsumer extends Closeable {
  int getPartitionCount(String topic, long timeoutMillis);

  MessageBatch fetchMessages(long startOffset, long endOffset, int timeoutMillis)
        throws java.util.concurrent.TimeoutException;

  /**
   *
   * @param requestedOffset nonnull
   * @param timeoutMillis
   * @return
   * @throws java.util.concurrent.TimeoutException
   */
  long fetchPartitionOffset(@Nonnull String requestedOffset, int timeoutMillis)
        throws java.util.concurrent.TimeoutException;

  void close() throws IOException;
}
