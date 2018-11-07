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
package com.linkedin.pinot.core.realtime.stream;

import java.io.Closeable;


/**
 * Interface for a consumer which fetches messages at the partition level of a stream, for given offsets
 */
public interface PartitionLevelConsumer extends Closeable {

  /**
   * Fetch messages from the stream between the specified offsets
   * @param startOffset
   * @param endOffset
   * @param timeoutMillis
   * @return
   * @throws java.util.concurrent.TimeoutException
   */
  MessageBatch fetchMessages(long startOffset, long endOffset, int timeoutMillis)
      throws java.util.concurrent.TimeoutException;
}
