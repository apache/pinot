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
package org.apache.pinot.spi.stream;

import java.io.Closeable;
import org.apache.pinot.spi.annotations.InterfaceAudience;
import org.apache.pinot.spi.annotations.InterfaceStability;


/**
 * Interface for a consumer which fetches messages at the partition level of a stream, for given offsets
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public interface PartitionLevelConsumer extends Closeable, PartitionGroupConsumer {

  /**
   * Is here for backward compatibility for a short time.
   * TODO Issue 5359 remove this API once external kafka consumers implements return of StreamPartitionMsgOffset
   * Fetch messages from the stream between the specified offsets
   * @param startOffset
   * @param endOffset
   * @param timeoutMillis
   * @return
   * @throws java.util.concurrent.TimeoutException
   */
  @Deprecated
  MessageBatch fetchMessages(long startOffset, long endOffset, int timeoutMillis)
      throws java.util.concurrent.TimeoutException;

  /**
   * Fetch messages and the per-partition high watermark from Kafka between the specified offsets.
   *
   * @param startOffset The offset of the first message desired, inclusive
   * @param endOffset The offset of the last message desired, exclusive, or null
   * @param timeoutMillis Timeout in milliseconds
   * @throws java.util.concurrent.TimeoutException If the operation could not be completed within {@code timeoutMillis}
   * milliseconds
   * @return An iterable containing messages fetched from the stream partition and their offsets, as well as the
   * high watermark for this partition.
   */
  default MessageBatch fetchMessages(StreamPartitionMsgOffset startOffset, StreamPartitionMsgOffset endOffset, int timeoutMillis)
      throws java.util.concurrent.TimeoutException {
   // TODO Issue 5359 remove this default implementation once all kafka consumers have migrated to use this API
    long startOffsetLong = ((LongMsgOffset)startOffset).getOffset();
    long endOffsetLong = endOffset == null ? Long.MAX_VALUE : ((LongMsgOffset)endOffset).getOffset();
    return fetchMessages(startOffsetLong, endOffsetLong, timeoutMillis);
  }

  default MessageBatch fetchMessages(Checkpoint startCheckpoint, Checkpoint endCheckpoint, int timeoutMs)
      throws java.util.concurrent.TimeoutException {
    return fetchMessages((StreamPartitionMsgOffset) startCheckpoint, (StreamPartitionMsgOffset) endCheckpoint,
        timeoutMs);
  }
}
