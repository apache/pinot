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
import java.util.concurrent.TimeoutException;

/**
 * Consumer interface for consuming from a partition group of a stream
 */
public interface PartitionGroupConsumer extends Closeable {
  /**
   * Starts a stream consumer
   *
   * This is useful in cases where starting the consumer involves preparing / initializing the source.
   * A typical example is that of an asynchronous / non-poll based consumption model where this method will be used to
   * setup or initialize the consumer to fetch messages from the source stream.
   *
   * Poll-based consumers can optionally use this to prefetch metadata from the source.
   *
   * This method should be invoked by the caller before trying to invoke
   * {@link #fetchMessages(StreamPartitionMsgOffset, StreamPartitionMsgOffset, int)}.
   *
   * @param startOffset Offset (inclusive) at which the consumption should begin
   */
  default void start(StreamPartitionMsgOffset startOffset) {
  }

  /**
   * Return messages from the stream partition group within the specified timeout
   *
   * The message may be fetched by actively polling the source or by retrieving from a pre-fetched buffer. This depends
   * on the implementation.
   *
   * @param startOffset The offset of the first message desired, inclusive
   * @param endOffset The offset of the last message desired, exclusive, or null
   * @param timeoutMs Timeout in milliseconds
   * @throws java.util.concurrent.TimeoutException If the operation could not be completed within {@code timeoutMillis}
   * milliseconds
   * @return An iterable containing messages fetched from the stream partition and their offsets
   */
  MessageBatch fetchMessages(StreamPartitionMsgOffset startOffset, StreamPartitionMsgOffset endOffset, int timeoutMs)
      throws TimeoutException;

  /**
   * Checkpoints the consumption state of the stream partition group in the source
   *
   * This is useful in streaming systems that require preserving consumption state on the source in order to resume or
   * replay consumption of data. The consumer implementation is responsible for managing this state.
   *
   * The offset returned will be used for offset comparisons within the local server (say, for catching up) and also,
   * persisted to the ZK segment metadata. Hence, the returned value should be same or equivalent to the lastOffset
   * provided as input (that is, compareTo of the input and returned offset should be 0).
   *
   * @param lastOffset checkpoint the stream at this offset (exclusive)
   * @return Returns the offset that should be used as the next upcoming offset for the stream partition group
   */
  default StreamPartitionMsgOffset checkpoint(StreamPartitionMsgOffset lastOffset) {
    return lastOffset;
  }
}
