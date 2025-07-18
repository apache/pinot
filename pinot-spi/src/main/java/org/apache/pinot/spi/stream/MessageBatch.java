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

import javax.annotation.Nullable;
import org.apache.pinot.spi.annotations.InterfaceAudience;
import org.apache.pinot.spi.annotations.InterfaceStability;


/**
 * Interface wrapping streaming messages. Throws IndexOutOfBoundsException when trying to access a message at an invalid
 * index.
 * @param <T> type of the stream message values.
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public interface MessageBatch<T> {

  /**
   * Returns the number of available messages (excluding tombstone).
   */
  int getMessageCount();

  /**
   * Returns the number of messages returned from the stream (including tombstone).
   */
  default int getUnfilteredMessageCount() {
    return getMessageCount();
  }

  /**
   * Returns the stream message at the given index within the batch.
   */
  StreamMessage<T> getStreamMessage(int index);

  /**
   * Returns the start offset of the next batch.
   */
  StreamPartitionMsgOffset getOffsetOfNextBatch();

  /**
   * Returns the offset of the first message (including tombstone) in the batch.
   * This is useful to determine if there were gaps in the stream.
   */
  @Nullable
  default StreamPartitionMsgOffset getFirstMessageOffset() {
    int numMessages = getMessageCount();
    if (numMessages == 0) {
      return null;
    }
    return getStreamMessage(0).getMetadata().getOffset();
  }

  /**
   * Returns the message metadata for the last message (including tombstone) in the batch.
   * This is useful while determining ingestion delay for a message batch.
   */
  @Nullable
  default StreamMessageMetadata getLastMessageMetadata() {
    int numMessages = getMessageCount();
    if (numMessages == 0) {
      return null;
    }
    return getStreamMessage(numMessages - 1).getMetadata();
  }

  /**
   * Returns {code true} if the current batch is the end of the consumer, and no more messages can be read from this
   * partition group.
   */
  default boolean isEndOfPartitionGroup() {
    return false;
  }

  /**
   * Returns {code true} if the current batch has data loss.
   * This is useful to determine if there were gaps in the stream.
   */
  default boolean hasDataLoss() {
    return false;
  }
}
