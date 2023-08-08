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
 * Interface wrapping stream consumer. Throws IndexOutOfBoundsException when trying to access a message at an
 * invalid index.
 * @param <T>
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public interface MessageBatch<T> {
  /**
   * @return number of available messages
   */
  int getMessageCount();

  /**
   * @return number of messages returned from the stream
   */
  default int getUnfilteredMessageCount() {
    return getMessageCount();
  }

  /**
   * Returns the message at a particular index inside a set of messages returned from the stream.
   * @param index
   * @return
   */
  @Deprecated
  T getMessageAtIndex(int index);

  // for backward-compatibility
  default byte[] getMessageBytesAtIndex(int index) {
    return (byte[]) getMessageAtIndex(index);
  }

  default StreamMessage<T> getStreamMessage(int index) {
    return new LegacyStreamMessage(getMessageBytesAtIndex(index), (StreamMessageMetadata) getMetadataAtIndex(index));
  }

  class LegacyStreamMessage extends StreamMessage {
    public LegacyStreamMessage(byte[] value, StreamMessageMetadata metadata) {
      super(value, value.length, metadata);
    }
  }
  /**
   * Returns the offset of the message at a particular index inside a set of messages returned from the stream.
   * @param index
   * @return
   */
  int getMessageOffsetAtIndex(int index);

  /**
   * Returns the length of the message at a particular index inside a set of messages returned from the stream.
   * @param index
   * @return
   */
  int getMessageLengthAtIndex(int index);

  /**
   * Returns the metadata associated with the message at a particular index. This typically includes the timestamp
   * when the message was ingested by the upstream stream-provider and other relevant metadata.
   */
  default RowMetadata getMetadataAtIndex(int index) {
    return null;
  }

  /**
   * Returns the offset of the next message.
   * @param index
   * @return
   */
  @Deprecated
  long getNextStreamMessageOffsetAtIndex(int index);

  /**
   * Returns the offset of the next message.
   * @param index
   * @return
   */
  default StreamPartitionMsgOffset getNextStreamPartitionMsgOffsetAtIndex(int index) {
    return new LongMsgOffset(getNextStreamMessageOffsetAtIndex(index));
  }

  /**
   * @return last offset in the batch
   */
  default StreamPartitionMsgOffset getOffsetOfNextBatch() {
    return getNextStreamPartitionMsgOffsetAtIndex(getMessageCount() - 1);
  }

  /**
   * Returns true if end of the consumer detects that no more records can be read from this partition group for good
   */
  default boolean isEndOfPartitionGroup() {
    return false;
  }

  /**
   * This is useful while determining ingestion delay for a message batch. Retaining metadata for last filtered message
   * in a batch can enable us to estimate the ingestion delay for the batch.
   * Note that a batch can be fully filtered, and we can still retain the metadata for the last filtered message to
   * facilitate computing ingestion delay in the face of a fully filtered batch.
   *
   * @return null by default.
   */
  @Nullable
  default public StreamMessageMetadata getLastMessageMetadata() {
    return null;
  }
}
