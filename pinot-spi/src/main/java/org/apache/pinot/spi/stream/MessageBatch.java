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
   *
   * @return number of messages returned from the stream
   */
  int getMessageCount();

  /**
   * Returns the message at a particular index inside a set of messages returned from the stream.
   * @param index
   * @return
   */
  T getMessageAtIndex(int index);

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
  default StreamPartitionMsgOffset getNextStreamParitionMsgOffsetAtIndex(int index) {
    return new LongMsgOffset(getNextStreamMessageOffsetAtIndex(index));
  }
}
