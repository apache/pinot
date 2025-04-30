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

import org.apache.pinot.spi.annotations.InterfaceStability;


/**
 * An interface to be implemented by streams consumed using Pinot LLC consumers.
 * Pinot expects a stream partition to be a queue of messages. Each time a message
 * is appended to the queue, the offset of that message should be higher than all
 * the previous messages appended to the queue. Messages will be retrieved (by Pinot)
 * in the order in which they were appended to the queue.
 *
 * This object represents a direct reference to a message within a stream partition.
 *
 * The behavior of the {@link #compareTo(Object other)} method when comparing offsets
 * across partitions is undefined. Pinot will not invoke this method to compare offsets
 * across stream partitions
 *
 * It is useful to note that these comparators and serialization methods should not change
 * across versions of the stream implementation. Pinot stores serialized version of
 * the offset in metadata. Also, Pinot may consume a message delivered by earlier (or later)
 * versions of the stream implementation
 */
@InterfaceStability.Evolving
public interface StreamPartitionMsgOffset extends Comparable<StreamPartitionMsgOffset> {

  /**
   *  A serialized representation of the offset object as a String.
   */
  String toString();

  /**
   *  Few streams like Kinesis can return invalid offsets.
   *  For example: In Kinesis shards that are in the OPEN state have an ending sequence number of null.
   *  This method can be used to compare if offset is valid for comparison with other offset.
   */
  default boolean isValidOffset() {
    return true;
  }

  /**
   * @deprecated Should be done via a static function
   */
  @Deprecated
  default StreamPartitionMsgOffset fromString(String streamPartitionMsgOffsetStr) {
    throw new UnsupportedOperationException();
  }
}
