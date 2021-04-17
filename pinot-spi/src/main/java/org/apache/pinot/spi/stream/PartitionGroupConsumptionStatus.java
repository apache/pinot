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

/**
 * A PartitionGroup is a group of partitions/shards that the same consumer should consume from.
 * This class contains all information which describes the latest state of a partition group.
 * It is constructed by looking at the segment zk metadata of the latest segment of each partition group.
 * It consists of:
 * 1. partitionGroupId - A unique ID for the partitionGroup
 * 2. sequenceNumber - The sequenceNumber this partitionGroup is currently at
 * 3. startOffset - The start offset that the latest segment started consuming from
 * 4. endOffset - The endOffset (if segment consuming from this partition group has finished consuming the segment and recorded the end offset)
 * 5. status - the consumption status IN_PROGRESS/DONE
 *
 * This information is needed by the stream, when grouping the partitions/shards into new partition groups.
 */
public class PartitionGroupConsumptionStatus {

  private final int _partitionGroupId;
  private int _sequenceNumber;
  private StreamPartitionMsgOffset _startOffset;
  private StreamPartitionMsgOffset _endOffset;
  private String _status;

  public PartitionGroupConsumptionStatus(int partitionGroupId, int sequenceNumber, StreamPartitionMsgOffset startOffset,
      StreamPartitionMsgOffset endOffset, String status) {
    _partitionGroupId = partitionGroupId;
    _sequenceNumber = sequenceNumber;
    _startOffset = startOffset;
    _endOffset = endOffset;
    _status = status;
  }

  public int getPartitionGroupId() {
    return _partitionGroupId;
  }

  public int getSequenceNumber() {
    return _sequenceNumber;
  }

  public void setSequenceNumber(int sequenceNumber) {
    _sequenceNumber = sequenceNumber;
  }

  public StreamPartitionMsgOffset getStartOffset() {
    return _startOffset;
  }

  public void setStartOffset(StreamPartitionMsgOffset startOffset) {
    _startOffset = startOffset;
  }

  public StreamPartitionMsgOffset getEndOffset() {
    return _endOffset;
  }

  public void setEndOffset(StreamPartitionMsgOffset endOffset) {
    _endOffset = endOffset;
  }

  public String getStatus() {
    return _status;
  }

  public void setStatus(String status) {
    _status = status;
  }
}
