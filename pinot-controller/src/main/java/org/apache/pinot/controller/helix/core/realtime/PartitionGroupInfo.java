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
package org.apache.pinot.controller.helix.core.realtime;

import org.apache.pinot.controller.helix.core.WatermarkInductionResult;
import org.apache.pinot.spi.stream.LongMsgOffset;
import org.apache.pinot.spi.stream.PartitionGroupMetadata;
import org.apache.pinot.spi.stream.StreamPartitionMsgOffset;


/**
 * The {@code PartitionGroupInfo} class represents the metadata and sequence number for a partition group.
 * It encapsulates the {@link PartitionGroupMetadata} and the sequence number associated with it.
 */
public class PartitionGroupInfo {
  PartitionGroupMetadata _metadata;
  int _sequence;

  public PartitionGroupInfo(PartitionGroupMetadata metadata, int sequence) {
    _metadata = metadata;
    _sequence = sequence;
  }

  /**
   * Creates a {@code PartitionGroupInfo} instance from a {@link WatermarkInductionResult.Watermark}.
   *
   * @param watermark The watermark object containing the partition group information.
   * @return A new {@code PartitionGroupInfo} instance.
   */
  public static PartitionGroupInfo from(WatermarkInductionResult.Watermark watermark) {
    return new PartitionGroupInfo(
        new PartitionGroupMetadata((int) watermark.getPartitionGroupId(), new LongMsgOffset(watermark.getOffset())),
        (int) watermark.getSequenceNumber());
  }

  /**
   * Gets the sequence number of the partition group.
   *
   * @return The sequence number.
   */
  public int getSequence() {
    return _sequence;
  }

  /**
   * Gets the partition group ID.
   *
   * @return The partition group ID.
   */
  public int getPartitionGroupId() {
    return _metadata.getPartitionGroupId();
  }

  /**
   * Gets the starting offset of the partition group.
   *
   * @return The starting offset.
   */
  public StreamPartitionMsgOffset getStartOffset() {
    return _metadata.getStartOffset();
  }
}
