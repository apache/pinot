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
package org.apache.pinot.core.data.manager.realtime;

import java.util.Map;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.segment.local.data.manager.SegmentDataManager;
import org.apache.pinot.segment.local.io.readerwriter.PinotDataBufferMemoryManager;
import org.apache.pinot.segment.local.io.writer.impl.DirectMemoryManager;
import org.apache.pinot.segment.local.io.writer.impl.MmapMemoryManager;
import org.apache.pinot.segment.spi.MutableSegment;
import org.apache.pinot.spi.utils.CommonConstants.ConsumerState;


public abstract class RealtimeSegmentDataManager extends SegmentDataManager {

  @Override
  public abstract MutableSegment getSegment();

  protected static PinotDataBufferMemoryManager getMemoryManager(String consumerDir, String segmentName,
      boolean offHeap, boolean directOffHeap, ServerMetrics serverMetrics) {
    if (offHeap && !directOffHeap) {
      return new MmapMemoryManager(consumerDir, segmentName, serverMetrics);
    } else {
      // For on-heap allocation, we still need a memory manager for forward index.
      // Dictionary will be allocated on heap.
      return new DirectMemoryManager(segmentName, serverMetrics);
    }
  }

  /**
   * Get the current offsets for all partitions of this consumer
   */
  public abstract Map<String, String> getPartitionToCurrentOffset();

  /**
   * Get the state of the consumer
   */
  public abstract ConsumerState getConsumerState();

  public abstract long getLastConsumedTimestamp();
}
