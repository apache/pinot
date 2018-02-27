/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
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

package com.linkedin.pinot.core.data.manager.realtime;

import com.linkedin.pinot.common.metrics.ServerMetrics;
import com.linkedin.pinot.core.data.manager.offline.SegmentDataManager;
import com.linkedin.pinot.core.io.readerwriter.PinotDataBufferMemoryManager;
import com.linkedin.pinot.core.io.writer.impl.DirectMemoryManager;
import com.linkedin.pinot.core.io.writer.impl.MmapMemoryManager;


public abstract class RealtimeSegmentDataManager extends SegmentDataManager {
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
}
