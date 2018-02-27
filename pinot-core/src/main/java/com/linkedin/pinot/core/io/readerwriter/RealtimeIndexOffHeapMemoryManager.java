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

package com.linkedin.pinot.core.io.readerwriter;

import com.google.common.base.Preconditions;
import com.linkedin.pinot.common.metrics.ServerGauge;
import com.linkedin.pinot.common.metrics.ServerMetrics;
import com.linkedin.pinot.common.utils.HLCSegmentName;
import com.linkedin.pinot.common.utils.LLCSegmentName;
import com.linkedin.pinot.common.utils.SegmentName;
import com.linkedin.pinot.core.segment.memory.PinotDataBuffer;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;


/**
 * @class RealtimeIndexOffHeapMemoryManager is an abstract class that implements base functionality to allocate and release
 * memory that is acquired during realtime segment consumption.
 *
 * Realtime consuming segments use memory for dictionary, forward index, and inverted indices. For off-heap
 * allocation of memory, we instantiate one OffHeapMemoryManager for each segment in the server,
 *
 * Closing the RealtimeOffHeapMemoryManager also releases all the resources allocated by the OffHeapMemoryManager.
 */
public abstract class RealtimeIndexOffHeapMemoryManager implements PinotDataBufferMemoryManager {
  private final List<PinotDataBuffer> _buffers = new LinkedList<>();
  private final String _segmentName;
  private final ServerMetrics _serverMetrics;
  private long _totalAllocatedBytes = 0;
  private final String _tableName;

  protected RealtimeIndexOffHeapMemoryManager(ServerMetrics serverMetrics, String segmentName) {
    _serverMetrics = serverMetrics;
    _segmentName = segmentName;
    if (SegmentName.isLowLevelConsumerSegmentName(segmentName)) {
      LLCSegmentName llcSegmentName = new LLCSegmentName(segmentName);
      _tableName = llcSegmentName.getTableName();
    } else if (SegmentName.isHighLevelConsumerSegmentName(segmentName)) {
      HLCSegmentName hlcSegmentName = new HLCSegmentName(segmentName);
      _tableName = hlcSegmentName.getTableName();
    } else {
      // For testing only
      _tableName = "NoSuchTable";
    }
  }

  /**
   * Allocate memory for use by a column.
   *
   * Sub-classes may implement this method according using different allocation policies.
   * This method can be called multiple times for each column within the segment. Each invocation
   * is guaranteed to return a new block of memory.
   *
   * @param size size of memory
   * @param allocationContext Name of the column for which memory is being allocated
   * @return PinotDataBuffer
   */
  @Override
  public PinotDataBuffer allocate(long size, String allocationContext) {
    Preconditions.checkArgument(size > 0, "Illegal memory allocation " + size + " for segment " + _segmentName + " column " + allocationContext);
    PinotDataBuffer buffer = allocateInternal(size, allocationContext);
    _totalAllocatedBytes += size;
    _buffers.add(buffer);
    _serverMetrics.addValueToTableGauge(_tableName, ServerGauge.REALTIME_OFFHEAP_MEMORY_USED, size);
    return buffer;
  }

  /**
   * Method to be implemented by inheriting concrete classes
   */
  protected abstract void doClose();

  protected abstract PinotDataBuffer allocateInternal(long size, String columnName);

  /**
   * Close out this memory manager and release all memory and resources.
   * This method must be called when all the memory allocated by this class is not longer in use.
   * The application may choose to call (or not call) PinotDataBuffer.close(), but this.close() MUST
   * be called to release all resources allocated.
   *
   * @throws IOException
   */
  public void close() throws IOException {
    for (PinotDataBuffer buffer : _buffers) {
      buffer.close();
    }
    _serverMetrics.addValueToTableGauge(_tableName, ServerGauge.REALTIME_OFFHEAP_MEMORY_USED, -_totalAllocatedBytes);
    doClose();
    _buffers.clear();
    _totalAllocatedBytes = 0;
  }

  @Override
  public long getTotalAllocatedBytes() {
    return _totalAllocatedBytes;
  }
}
