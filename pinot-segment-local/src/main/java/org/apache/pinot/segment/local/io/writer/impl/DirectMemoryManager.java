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
package org.apache.pinot.segment.local.io.writer.impl;

import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.common.utils.LLCSegmentName;
import org.apache.pinot.segment.local.io.readerwriter.RealtimeIndexOffHeapMemoryManager;
import org.apache.pinot.segment.spi.memory.PinotDataBuffer;
import org.apache.pinot.spi.metrics.PinotMetricUtils;


// Allocates memory using direct allocation
public class DirectMemoryManager extends RealtimeIndexOffHeapMemoryManager {

  /**
   * @see RealtimeIndexOffHeapMemoryManager
   */
  public DirectMemoryManager(final String tableName, final String segmentName, ServerMetrics serverMetrics) {
    super(serverMetrics, tableName, segmentName);
  }

  public DirectMemoryManager(final String tableName, final String segmentName) {
    this(tableName, segmentName, new ServerMetrics(PinotMetricUtils.getPinotMetricsRegistry()));
  }

  public DirectMemoryManager(final String segmentName) {
    this(LLCSegmentName.of(segmentName) == null ? "NoSuchTable" : LLCSegmentName.of(segmentName).getTableName(),
        segmentName, new ServerMetrics(PinotMetricUtils.getPinotMetricsRegistry()));
  }

  /**
   *
   * @param size size of memory
   * @param allocationContext String describing context of allocation (typically segment:column name).
   * @return PinotDataBuffer via direct allocation
   *
   * @see RealtimeIndexOffHeapMemoryManager#allocate(long, String)
   */
  @Override
  protected PinotDataBuffer allocateInternal(long size, String allocationContext) {
    return PinotDataBuffer.allocateDirect(size, PinotDataBuffer.NATIVE_ORDER, allocationContext);
  }

  @Override
  protected void doClose() {
    // Nothing to do.
  }
}
