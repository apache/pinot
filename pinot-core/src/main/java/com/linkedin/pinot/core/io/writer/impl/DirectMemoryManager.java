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

package com.linkedin.pinot.core.io.writer.impl;

import com.google.common.annotations.VisibleForTesting;
import com.linkedin.pinot.common.metrics.ServerMetrics;
import com.linkedin.pinot.core.io.readerwriter.RealtimeIndexOffHeapMemoryManager;
import com.linkedin.pinot.core.segment.memory.PinotDataBuffer;
import com.yammer.metrics.core.MetricsRegistry;


// Allocates memory using direct allocation
public class DirectMemoryManager extends RealtimeIndexOffHeapMemoryManager {

  /**
   * @see RealtimeIndexOffHeapMemoryManager
   */
  public DirectMemoryManager(final String segmentName, ServerMetrics serverMetrics) {
    super(serverMetrics, segmentName);
  }

  @VisibleForTesting
  public DirectMemoryManager(final String segmentName) {
    this(segmentName, new ServerMetrics(new MetricsRegistry()));
  }

  /**
   *
   * @param size size of memory
   * @param allocationContext String describing context of allocation (typically segment:column name).
   * @return PinotDataBuffer via direct allocation
   *
   * @see {@link RealtimeIndexOffHeapMemoryManager#allocate(long, String)}
   */
  @Override
  protected PinotDataBuffer allocateInternal(long size, String allocationContext) {
    return PinotDataBuffer.allocateDirect(size, allocationContext);
  }

  @Override
  protected void doClose() {
    // Nothing to do.
  }
}
