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

import com.linkedin.pinot.core.io.readerwriter.OffHeapMemoryManager;
import com.linkedin.pinot.core.segment.memory.PinotDataBuffer;


// Allocates memory using direct allocation
public class DirectMemoryManager extends OffHeapMemoryManager {

  public DirectMemoryManager(final String segmentName) {
    super(segmentName);
  }

  @Override
  public PinotDataBuffer allocate(long size, String columnName) {
    return PinotDataBuffer.allocateDirect(size, getSegmentName() + "." + columnName);
  }

  @Override
  public void doClose() {
    // Nothing to do.
  }
}
