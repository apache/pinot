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
package org.apache.pinot.segment.local.io.readerwriter;

import java.io.Closeable;
import org.apache.pinot.segment.spi.memory.PinotDataBuffer;


/**
 * Interface for memory manager that allocates/manages PinotDataBuffer.
 * At the moment, this is far from a memory manager, and is just an allocator.
 */
public interface PinotDataBufferMemoryManager extends Closeable {

  /**
   * Allocates and returns a PinotDataBuffer of specified size with native byte order.
   *
   * @param size Size of the data buffer to be allocated.
   * @param allocationContext Context for allocation.
   * @return Allocated data buffer.
   */
  PinotDataBuffer allocate(long size, String allocationContext);

  /**
   * Returns total size of memory allocated in bytes.
   *
   * @return Total memory size in bytes.
   */
  long getTotalAllocatedBytes();
}
