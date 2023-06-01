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
package org.apache.pinot.segment.spi.memory;

import java.io.File;
import java.io.IOException;
import java.nio.ByteOrder;


/**
 * A factory used to create {@link PinotDataBuffer} instances.
 *
 * Normal code should not use factories directly. {@link PinotDataBuffer} static methods should be used instead. These
 * static methods delegate on a factory and also accounts the amount of memory that is being reserved and export that
 * as a {@link org.apache.pinot.spi.metrics.PinotMetric}.
 */
public interface PinotBufferFactory {

  /**
   * Returns a buffer with at least the given number of bytes. The buffer should be backed by direct memory.
   * @param size the number of bytes to allocate. A non-negative value.
   * @param byteOrder the byte order to use. Remember to do not use native if the buffer is going to be persisted.
   * @return the buffer to use. The ownership is transferred to the caller.
   */
  PinotDataBuffer allocateDirect(long size, ByteOrder byteOrder);

  /**
   * Copies the content of a file into a buffer.
   * <p>
   * Further modifications on the file or the buffer will not affect each other.
   * </p>
   * @param file The file to be read.
   * @param offset The offset in the file where the read will start.
   * @param size The number of bytes that will be read.
   * @param byteOrder The byte order the buffer will use.
   * @return a buffer with a copy of content of the file. The ownership is transferred to the caller.
   */
  default PinotDataBuffer readFile(File file, long offset, long size, ByteOrder byteOrder)
      throws IOException {
    PinotDataBuffer buffer = allocateDirect(size, byteOrder);
    buffer.readFrom(0, file, offset, size);
    return buffer;
  }

  /**
   * Maps a section of a file in memory.
   * <p>
   * Each OS has its own restrictions on memory mapped files. For example, Linux (and possible other POSIX base OS)
   * requires the offset to be page aligned. This method tries abstract all these requirements from the caller.
   * </p>
   * @param file the file to be mapped.
   * @param readOnly whether the map should be read only or not.
   * @param offset the offset in the file where the map should start. Doesn't have to be page aligned.
   * @param size the number of bytes that will be mapped.
   * @param byteOrder the byte order the buffer will use.
   * @return a buffer with a copy of content of the file. The ownership is transferred to the caller.
   */
  PinotDataBuffer mapFile(File file, boolean readOnly, long offset, long size, ByteOrder byteOrder)
      throws IOException;
}
