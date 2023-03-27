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
package org.apache.pinot.foreign.memory;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import org.apache.pinot.segment.spi.memory.PinotBufferFactory;
import org.apache.pinot.segment.spi.memory.PinotDataBuffer;


public class ForeignMemoryPinotBufferFactory implements PinotBufferFactory {
  @Override
  public PinotDataBuffer allocateDirect(long size, ByteOrder byteOrder) {
    Arena arena = Arena.openShared();
    MemorySegment memorySegment = arena.allocate(size);

    if (byteOrder == ByteOrder.BIG_ENDIAN) {
      return new BigEndianForeignMemoryPinotDataBuffer(memorySegment, arena);
    } else {
      return new LittleEndianForeignMemoryPinotDataBuffer(memorySegment, arena);
    }
  }

  @Override
  public PinotDataBuffer mapFile(File file, boolean readOnly, long offset, long size, ByteOrder byteOrder)
      throws IOException {

    String mode = readOnly ? "r" : "rw";
    FileChannel.MapMode mapMode = readOnly ? FileChannel.MapMode.READ_ONLY : FileChannel.MapMode.READ_WRITE;

    try (RandomAccessFile raf = new RandomAccessFile(file, mode);
        FileChannel fileChannel = raf.getChannel()) {
      Arena arena = Arena.openShared();
      MemorySegment memorySegment = fileChannel.map(mapMode, offset, size, arena.scope());

      return byteOrder == ByteOrder.BIG_ENDIAN
          ? new BigEndianForeignMemoryPinotDataBuffer(memorySegment, arena)
          : new LittleEndianForeignMemoryPinotDataBuffer(memorySegment, arena);
    }
  }
}
