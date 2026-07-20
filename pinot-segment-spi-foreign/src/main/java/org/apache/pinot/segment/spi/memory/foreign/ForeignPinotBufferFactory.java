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
package org.apache.pinot.segment.spi.memory.foreign;

import java.io.File;
import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import org.apache.pinot.segment.spi.memory.NonNativePinotDataBuffer;
import org.apache.pinot.segment.spi.memory.PinotBufferFactory;
import org.apache.pinot.segment.spi.memory.PinotDataBuffer;


/// A [PinotBufferFactory] that creates buffers backed by the Foreign Function &amp; Memory API.
///
/// This is the Java 21+ alternative to
/// [org.apache.pinot.segment.spi.memory.unsafe.UnsafePinotBufferFactory]. It produces [ForeignPinotBuffer] instances
/// backed by a [MemorySegment]. Every buffer owns a dedicated [shared arena][Arena#ofShared()] so that the buffer can
/// be accessed and closed from any thread; closing the buffer closes the arena, which frees the native memory or
/// unmaps the file.
///
/// Unlike the Unsafe factory this works on every operating system (including Windows), needs no reflection, and maps
/// files larger than [Integer#MAX_VALUE] directly through [FileChannel#map(FileChannel.MapMode, long, long, Arena)].
///
/// To select this factory set the `pinot.offheap.buffer.factory` configuration (or the `PINOT_BUFFER_LIBRARY`
/// environment variable) to this class' fully qualified name.
public class ForeignPinotBufferFactory implements PinotBufferFactory {

  @Override
  public PinotDataBuffer allocateDirect(long size, ByteOrder byteOrder) {
    Arena arena = Arena.ofShared();
    try {
      MemorySegment segment = arena.allocate(size);
      return wrap(new ForeignPinotBuffer(segment, arena), byteOrder);
    } catch (RuntimeException | Error e) {
      arena.close();
      throw e;
    }
  }

  @Override
  public PinotDataBuffer mapFile(File file, boolean readOnly, long offset, long size, ByteOrder byteOrder)
      throws IOException {
    FileChannel.MapMode mapMode = readOnly ? FileChannel.MapMode.READ_ONLY : FileChannel.MapMode.READ_WRITE;
    // The channel can be closed right after mapping; the mapping outlives it (as with MappedByteBuffer). The mapping
    // itself is kept alive by the arena and released when the arena is closed.
    Arena arena = Arena.ofShared();
    try (FileChannel channel = openChannel(file, readOnly)) {
      MemorySegment segment = channel.map(mapMode, offset, size, arena);
      return wrap(new ForeignPinotBuffer(segment, arena), byteOrder);
    } catch (RuntimeException | Error | IOException e) {
      arena.close();
      throw e;
    }
  }

  private static FileChannel openChannel(File file, boolean readOnly)
      throws IOException {
    if (readOnly) {
      return FileChannel.open(file.toPath(), StandardOpenOption.READ);
    }
    return FileChannel.open(file.toPath(), StandardOpenOption.READ, StandardOpenOption.WRITE,
        StandardOpenOption.CREATE);
  }

  private static PinotDataBuffer wrap(ForeignPinotBuffer buffer, ByteOrder byteOrder) {
    if (byteOrder == ByteOrder.nativeOrder()) {
      return buffer;
    } else {
      return new NonNativePinotDataBuffer(buffer);
    }
  }
}
