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

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import javax.annotation.Nullable;
import org.apache.pinot.segment.spi.memory.NonNativePinotDataBuffer;
import org.apache.pinot.segment.spi.memory.PinotDataBuffer;


/// A [PinotDataBuffer] backed by the Foreign Function &amp; Memory API ([MemorySegment]).
///
/// This is the Java 21+ replacement for the `sun.misc.Unsafe` based buffer. Like the Unsafe implementation it only
/// reads and writes in [native byte order][ByteOrder#nativeOrder()]; non-native order is provided by wrapping it in a
/// [NonNativePinotDataBuffer]. Because [MemorySegment] natively supports `long` indexing, a single implementation
/// covers buffers larger than [Integer#MAX_VALUE] without any of the manual pointer arithmetic, page alignment or
/// reflective `DirectByteBuffer` construction the Unsafe path required.
///
/// All accessors use the `*_UNALIGNED` [ValueLayout] constants because callers read and write at arbitrary byte
/// offsets. Bounds checking is performed by [MemorySegment] itself (throwing [IndexOutOfBoundsException]), so this
/// class does not add its own checks. Access is memory-safe: reads and writes after the backing [Arena] is closed fail
/// deterministically instead of corrupting memory.
///
/// Thread-safety: instances created through [ForeignPinotBufferFactory] use a [shared arena][Arena#ofShared()], so the
/// buffer may be read, written and closed from any thread. As with every [PinotDataBuffer], closing a buffer while
/// another thread accesses it is a caller error. Note that closing a shared arena does not fail fast: [#release()]
/// blocks until all in-flight accesses on this buffer and its views have completed (and the inherited synchronized
/// [#close()] serialises concurrent close calls on the same instance).
public class ForeignPinotBuffer extends PinotDataBuffer {

  // Native-order, byte-aligned (alignment == 1) layouts. Single-byte access is always aligned.
  private static final ValueLayout.OfByte BYTE = ValueLayout.JAVA_BYTE;
  private static final ValueLayout.OfChar CHAR = ValueLayout.JAVA_CHAR_UNALIGNED;
  private static final ValueLayout.OfShort SHORT = ValueLayout.JAVA_SHORT_UNALIGNED;
  private static final ValueLayout.OfInt INT = ValueLayout.JAVA_INT_UNALIGNED;
  private static final ValueLayout.OfLong LONG = ValueLayout.JAVA_LONG_UNALIGNED;
  private static final ValueLayout.OfFloat FLOAT = ValueLayout.JAVA_FLOAT_UNALIGNED;
  private static final ValueLayout.OfDouble DOUBLE = ValueLayout.JAVA_DOUBLE_UNALIGNED;

  private final MemorySegment _segment;
  /// The arena owning the native memory, or {@code null} when this buffer is a non-owning view. Only owning buffers
  /// close the arena on [#release()].
  @Nullable
  private final Arena _arena;

  /// Creates a buffer over the given segment.
  ///
  /// @param segment the memory segment this buffer reads and writes. Its [MemorySegment#byteSize()] is the buffer size.
  /// @param arena the arena to close when this buffer is released, or {@code null} for a non-owning view.
  public ForeignPinotBuffer(MemorySegment segment, @Nullable Arena arena) {
    super(arena != null);
    _segment = segment;
    _arena = arena;
  }

  @Override
  public byte getByte(long offset) {
    return _segment.get(BYTE, offset);
  }

  @Override
  public void putByte(long offset, byte value) {
    _segment.set(BYTE, offset, value);
  }

  @Override
  public char getChar(long offset) {
    return _segment.get(CHAR, offset);
  }

  @Override
  public void putChar(long offset, char value) {
    _segment.set(CHAR, offset, value);
  }

  @Override
  public short getShort(long offset) {
    return _segment.get(SHORT, offset);
  }

  @Override
  public void putShort(long offset, short value) {
    _segment.set(SHORT, offset, value);
  }

  @Override
  public int getInt(long offset) {
    return _segment.get(INT, offset);
  }

  @Override
  public void putInt(long offset, int value) {
    _segment.set(INT, offset, value);
  }

  @Override
  public long getLong(long offset) {
    return _segment.get(LONG, offset);
  }

  @Override
  public void putLong(long offset, long value) {
    _segment.set(LONG, offset, value);
  }

  @Override
  public float getFloat(long offset) {
    return _segment.get(FLOAT, offset);
  }

  @Override
  public void putFloat(long offset, float value) {
    _segment.set(FLOAT, offset, value);
  }

  @Override
  public double getDouble(long offset) {
    return _segment.get(DOUBLE, offset);
  }

  @Override
  public void putDouble(long offset, double value) {
    _segment.set(DOUBLE, offset, value);
  }

  @Override
  public void copyTo(long offset, byte[] buffer, int destOffset, int size) {
    // Direct segment-to-array copy: a bounds-checked, JIT-intrinsified memcpy with no intermediate ByteBuffer.
    MemorySegment.copy(_segment, BYTE, offset, buffer, destOffset, size);
  }

  @Override
  public void readFrom(long offset, byte[] buffer, int srcOffset, int size) {
    // Direct array-to-segment copy: a bounds-checked, JIT-intrinsified memcpy with no intermediate ByteBuffer.
    MemorySegment.copy(buffer, srcOffset, _segment, BYTE, offset, size);
  }

  @Override
  public long size() {
    return _segment.byteSize();
  }

  @Override
  public ByteOrder order() {
    return ByteOrder.nativeOrder();
  }

  @Override
  public PinotDataBuffer view(long start, long end, ByteOrder byteOrder) {
    // asSlice validates start/size against the segment bounds.
    ForeignPinotBuffer nativeView = new ForeignPinotBuffer(_segment.asSlice(start, end - start), null);
    if (byteOrder == ByteOrder.nativeOrder()) {
      return nativeView;
    } else {
      return new NonNativePinotDataBuffer(nativeView);
    }
  }

  @Override
  public ByteBuffer toDirectByteBuffer(long offset, int size, ByteOrder byteOrder) {
    // The returned ByteBuffer is a borrowed view over the same native memory and must not be freed by the caller.
    // It stays valid for as long as the backing arena is alive.
    return _segment.asSlice(offset, size).asByteBuffer().order(byteOrder);
  }

  @Override
  public void flush() {
    // force() is only valid on a writable mapped segment; it is a no-op for anonymous (allocated) memory.
    if (_segment.isMapped() && !_segment.isReadOnly()) {
      _segment.force();
    }
  }

  @Override
  public void release() {
    if (_arena != null) {
      _arena.close();
    }
  }
}
