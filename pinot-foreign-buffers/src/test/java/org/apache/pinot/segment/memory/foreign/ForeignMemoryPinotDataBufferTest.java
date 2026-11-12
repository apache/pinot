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
package org.apache.pinot.segment.memory.foreign;

import java.io.RandomAccessFile;
import java.lang.ref.WeakReference;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.segment.spi.memory.PinotDataBuffer;
import org.apache.pinot.segment.spi.memory.PinotDataBufferTest;
import org.testng.Assert;
import org.testng.annotations.Test;


public class ForeignMemoryPinotDataBufferTest extends PinotDataBufferTest {
  public ForeignMemoryPinotDataBufferTest() {
    super(new ForeignPinotBufferFactory());
  }

  @Override
  protected boolean prioritizeByteBuffer() {
    return false;
  }

  /// Verifies that {@link PinotDataBuffer#toDirectByteBuffer} returns a zero-copy, live view over the same native
  /// memory (not a copy). This is what lets consumers such as RoaringBitmap read a buffer region in place (e.g. via
  /// {@link PinotDataBuffer#viewAsRoaringBitmap}) without materialising a copy on the query path. The check writes
  /// through the buffer AFTER the view is obtained and asserts the change is visible through the view, and vice versa.
  @Test
  public void testToDirectByteBufferIsZeroCopyView()
      throws Exception {
    try (PinotDataBuffer buffer = _factory.allocateDirect(BUFFER_SIZE, PinotDataBuffer.NATIVE_ORDER)) {
      ByteBuffer view = buffer.toDirectByteBuffer(0, BUFFER_SIZE);
      Assert.assertTrue(view.isDirect(), "view must be a direct ByteBuffer over native memory");
      Assert.assertEquals(view.capacity(), BUFFER_SIZE);
      // Write through the buffer after the view exists -> must be visible through the view (proves it is not a copy).
      buffer.putInt(0, 0x0A0B0C0D);
      Assert.assertEquals(view.getInt(0), 0x0A0B0C0D);
      // Write through the view -> must be visible through the buffer.
      view.putInt(Integer.BYTES, 0x01020304);
      Assert.assertEquals(buffer.getInt(Integer.BYTES), 0x01020304);
    }
  }

  /// Verifies the headline memory-safety guarantee: reading or writing after the backing arena is closed fails
  /// deterministically with {@link IllegalStateException} instead of corrupting memory (as the {@code Unsafe}
  /// implementation would).
  @Test
  public void testUseAfterCloseFailsDeterministically() {
    PinotDataBuffer buffer = _factory.allocateDirect(BUFFER_SIZE, PinotDataBuffer.NATIVE_ORDER);
    buffer.putInt(0, 42);
    Assert.assertEquals(buffer.getInt(0), 42);
    try {
      buffer.close();
    } catch (Exception e) {
      throw new AssertionError("close() should not throw", e);
    }
    Assert.assertThrows(IllegalStateException.class, () -> buffer.getInt(0));
    Assert.assertThrows(IllegalStateException.class, () -> buffer.putInt(0, 7));
  }

  /// Verifies that {@code long} offset addressing is correct beyond {@link Integer#MAX_VALUE}. A narrowing bug in the
  /// offset arithmetic would silently read/write the wrong location; this writes distinct values at offsets below, on
  /// and above the 2^31 boundary and reads them back.
  @Test
  public void testLargeOffsetAddressing()
      throws Exception {
    // A few bytes beyond Integer.MAX_VALUE so that an 8-byte long can be addressed strictly above the int range.
    // Offsets are chosen so that the four written regions do not overlap.
    long size = Integer.MAX_VALUE + 32L;
    long lowOffset = 0;                             // bytes [0, 8)
    long straddlingOffset = Integer.MAX_VALUE - 3L; // bytes span the 2^31 boundary: [MAX-3, MAX+5)
    long highOffset = Integer.MAX_VALUE + 8L;       // fully above the int range: [MAX+8, MAX+16)
    long lastByteOffset = size - 1;                 // last addressable byte

    try (PinotDataBuffer buffer = _factory.allocateDirect(size, PinotDataBuffer.NATIVE_ORDER)) {
      Assert.assertEquals(buffer.size(), size);
      buffer.putLong(lowOffset, 0x0102030405060708L);
      buffer.putLong(straddlingOffset, 0x1112131415161718L);
      buffer.putLong(highOffset, 0x2122232425262728L);
      buffer.putByte(lastByteOffset, (byte) 0x5A);

      Assert.assertEquals(buffer.getLong(lowOffset), 0x0102030405060708L);
      Assert.assertEquals(buffer.getLong(straddlingOffset), 0x1112131415161718L);
      Assert.assertEquals(buffer.getLong(highOffset), 0x2122232425262728L);
      Assert.assertEquals(buffer.getByte(lastByteOffset), (byte) 0x5A);
    }
  }

  /// Verifies that writing to a read-only memory-mapped buffer throws {@link IllegalArgumentException}. This is an
  /// intentional improvement over the {@code Unsafe} implementation, where writing to a {@code PROT_READ} mapping is a
  /// native SIGSEGV that crashes the JVM.
  @Test
  public void testReadOnlyMappedWriteThrows()
      throws Exception {
    try (RandomAccessFile randomAccessFile = new RandomAccessFile(TEMP_FILE, "rw")) {
      randomAccessFile.setLength(FILE_OFFSET + BUFFER_SIZE);
      try (PinotDataBuffer buffer =
          _factory.mapFile(TEMP_FILE, true, FILE_OFFSET, BUFFER_SIZE, PinotDataBuffer.NATIVE_ORDER)) {
        Assert.assertThrows(IllegalArgumentException.class, () -> buffer.putInt(0, 1));
      }
    } finally {
      FileUtils.forceDelete(TEMP_FILE);
    }
  }

  /// Verifies the leak safety net: an owning buffer that is never closed has its arena closed once the buffer is
  /// garbage collected. The probe is a [ByteBuffer] from [PinotDataBuffer#toDirectByteBuffer], which does not keep
  /// the owner reachable, so it fails with [IllegalStateException] once the arena is closed.
  @Test
  public void testLeakedBufferIsFreedOnCollection()
      throws InterruptedException {
    PinotDataBuffer buffer = _factory.allocateDirect(BUFFER_SIZE, PinotDataBuffer.NATIVE_ORDER);
    ByteBuffer probe = buffer.toDirectByteBuffer(0, Long.BYTES);
    WeakReference<PinotDataBuffer> ownerRef = new WeakReference<>(buffer);
    buffer = null;

    long deadline = System.currentTimeMillis() + 10_000;
    boolean freed = false;
    while (!freed && System.currentTimeMillis() < deadline) {
      System.gc();
      Thread.sleep(10);
      try {
        probe.getLong(0);
      } catch (IllegalStateException e) {
        freed = true;
      }
    }
    Assert.assertNull(ownerRef.get(), "the leaked buffer must be collected");
    Assert.assertTrue(freed, "the arena of a leaked buffer must be closed after the buffer is collected");
  }

  /// Verifies that a view keeps its owning buffer reachable, as the `Unsafe` and `ByteBuffer` implementations do. If
  /// the owner is leaked (never closed) while a view is still in use, the leak safety net must not close the arena
  /// under the view. This covers native and non-native order views and a view of a view.
  @Test
  public void testViewKeepsLeakedOwnerAlive()
      throws Exception {
    PinotDataBuffer owner = _factory.allocateDirect(BUFFER_SIZE, PinotDataBuffer.NATIVE_ORDER);
    owner.putLong(0, 0x1122334455667788L);
    ByteOrder nonNativeOrder =
        PinotDataBuffer.NATIVE_ORDER == ByteOrder.BIG_ENDIAN ? ByteOrder.LITTLE_ENDIAN : ByteOrder.BIG_ENDIAN;
    PinotDataBuffer nativeView = owner.view(0, Long.BYTES, PinotDataBuffer.NATIVE_ORDER);
    PinotDataBuffer nonNativeView = owner.view(0, Long.BYTES, nonNativeOrder);
    WeakReference<PinotDataBuffer> ownerRef = new WeakReference<>(owner);
    owner = null;

    forceGc();
    Assert.assertNotNull(ownerRef.get(), "a reachable view must keep its owner reachable");
    Assert.assertEquals(nativeView.getLong(0), 0x1122334455667788L);
    Assert.assertEquals(nonNativeView.getLong(0), Long.reverseBytes(0x1122334455667788L));

    // A view of a view must keep the original owner reachable too.
    PinotDataBuffer nestedView = nativeView.view(0, Long.BYTES, PinotDataBuffer.NATIVE_ORDER);
    nativeView = null;
    nonNativeView = null;
    forceGc();
    Assert.assertNotNull(ownerRef.get(), "a view of a view must keep the original owner reachable");
    Assert.assertEquals(nestedView.getLong(0), 0x1122334455667788L);

    ownerRef.get().close();
  }

  private static void forceGc()
      throws InterruptedException {
    for (int i = 0; i < 5; i++) {
      System.gc();
      Thread.sleep(20);
    }
  }
}
