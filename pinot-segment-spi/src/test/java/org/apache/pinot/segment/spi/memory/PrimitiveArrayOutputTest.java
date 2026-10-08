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

import java.io.ByteArrayOutputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertThrows;


/// Checks that the bulk primitive array writes of [PinotOutputStream] and [PrimitiveArrayOutput] write the same bytes
/// as one [ByteBuffer] put per value, in pages of any size, from any offset in a page.
public class PrimitiveArrayOutputTest {
  private static final int NUM_VALUES = 5000;

  /// One primitive type: how to write a range of an array in bulk, and the per-value oracle.
  private enum Type {
    INT(Integer.BYTES) {
      @Override
      Object newArray(Random random) {
        int[] values = random.ints(NUM_VALUES).toArray();
        values[0] = Integer.MIN_VALUE;
        values[1] = -1;
        return values;
      }

      @Override
      void put(ByteBuffer buffer, Object values, int index) {
        buffer.putInt(((int[]) values)[index]);
      }

      @Override
      void writeBulk(PinotOutputStream output, Object values, int offset, int length)
          throws IOException {
        output.writeInts((int[]) values, offset, length);
      }

      @Override
      void writeChunked(DataOutput output, Object values, int offset, int length)
          throws IOException {
        PrimitiveArrayOutput.writeIntsChunked(output, (int[]) values, offset, length);
      }

      @Override
      void writeDispatched(DataOutput output, Object values, int offset, int length)
          throws IOException {
        PrimitiveArrayOutput.writeInts(output, (int[]) values, offset, length);
      }
    },
    LONG(Long.BYTES) {
      @Override
      Object newArray(Random random) {
        long[] values = random.longs(NUM_VALUES).toArray();
        values[0] = Long.MIN_VALUE;
        values[1] = -1;
        return values;
      }

      @Override
      void put(ByteBuffer buffer, Object values, int index) {
        buffer.putLong(((long[]) values)[index]);
      }

      @Override
      void writeBulk(PinotOutputStream output, Object values, int offset, int length)
          throws IOException {
        output.writeLongs((long[]) values, offset, length);
      }

      @Override
      void writeChunked(DataOutput output, Object values, int offset, int length)
          throws IOException {
        PrimitiveArrayOutput.writeLongsChunked(output, (long[]) values, offset, length);
      }

      @Override
      void writeDispatched(DataOutput output, Object values, int offset, int length)
          throws IOException {
        PrimitiveArrayOutput.writeLongs(output, (long[]) values, offset, length);
      }
    },
    DOUBLE(Double.BYTES) {
      @Override
      Object newArray(Random random) {
        double[] values = random.doubles(NUM_VALUES).map(d -> (d - 0.5) * 1e6).toArray();
        values[0] = Double.NaN;
        // A NaN with a payload keeps its raw bits, like ByteBuffer.putDouble.
        values[1] = Double.longBitsToDouble(0x7ff8_0000_0000_0123L);
        values[2] = -0.0;
        values[3] = Double.NEGATIVE_INFINITY;
        return values;
      }

      @Override
      void put(ByteBuffer buffer, Object values, int index) {
        buffer.putDouble(((double[]) values)[index]);
      }

      @Override
      void writeBulk(PinotOutputStream output, Object values, int offset, int length)
          throws IOException {
        output.writeDoubles((double[]) values, offset, length);
      }

      @Override
      void writeChunked(DataOutput output, Object values, int offset, int length)
          throws IOException {
        PrimitiveArrayOutput.writeDoublesChunked(output, (double[]) values, offset, length);
      }

      @Override
      void writeDispatched(DataOutput output, Object values, int offset, int length)
          throws IOException {
        PrimitiveArrayOutput.writeDoubles(output, (double[]) values, offset, length);
      }
    };

    final int _width;

    Type(int width) {
      _width = width;
    }

    abstract Object newArray(Random random);

    abstract void put(ByteBuffer buffer, Object values, int index);

    abstract void writeBulk(PinotOutputStream output, Object values, int offset, int length)
        throws IOException;

    abstract void writeChunked(DataOutput output, Object values, int offset, int length)
        throws IOException;

    abstract void writeDispatched(DataOutput output, Object values, int offset, int length)
        throws IOException;

    byte[] expected(int prefix, Object values, int offset, int length) {
      ByteBuffer buffer = ByteBuffer.allocate(prefix + length * _width + Integer.BYTES).order(ByteOrder.BIG_ENDIAN);
      for (int i = 0; i < prefix; i++) {
        buffer.put((byte) i);
      }
      for (int i = offset; i < offset + length; i++) {
        put(buffer, values, i);
      }
      buffer.putInt(0xCAFEBABE);
      return buffer.array();
    }
  }

  @DataProvider
  public Object[][] cases() {
    // Page sizes: one value per page or less (3, 7), not a multiple of 8 (13, 100), the default.
    int[] pageSizes = {3, 7, 13, 100, PagedPinotOutputStream.PageAllocator.MIN_RECOMMENDED_PAGE_SIZE};
    // Bytes written before the values, so the values start at different offsets in a page.
    int[] prefixes = {0, 1, 5};
    int[][] ranges = {{0, 0}, {7, 0}, {0, NUM_VALUES}, {3, NUM_VALUES - 10}, {NUM_VALUES - 1, 1}};
    List<Object[]> cases = new ArrayList<>();
    for (Type type : Type.values()) {
      for (int pageSize : pageSizes) {
        for (int prefix : prefixes) {
          for (int[] range : ranges) {
            cases.add(new Object[]{type, pageSize, prefix, range[0], range[1]});
          }
        }
      }
    }
    return cases.toArray(new Object[0][]);
  }

  @Test(dataProvider = "cases")
  public void testPagedBulkWriteMatchesPerValuePuts(Type type, int pageSize, int prefix, int offset, int length)
      throws IOException {
    Object values = type.newArray(new Random(pageSize * 31L + prefix));
    byte[] expected = type.expected(prefix, values, offset, length);
    try (PagedPinotOutputStream output = newStream(pageSize)) {
      writePrefix(output, prefix);
      type.writeBulk(output, values, offset, length);
      output.writeInt(0xCAFEBABE);
      assertEquals(output.getCurrentOffset(), expected.length);
      assertEquals(toBytes(output), expected);
    }
  }

  @Test(dataProvider = "cases")
  public void testChunkedWriteMatchesPerValuePuts(Type type, int pageSize, int prefix, int offset, int length)
      throws IOException {
    Object values = type.newArray(new Random(pageSize * 31L + prefix));
    byte[] expected = type.expected(prefix, values, offset, length);
    // The default implementation, into pages.
    try (PagedPinotOutputStream output = newStream(pageSize)) {
      writePrefix(output, prefix);
      type.writeChunked(output, values, offset, length);
      output.writeInt(0xCAFEBABE);
      assertEquals(toBytes(output), expected);
    }
    // A plain DataOutput, as used by data tables.
    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    DataOutputStream output = new DataOutputStream(bytes);
    writePrefix(output, prefix);
    type.writeDispatched(output, values, offset, length);
    output.writeInt(0xCAFEBABE);
    assertEquals(bytes.toByteArray(), expected);
  }

  /// Writing over earlier bytes after a seek back does not move the end of the written data.
  @Test
  public void testBulkWriteAfterSeekBackKeepsWrittenSize()
      throws IOException {
    try (PagedPinotOutputStream output = newStream(13)) {
      output.writeLongs(new long[]{1, 2, 3, 4}, 0, 4);
      output.seek(4);
      output.writeInts(new int[]{-1}, 0, 1);
      assertEquals(output.getCurrentOffset(), 8);
      ByteBuffer expected = ByteBuffer.allocate(32).putLong(1).putLong(2).putLong(3).putLong(4);
      expected.putInt(4, -1);
      assertEquals(toBytes(output), expected.array());
    }
  }

  @Test
  public void testInvalidRangeThrows()
      throws IOException {
    try (PagedPinotOutputStream output = newStream(16)) {
      assertThrows(IndexOutOfBoundsException.class, () -> output.writeInts(new int[2], 1, 2));
      assertThrows(IndexOutOfBoundsException.class, () -> output.writeLongs(new long[2], -1, 1));
      assertThrows(IndexOutOfBoundsException.class,
          () -> PrimitiveArrayOutput.writeDoubles(new DataOutputStream(new ByteArrayOutputStream()), new double[2], 0,
              3));
      assertEquals(output.getCurrentOffset(), 0);
    }
  }

  private static PagedPinotOutputStream newStream(int pageSize) {
    return new PagedPinotOutputStream(new PagedPinotOutputStream.HeapPageAllocator(pageSize));
  }

  private static void writePrefix(DataOutput output, int prefix)
      throws IOException {
    for (int i = 0; i < prefix; i++) {
      output.write(i);
    }
  }

  private static byte[] toBytes(PagedPinotOutputStream output) {
    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    for (ByteBuffer page : output.getPages()) {
      byte[] pageBytes = new byte[page.remaining()];
      page.get(pageBytes);
      bytes.writeBytes(pageBytes);
    }
    return bytes.toByteArray();
  }
}
