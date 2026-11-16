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
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.testng.Assert.*;


public class PagedPinotOutputStreamTest {

  private static final int NUM_BULK_VALUES = 5000;

  int _pageSize;
  PagedPinotOutputStream _pagedPinotOutputStream;

  @BeforeMethod
  public void setUp() {
    _pagedPinotOutputStream = PagedPinotOutputStream.createHeap();
    _pageSize = _pagedPinotOutputStream.getPageSize();
  }

  @Test
  void writeByteInPage()
      throws IOException {
    byte b = 42;
    _pagedPinotOutputStream.write(b);
    assertEquals(_pagedPinotOutputStream.getCurrentOffset(), 1);

    byte read = _pagedPinotOutputStream.asBuffer(ByteOrder.BIG_ENDIAN, false).getByte(0);
    assertEquals(read, b);
  }

  @Test
  void writeByteAcrossPage()
      throws IOException {
    byte b = 42;

    _pagedPinotOutputStream.seek(_pageSize);
    _pagedPinotOutputStream.write(b);

    assertEquals(_pagedPinotOutputStream.getPages().length, 2);

    byte read = _pagedPinotOutputStream.asBuffer(ByteOrder.BIG_ENDIAN, false).getByte(_pageSize);
    assertEquals(read, b);
  }

  @Test
  void writeIntInPage()
      throws IOException {
    int i = 42;
    _pagedPinotOutputStream.writeInt(i);
    assertEquals(_pagedPinotOutputStream.getCurrentOffset(), Integer.BYTES);

    int read = _pagedPinotOutputStream.asBuffer(ByteOrder.BIG_ENDIAN, false).getInt(0);
    assertEquals(read, i);
  }

  @Test
  void writeIntAcrossPage()
      throws IOException {
    int i = 42;

    _pagedPinotOutputStream.seek(_pageSize - Integer.BYTES + 1);
    _pagedPinotOutputStream.writeInt(i);

    assertEquals(_pagedPinotOutputStream.getPages().length, 2);

    int read = _pagedPinotOutputStream.asBuffer(ByteOrder.BIG_ENDIAN, false).getInt(_pageSize - Integer.BYTES + 1);
    assertEquals(read, i);
  }

  @Test
  void writeLongInPage()
      throws IOException {
    long l = 42;
    _pagedPinotOutputStream.writeLong(l);
    assertEquals(_pagedPinotOutputStream.getCurrentOffset(), Long.BYTES);

    long read = _pagedPinotOutputStream.asBuffer(ByteOrder.BIG_ENDIAN, false).getLong(0);
    assertEquals(read, l);
  }

  @Test
  void writeLongAcrossPage()
      throws IOException {
    long l = 42;

    _pagedPinotOutputStream.seek(_pageSize - Long.BYTES + 1);
    _pagedPinotOutputStream.writeLong(l);

    assertEquals(_pagedPinotOutputStream.getPages().length, 2);

    long read = _pagedPinotOutputStream.asBuffer(ByteOrder.BIG_ENDIAN, false).getLong(_pageSize - Long.BYTES + 1);
    assertEquals(read, l);
  }

  @Test
  void writeShortInPage()
      throws IOException {
    short s = 42;
    _pagedPinotOutputStream.writeShort(s);
    assertEquals(_pagedPinotOutputStream.getCurrentOffset(), Short.BYTES);

    short read = _pagedPinotOutputStream.asBuffer(ByteOrder.BIG_ENDIAN, false).getShort(0);
    assertEquals(read, s);
  }

  @Test
  void writeShortAcrossPage()
      throws IOException {
    short s = 42;

    _pagedPinotOutputStream.seek(_pageSize - Short.BYTES + 1);
    _pagedPinotOutputStream.writeShort(s);

    assertEquals(_pagedPinotOutputStream.getPages().length, 2);

    short read = _pagedPinotOutputStream.asBuffer(ByteOrder.BIG_ENDIAN, false).getShort(_pageSize - Short.BYTES + 1);
    assertEquals(read, s);
  }

  @Test
  void seekIntoPageStart()
      throws IOException {
    int i = 42;
    _pagedPinotOutputStream.seek(_pageSize);
    assertEquals(_pagedPinotOutputStream.getCurrentOffset(), _pageSize);

    _pagedPinotOutputStream.writeInt(i);

    int read = _pagedPinotOutputStream.asBuffer(ByteOrder.BIG_ENDIAN, false).getInt(_pageSize);
    assertEquals(read, i);
  }

  @Test
  void seekIntoNegative() {
    assertThrows(IllegalArgumentException.class, () -> _pagedPinotOutputStream.seek(-1));
  }

  @Test
  void seekIntoNotExistentPage()
      throws IOException {
    int i = 42;
    long newPosition = _pageSize * 10L + 2;
    _pagedPinotOutputStream.seek(newPosition);

    _pagedPinotOutputStream.writeInt(i);

    int read = _pagedPinotOutputStream.asBuffer(ByteOrder.BIG_ENDIAN, false).getInt(newPosition);
    assertEquals(read, i);
  }

  @Test
  void seekIntoStart()
      throws IOException {
    int i = 42;
    _pagedPinotOutputStream.seek(123);
    _pagedPinotOutputStream.seek(0);

    _pagedPinotOutputStream.writeInt(i);

    int read = _pagedPinotOutputStream.asBuffer(ByteOrder.BIG_ENDIAN, false).getInt(0);
    assertEquals(read, i);
  }

  @Test
  void seekIntoStartFromLaterPage()
      throws IOException {
    int i = 42;
    _pagedPinotOutputStream.seek(_pageSize * 2L);
    _pagedPinotOutputStream.seek(0);
    assertEquals(_pagedPinotOutputStream.getCurrentOffset(), 0);

    _pagedPinotOutputStream.writeInt(i);

    assertEquals(_pagedPinotOutputStream.getCurrentOffset(), Integer.BYTES);
    int read = _pagedPinotOutputStream.asBuffer(ByteOrder.BIG_ENDIAN, false).getInt(0);
    assertEquals(read, i);
  }

  @Test
  void writeLargeByteArray()
      throws IOException {
    Random r = new Random(42);
    byte[] bytes = new byte[_pageSize + 1];
    r.nextBytes(bytes);

    _pagedPinotOutputStream.write(bytes);

    byte[] read = new byte[_pageSize + 1];
    _pagedPinotOutputStream.asBuffer(ByteOrder.BIG_ENDIAN, false).copyTo(0, read);

    assertEquals(read, bytes);
    assertEquals(_pagedPinotOutputStream.getPages().length, 2);
  }

  @Test
  void writeSmallByteArray()
      throws IOException {
    Random r = new Random(42);
    byte[] bytes = new byte[_pageSize / 2];
    r.nextBytes(bytes);

    _pagedPinotOutputStream.write(bytes);

    byte[] read = new byte[_pageSize / 2];
    _pagedPinotOutputStream.asBuffer(ByteOrder.BIG_ENDIAN, false).copyTo(0, read);

    assertEquals(read, bytes);
    assertEquals(_pagedPinotOutputStream.getPages().length, 1);
  }

  @Test
  void writeLargeDataInput()
      throws IOException {
    Random r = new Random(42);
    byte[] bytes = new byte[_pageSize + 1];
    r.nextBytes(bytes);

    _pagedPinotOutputStream.write(PinotByteBuffer.wrap(bytes));

    byte[] read = new byte[_pageSize + 1];
    _pagedPinotOutputStream.asBuffer(ByteOrder.BIG_ENDIAN, false).copyTo(0, read);

    assertEquals(read, bytes);
    assertEquals(_pagedPinotOutputStream.getPages().length, 2);
  }

  @Test
  void writeSmallDataInput()
      throws IOException {
    Random r = new Random(42);
    byte[] bytes = new byte[_pageSize / 2];
    r.nextBytes(bytes);

    _pagedPinotOutputStream.write(PinotByteBuffer.wrap(bytes));

    byte[] read = new byte[_pageSize / 2];
    _pagedPinotOutputStream.asBuffer(ByteOrder.BIG_ENDIAN, false).copyTo(0, read);

    assertEquals(read, bytes);
    assertEquals(_pagedPinotOutputStream.getPages().length, 1);
  }

  @Test
  void testGetPagesEmpty() {
    ByteBuffer[] pages = _pagedPinotOutputStream.getPages();
    assertEquals(pages.length, 0);
  }

  @Test
  void testGetPagesSingleIntWrite()
      throws IOException {
    _pagedPinotOutputStream.writeInt(42);

    ByteBuffer[] pages = _pagedPinotOutputStream.getPages();
    assertEquals(pages.length, 1);
    assertEquals(pages[0].position(), 0);
    assertEquals(pages[0].limit(), 4);

    assertEquals(pages[0].getInt(0), 42);
  }

  @Test
  void testGetPagesAfterSeekingInTheMiddleOfAPage() {
    _pagedPinotOutputStream.seek(_pageSize * 2L + 1);

    ByteBuffer[] pages = _pagedPinotOutputStream.getPages();
    assertEquals(pages.length, 3);

    assertEquals(pages[0].position(), 0);
    assertEquals(pages[0].limit(), _pageSize);

    assertEquals(pages[1].position(), 0);
    assertEquals(pages[1].limit(), _pageSize);

    assertEquals(pages[2].position(), 0);
    assertEquals(pages[2].limit(), 1);
  }

  @Test
  void testGetPagesAfterSeekingInTheStartOfAPage() {
    _pagedPinotOutputStream.seek(_pageSize * 2L);

    ByteBuffer[] pages = _pagedPinotOutputStream.getPages();
    assertEquals(pages.length, 2);

    assertEquals(pages[0].position(), 0);
    assertEquals(pages[0].limit(), _pageSize);

    assertEquals(pages[1].position(), 0);
    assertEquals(pages[1].limit(), _pageSize);
  }

  @Test
  void testGetPagesUsesWrittenInsteadOfOffset() {
    _pagedPinotOutputStream.seek(_pageSize * 2L);
    _pagedPinotOutputStream.seek(1);

    ByteBuffer[] pages = _pagedPinotOutputStream.getPages();
    assertEquals(pages.length, 2);

    assertEquals(pages[0].position(), 0);
    assertEquals(pages[0].limit(), _pageSize);

    assertEquals(pages[1].position(), 0);
    assertEquals(pages[1].limit(), _pageSize);
  }

  /// This tests that getPages works as expected when data has been written to the stream and all the pages are full.
  ///
  /// Originally there was an error that caused the last returned page to be empty.
  ///
  /// Detected in [#14375](https://github.com/apache/pinot/issues/14375)
  /// @throws IOException
  @Test
  void testGetPagesFullPages()
      throws IOException {
    byte[] bytes = new byte[_pageSize];
    _pagedPinotOutputStream.write(bytes);
    _pagedPinotOutputStream.write(bytes);

    ByteBuffer[] pages = _pagedPinotOutputStream.getPages();
    assertEquals(pages.length, 2);

    assertEquals(pages[0].position(), 0);
    assertEquals(pages[0].limit(), _pageSize);

    assertEquals(pages[1].position(), 0);
    assertEquals(pages[1].limit(), _pageSize);
  }

  /// One primitive type of the bulk writes: how to write a range of an array in bulk, and the per-value oracle.
  private enum BulkType {
    INT(Integer.BYTES) {
      @Override
      Object newArray(Random random) {
        int[] values = random.ints(NUM_BULK_VALUES).toArray();
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
    },
    LONG(Long.BYTES) {
      @Override
      Object newArray(Random random) {
        long[] values = random.longs(NUM_BULK_VALUES).toArray();
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
    },
    FLOAT(Float.BYTES) {
      @Override
      Object newArray(Random random) {
        float[] values = new float[NUM_BULK_VALUES];
        for (int i = 0; i < NUM_BULK_VALUES; i++) {
          values[i] = (random.nextFloat() - 0.5f) * 1e6f;
        }
        values[0] = Float.NaN;
        // A NaN with a payload keeps its raw bits, like ByteBuffer.putFloat.
        values[1] = Float.intBitsToFloat(0x7fa1_2345);
        values[2] = -0.0f;
        values[3] = Float.NEGATIVE_INFINITY;
        return values;
      }

      @Override
      void put(ByteBuffer buffer, Object values, int index) {
        buffer.putFloat(((float[]) values)[index]);
      }

      @Override
      void writeBulk(PinotOutputStream output, Object values, int offset, int length)
          throws IOException {
        output.writeFloats((float[]) values, offset, length);
      }
    },
    DOUBLE(Double.BYTES) {
      @Override
      Object newArray(Random random) {
        double[] values = random.doubles(NUM_BULK_VALUES).map(d -> (d - 0.5) * 1e6).toArray();
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
    };

    final int _width;

    BulkType(int width) {
      _width = width;
    }

    abstract Object newArray(Random random);

    abstract void put(ByteBuffer buffer, Object values, int index);

    abstract void writeBulk(PinotOutputStream output, Object values, int offset, int length)
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
  public Object[][] bulkWriteCases() {
    // Page sizes: one value per page or less (3, 7), not a multiple of 8 (13, 100), the default.
    int[] pageSizes = {3, 7, 13, 100, PagedPinotOutputStream.PageAllocator.MIN_RECOMMENDED_PAGE_SIZE};
    // Bytes written before the values, so the values start at different offsets in a page.
    int[] prefixes = {0, 1, 5};
    // Lengths on both sides of PrimitiveArrays.MIN_BULK_VALUES (64), where the per-value path takes over.
    int[][] ranges = {
        {0, 0}, {7, 0}, {0, NUM_BULK_VALUES}, {3, NUM_BULK_VALUES - 10}, {NUM_BULK_VALUES - 1, 1}, {9, 63}, {9, 64}
    };
    List<Object[]> cases = new ArrayList<>();
    for (BulkType type : BulkType.values()) {
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

  /// The bulk writes write the same bytes as one [ByteBuffer] put per value, in pages of any size, from any offset in
  /// a page.
  @Test(dataProvider = "bulkWriteCases")
  public void testBulkWriteMatchesPerValuePuts(BulkType type, int pageSize, int prefix, int offset, int length)
      throws IOException {
    Object values = type.newArray(new Random(pageSize * 31L + prefix));
    byte[] expected = type.expected(prefix, values, offset, length);
    try (PagedPinotOutputStream output = newStream(pageSize)) {
      for (int i = 0; i < prefix; i++) {
        output.write(i);
      }
      type.writeBulk(output, values, offset, length);
      output.writeInt(0xCAFEBABE);
      assertEquals(output.getCurrentOffset(), expected.length);
      assertEquals(toBytes(output), expected);
    }
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

  private static PagedPinotOutputStream newStream(int pageSize) {
    return new PagedPinotOutputStream(new PagedPinotOutputStream.HeapPageAllocator(pageSize));
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
