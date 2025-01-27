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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Random;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.testng.Assert.*;


public class PagedPinotOutputStreamTest {

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

  /**
   * This tests that getPages works as expected when data has been written to the stream and all the pages are full.
   *
   * Originally there was an error that caused the last returned page to be empty.
   *
   * Detected in <a href="https://github.com/apache/pinot/issues/14375">#14375</a>
   * @throws IOException
   */
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
}
