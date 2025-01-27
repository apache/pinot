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

import com.google.common.base.Preconditions;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * An implementation of {@link PinotOutputStream} that writes to a sequence of pages.
 * <p>
 * When a page is full, a new page is allocated and writing continues there.
 * The page size is determined by the {@link PageAllocator} used to create the stream.
 * <p>
 * This class is specially useful when writing data whose final size is not known in advance or when it is needed to
 * {@link #seek(long) seek} to a specific position in the stream.
 * Writes that cross page boundaries are handled transparently, although a performance penalty will be paid.
 * <p>
 * Some {@link PageAllocator} implementations may support releasing pages, which can be useful to reduce memory usage.
 * The smaller the page size, the more pages will be allocated, but the allocation is quite faster. This is specially
 * important in the case of heap allocation, in which case allocations smaller than TLAB will be faster.
 * <p>
 * Data written in this stream can be retrieved as a list of {@link ByteBuffer} pages using {@link #getPages()}, which
 * can be directly read using {@link CompoundDataBuffer} or send to the network as using
 * {@link com.google.protobuf.UnsafeByteOperations#unsafeWrap(byte[]) GRPC}.
 */
public class PagedPinotOutputStream extends PinotOutputStream {
  private final PageAllocator _allocator;
  private final int _pageSize;
  private final ArrayList<ByteBuffer> _pages;
  private ByteBuffer _currentPage;
  private long _currentPageStartOffset;
  private int _offsetInPage;
  private long _written = 0;

  private static final Logger LOGGER = LoggerFactory.getLogger(PagedPinotOutputStream.class);

  public PagedPinotOutputStream(PageAllocator allocator) {
    _pageSize = allocator.pageSize();
    _allocator = allocator;
    _pages = new ArrayList<>(8);
    _currentPage = _allocator.allocate().order(ByteOrder.BIG_ENDIAN);
    _currentPageStartOffset = 0;
    _pages.add(_currentPage);
  }

  public static PagedPinotOutputStream createHeap() {
    return new PagedPinotOutputStream(HeapPageAllocator.createSmall());
  }

  private void nextPage() {
    moveCurrentOffset(remainingInPage());
  }

  private int remainingInPage() {
    return _pageSize - _offsetInPage;
  }

  /**
   * Returns a read only view of the pages written so far.
   * <p>
   * All pages but the last one will have its position set to 0 and the limit to their capacity.
   * The latest page will have its position set to 0 and its limit set to the last byte written.
   *
   * TODO: Add one option that let caller choose start and end offset.
   */
  public ByteBuffer[] getPages() {
    int numPages = _pages.size();

    long lastPageStart = (numPages - 1) * (long) _pageSize;
    assert lastPageStart >= 0 : "lastPageStart=" + lastPageStart;
    assert lastPageStart <= _written : "lastPageStart=" + lastPageStart + ", _written=" + _written;
    boolean lastPageIsEmpty = _written == lastPageStart;
    if (lastPageIsEmpty) {
      numPages--;
    }
    if (numPages == 0) {
      return new ByteBuffer[0];
    }
    ByteBuffer[] result = new ByteBuffer[numPages];

    for (int i = 0; i < numPages; i++) {
      ByteBuffer byteBuffer = _pages.get(i);
      ByteBuffer page = byteBuffer.asReadOnlyBuffer();
      page.clear();
      result[i] = page;
    }

    if (!lastPageIsEmpty) {
      result[numPages - 1].limit((int) (_written - lastPageStart));
    }

    return result;
  }

  @Override
  public long getCurrentOffset() {
    return _currentPageStartOffset + _offsetInPage;
  }

  @Override
  public void seek(long newPos) {
    if (newPos < 0) {
      throw new IllegalArgumentException("New position cannot be negative");
    }
    if (newPos == 0) {
      _currentPage = _pages.get(0);
      _offsetInPage = 0;
    } else {
      int pageIdx = (int) (newPos / _pageSize);
      if (pageIdx >= _pages.size()) {
        _pages.ensureCapacity(pageIdx + 1);
        while (_pages.size() <= pageIdx) {
          _pages.add(_allocator.allocate().order(ByteOrder.BIG_ENDIAN));
        }
      }
      int offsetInPage = (int) (newPos % _pageSize);
      _currentPage = _pages.get(pageIdx);
      _currentPageStartOffset = pageIdx * (long) _pageSize;
      _offsetInPage = offsetInPage;
    }
    _written = Math.max(_written, newPos);
  }

  @Override
  public void write(int b)
      throws IOException {
    if (remainingInPage() == 0) {
      nextPage();
    }
    _currentPage.put(_offsetInPage++, (byte) b);
    _written = Math.max(_written, _offsetInPage + _currentPageStartOffset);
  }

  @Override
  public void writeInt(int v)
      throws IOException {
    if (remainingInPage() >= Integer.BYTES) {
      _currentPage.putInt(_offsetInPage, v);
      _offsetInPage += Integer.BYTES;
      _written = Math.max(_written, _offsetInPage + _currentPageStartOffset);
    } else {
      super.writeInt(v);
    }
  }

  @Override
  public void writeLong(long v)
      throws IOException {
    if (remainingInPage() >= Long.BYTES) {
      _currentPage.putLong(_offsetInPage, v);
      _offsetInPage += Long.BYTES;
      _written = Math.max(_written, _offsetInPage + _currentPageStartOffset);
    } else {
      super.writeLong(v);
    }
  }

  @Override
  public void writeShort(int v)
      throws IOException {
    if (remainingInPage() >= Short.BYTES) {
      _currentPage.putShort(_offsetInPage, (short) v);
      _offsetInPage += Short.BYTES;
      _written = Math.max(_written, _offsetInPage + _currentPageStartOffset);
    } else {
      super.writeShort(v);
    }
  }

  @Override
  public void write(byte[] b, int off, int len)
      throws IOException {
    if (remainingInPage() >= len) {
      _currentPage.position(_offsetInPage);
      _currentPage.put(b, off, len);
      _offsetInPage += len;
      _currentPage.position(0);
    } else {
      int written = 0;
      while (written < len) {
        int remainingInPage = remainingInPage();
        if (remainingInPage == 0) {
          nextPage();
          continue;
        }
        int toWrite = Math.min(len - written, remainingInPage);
        _currentPage.position(_offsetInPage);
        _currentPage.put(b, off + written, toWrite);
        _currentPage.position(0);
        written += toWrite;
        _offsetInPage += toWrite;
      }
    }
    _written = Math.max(_written, _offsetInPage + _currentPageStartOffset);
  }

  @Override
  public void write(DataBuffer input, long offset, long length)
      throws IOException {
    if (remainingInPage() >= length) {
      int intLength = (int) length;
      input.copyTo(offset, _currentPage, _offsetInPage, intLength);
      _offsetInPage += intLength;
    } else {
      long written = 0;
      while (written < length) {
        int remainingInPage = remainingInPage();
        if (remainingInPage == 0) {
          nextPage();
          continue;
        }
        int toWrite = (int) Math.min(length - written, remainingInPage);
        input.copyTo(offset + written, _currentPage, _offsetInPage, toWrite);
        written += toWrite;
        _offsetInPage += toWrite;
      }
    }
    _written = Math.max(_written, _offsetInPage + _currentPageStartOffset);
  }

  /**
   * Returns a view of the data written so far as a {@link DataBuffer}.
   * <p>
   * The returned DataBuffer will contain all the data being written. This is specially important when
   * {@link #getCurrentOffset()} has been moved back from the latest written position.
   *
   * TODO: Add one option that let caller choose start and end offset.
   */
  public DataBuffer asBuffer(ByteOrder order, boolean owner) {
    if (_written == 0) {
      return PinotDataBuffer.empty();
    }

    // TODO: We can remove this check
    ByteBuffer[] pages = getPages();
    for (int i = 0; i < pages.length; i++) {
      ByteBuffer page = pages[i];
      if (page.remaining() != _pageSize && (i != pages.length - 1 || !page.hasRemaining())) {
        throw new IllegalArgumentException("Unexpected remaining bytes in page " + i + ": " + page.remaining());
      }
    }

    return new CompoundDataBuffer(pages, order, owner);
  }

  public int getPageSize() {
    return _pageSize;
  }

  @Override
  public void close()
      throws IOException {
    IOException ex = null;
    for (ByteBuffer page : _pages) {
      try {
        _allocator.release(page);
      } catch (IOException e) {
        if (ex == null) {
          ex = e;
        } else {
          ex.addSuppressed(e);
        }
      }
    }
    if (ex != null) {
      throw ex;
    }
  }

  public static abstract class PageAllocator {
    public static final int MIN_RECOMMENDED_PAGE_SIZE = 16 * 1024;
    public static final int MAX_RECOMMENDED_PAGE_SIZE = 1024 * 1024;

    public abstract int pageSize();

    public abstract ByteBuffer allocate();

    public abstract void release(ByteBuffer buffer)
        throws IOException;
  }

  public static class HeapPageAllocator extends PageAllocator {

    private final int _pageSize;

    public static HeapPageAllocator createSmall() {
      return new HeapPageAllocator(MIN_RECOMMENDED_PAGE_SIZE);
    }

    public static HeapPageAllocator createLarge() {
      return new HeapPageAllocator(MAX_RECOMMENDED_PAGE_SIZE);
    }

    public HeapPageAllocator(int pageSize) {
      Preconditions.checkArgument(pageSize > 0, "Page size must be positive");
      _pageSize = pageSize;
    }

    @Override
    public int pageSize() {
      return _pageSize;
    }

    @Override
    public ByteBuffer allocate() {
      return ByteBuffer.allocate(_pageSize);
    }

    @Override
    public void release(ByteBuffer buffer) {
      // Do nothing
    }
  }

  public static class DirectPageAllocator extends PageAllocator {

    private final int _pageSize;
    private final boolean _release;

    public DirectPageAllocator(int pageSize) {
      this(pageSize, false);
    }

    public DirectPageAllocator(int pageSize, boolean release) {
      Preconditions.checkArgument(pageSize > 0, "Page size must be positive");
      _pageSize = pageSize;
      _release = release;
    }

    public static DirectPageAllocator createSmall(boolean release) {
      return new DirectPageAllocator(MIN_RECOMMENDED_PAGE_SIZE, release);
    }

    public static DirectPageAllocator createLarge(boolean release) {
      return new DirectPageAllocator(MAX_RECOMMENDED_PAGE_SIZE, release);
    }

    @Override
    public int pageSize() {
      return _pageSize;
    }

    @Override
    public ByteBuffer allocate() {
      return ByteBuffer.allocateDirect(_pageSize);
    }

    @Override
    public void release(ByteBuffer buffer)
        throws IOException {
      if (_release && CleanerUtil.UNMAP_SUPPORTED) {
        CleanerUtil.getCleaner().freeBuffer(buffer);
      }
    }
  }
}
