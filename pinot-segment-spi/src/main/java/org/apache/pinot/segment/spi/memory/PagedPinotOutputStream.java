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
import javax.annotation.concurrent.NotThreadSafe;


/// A [PinotOutputStream] that writes to a sequence of pages of the same size, taken from a [PageAllocator]. When the
/// current page is full, writing continues on a new page.
///
/// The stream suits data whose final size is not known in advance, and writers that [`seek`][#seek(long)] to an
/// earlier or later position. A value that crosses a page boundary is written one byte at a time, which is slower than
/// a write within a page.
///
/// Smaller pages mean more allocations, but each one is cheaper, especially on the heap, where an allocation that fits
/// in a thread-local allocation buffer is fastest. Some allocators free the pages when the stream is closed.
///
/// [#getPages()] returns the written data as read-only [ByteBuffer] pages, which a [CompoundDataBuffer] reads directly
/// and `UnsafeByteOperations.unsafeWrap(ByteBuffer)` hands to gRPC without a copy. [#asBuffer] returns the same data
/// as one [DataBuffer].
@NotThreadSafe
public class PagedPinotOutputStream extends PinotOutputStream {
  private final PageAllocator _allocator;
  private final int _pageSize;
  private final ArrayList<ByteBuffer> _pages;
  private ByteBuffer _currentPage;
  private long _currentPageStartOffset;
  private int _offsetInPage;
  private long _written = 0;

  /// Creates a stream that takes its pages from `allocator`, starting with one page.
  public PagedPinotOutputStream(PageAllocator allocator) {
    _pageSize = allocator.pageSize();
    _allocator = allocator;
    _pages = new ArrayList<>(8);
    _currentPage = _allocator.allocate().order(ByteOrder.BIG_ENDIAN);
    _currentPageStartOffset = 0;
    _pages.add(_currentPage);
  }

  /// Creates a stream over heap pages of [PageAllocator#MIN_RECOMMENDED_PAGE_SIZE] bytes.
  public static PagedPinotOutputStream createHeap() {
    return new PagedPinotOutputStream(HeapPageAllocator.createSmall());
  }

  private void nextPage() {
    seek(getCurrentOffset() + remainingInPage());
  }

  private int remainingInPage() {
    return _pageSize - _offsetInPage;
  }

  /// Moves the position `numBytes` forward within the current page, and the end of the written data with it.
  private void advance(int numBytes) {
    _offsetInPage += numBytes;
    _written = Math.max(_written, _offsetInPage + _currentPageStartOffset);
  }

  /// Returns read-only views of the pages that hold the written data, each positioned at 0. Every page but the last is
  /// limited to its capacity, and the last one to the end of the written data: the furthest position written or sought
  /// to, even after a seek back. A trailing page that holds no written data is left out.
  ///
  /// TODO: Add an option that lets the caller choose the start and end offsets.
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
    int pageIdx = (int) (newPos / _pageSize);
    if (pageIdx >= _pages.size()) {
      _pages.ensureCapacity(pageIdx + 1);
      while (_pages.size() <= pageIdx) {
        _pages.add(_allocator.allocate().order(ByteOrder.BIG_ENDIAN));
      }
    }
    _currentPage = _pages.get(pageIdx);
    _currentPageStartOffset = pageIdx * (long) _pageSize;
    _offsetInPage = (int) (newPos % _pageSize);
    _written = Math.max(_written, newPos);
  }

  @Override
  public void write(int b)
      throws IOException {
    if (remainingInPage() == 0) {
      nextPage();
    }
    _currentPage.put(_offsetInPage, (byte) b);
    advance(1);
  }

  @Override
  public void writeInt(int v)
      throws IOException {
    if (remainingInPage() >= Integer.BYTES) {
      _currentPage.putInt(_offsetInPage, v);
      advance(Integer.BYTES);
    } else {
      writeAcrossPages(v, Integer.BYTES);
    }
  }

  @Override
  public void writeLong(long v)
      throws IOException {
    if (remainingInPage() >= Long.BYTES) {
      _currentPage.putLong(_offsetInPage, v);
      advance(Long.BYTES);
    } else {
      writeAcrossPages(v, Long.BYTES);
    }
  }

  @Override
  public void writeShort(int v)
      throws IOException {
    if (remainingInPage() >= Short.BYTES) {
      _currentPage.putShort(_offsetInPage, (short) v);
      advance(Short.BYTES);
    } else {
      writeAcrossPages(v, Short.BYTES);
    }
  }

  /// Writes the low `numBytes` bytes of `v`, highest first, one byte at a time, for a value that does not fit in the
  /// rest of the page.
  private void writeAcrossPages(long v, int numBytes)
      throws IOException {
    for (int shift = (numBytes - 1) * Byte.SIZE; shift >= 0; shift -= Byte.SIZE) {
      write((int) (v >> shift));
    }
  }

  @Override
  public void write(byte[] b, int off, int len)
      throws IOException {
    if (remainingInPage() >= len) {
      _currentPage.position(_offsetInPage);
      _currentPage.put(b, off, len);
      advance(len);
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
        advance(toWrite);
      }
    }
  }

  /// Puts as many values as fit in the rest of the current page straight into it, then continues on the next page.
  /// When at least [PrimitiveArrays#MIN_BULK_VALUES] values go to one page, they are copied through a big-endian typed
  /// view of it; fewer values are put one at a time, since the copy through a view has a fixed cost. The position
  /// depends on everything written before, so a page boundary can fall inside a value: such a value is written with
  /// [#writeInt(int)], which splits it across the two pages.
  @Override
  public void writeInts(int[] values, int offset, int length)
      throws IOException {
    int end = offset + length;
    int i = offset;
    while (i < end) {
      int numValues = Math.min(end - i, remainingInPage() / Integer.BYTES);
      if (numValues == 0) {
        writeInt(values[i++]);
        continue;
      }
      if (numValues < PrimitiveArrays.MIN_BULK_VALUES) {
        int position = _offsetInPage;
        for (int j = i; j < i + numValues; j++, position += Integer.BYTES) {
          _currentPage.putInt(position, values[j]);
        }
      } else {
        currentPageView().asIntBuffer().put(values, i, numValues);
      }
      i += numValues;
      advance(numValues * Integer.BYTES);
    }
  }

  /// Same as [#writeInts(int[], int, int)], for longs, with [#writeLong(long)] for a value split across two pages.
  @Override
  public void writeLongs(long[] values, int offset, int length)
      throws IOException {
    int end = offset + length;
    int i = offset;
    while (i < end) {
      int numValues = Math.min(end - i, remainingInPage() / Long.BYTES);
      if (numValues == 0) {
        writeLong(values[i++]);
        continue;
      }
      if (numValues < PrimitiveArrays.MIN_BULK_VALUES) {
        int position = _offsetInPage;
        for (int j = i; j < i + numValues; j++, position += Long.BYTES) {
          _currentPage.putLong(position, values[j]);
        }
      } else {
        currentPageView().asLongBuffer().put(values, i, numValues);
      }
      i += numValues;
      advance(numValues * Long.BYTES);
    }
  }

  /// Same as [#writeInts(int[], int, int)], for floats written as raw bits.
  @Override
  public void writeFloats(float[] values, int offset, int length)
      throws IOException {
    int end = offset + length;
    int i = offset;
    while (i < end) {
      int numValues = Math.min(end - i, remainingInPage() / Float.BYTES);
      if (numValues == 0) {
        // Raw bits, like the typed view (and ByteBuffer#putFloat): writeFloat() would canonicalize NaN.
        writeInt(Float.floatToRawIntBits(values[i++]));
        continue;
      }
      if (numValues < PrimitiveArrays.MIN_BULK_VALUES) {
        int position = _offsetInPage;
        for (int j = i; j < i + numValues; j++, position += Float.BYTES) {
          _currentPage.putInt(position, Float.floatToRawIntBits(values[j]));
        }
      } else {
        currentPageView().asFloatBuffer().put(values, i, numValues);
      }
      i += numValues;
      advance(numValues * Float.BYTES);
    }
  }

  /// Same as [#writeInts(int[], int, int)], for doubles written as raw bits.
  @Override
  public void writeDoubles(double[] values, int offset, int length)
      throws IOException {
    int end = offset + length;
    int i = offset;
    while (i < end) {
      int numValues = Math.min(end - i, remainingInPage() / Double.BYTES);
      if (numValues == 0) {
        // Raw bits, like the typed view (and ByteBuffer#putDouble): writeDouble() would canonicalize NaN.
        writeLong(Double.doubleToRawLongBits(values[i++]));
        continue;
      }
      if (numValues < PrimitiveArrays.MIN_BULK_VALUES) {
        int position = _offsetInPage;
        for (int j = i; j < i + numValues; j++, position += Double.BYTES) {
          _currentPage.putLong(position, Double.doubleToRawLongBits(values[j]));
        }
      } else {
        currentPageView().asDoubleBuffer().put(values, i, numValues);
      }
      i += numValues;
      advance(numValues * Double.BYTES);
    }
  }

  /// Returns a big-endian view of the current page that starts at the current offset. The page itself is not changed.
  private ByteBuffer currentPageView() {
    ByteBuffer view = _currentPage.duplicate();
    view.position(_offsetInPage);
    return view.order(ByteOrder.BIG_ENDIAN);
  }

  @Override
  public void write(DataBuffer input, long offset, long length)
      throws IOException {
    if (remainingInPage() >= length) {
      int intLength = (int) length;
      input.copyTo(offset, _currentPage, _offsetInPage, intLength);
      advance(intLength);
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
        advance(toWrite);
      }
    }
  }

  /// Returns the written data as one [DataBuffer] over the pages of [#getPages()], read in `order`. It reaches the end
  /// of the written data even when the position has been moved back since. The pages stay owned by this stream, so
  /// closing the buffer releases none of them, whatever `owner` is.
  ///
  /// TODO: Add an option that lets the caller choose the start and end offsets.
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

  /// Returns the size of each page in bytes.
  public int getPageSize() {
    return _pageSize;
  }

  /// Returns every page to the allocator. The stream must not be used afterwards. The views from [#getPages()] and
  /// [#asBuffer(ByteOrder, boolean)] stay readable only while the allocator keeps the pages, as [HeapPageAllocator]
  /// does; a [DirectPageAllocator] created with `release` set frees them.
  @Override
  public void close()
      throws IOException {
    IOException ex = null;
    for (ByteBuffer page : _pages) {
      try {
        _allocator.release(page);
      } catch (IOException e) {
        // This module doesn't import pinot-common, so we cannot use ExceptionUtils.suppress here.
        if (ex == null) {
          ex = e;
        } else if (e != ex) {
          ex.addSuppressed(e);
        }
      }
    }
    if (ex != null) {
      throw ex;
    }
  }

  /// Allocates the pages of a [PagedPinotOutputStream], all of the same size, and releases them when the stream is
  /// closed.
  public static abstract class PageAllocator {
    /// Page size of the `createSmall` allocators.
    public static final int MIN_RECOMMENDED_PAGE_SIZE = 16 * 1024;
    /// Page size of the `createLarge` allocators.
    public static final int MAX_RECOMMENDED_PAGE_SIZE = 1024 * 1024;

    /// Returns the size of each page in bytes.
    public abstract int pageSize();

    /// Returns a new page of [#pageSize()] bytes.
    public abstract ByteBuffer allocate();

    /// Releases a page the stream no longer uses.
    public abstract void release(ByteBuffer buffer)
        throws IOException;
  }

  /// Allocates heap pages, which the garbage collector reclaims.
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

  /// Allocates direct pages. When created with `release` set, it frees each page as soon as the stream is closed, where
  /// the JVM supports it; otherwise the garbage collector reclaims the pages.
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
