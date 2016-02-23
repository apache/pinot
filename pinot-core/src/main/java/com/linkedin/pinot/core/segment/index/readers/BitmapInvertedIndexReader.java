/**
 * Copyright (C) 2014-2015 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.core.segment.index.readers;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.ref.SoftReference;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel.MapMode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;

import com.linkedin.pinot.common.utils.MmapUtils;
import com.linkedin.pinot.common.utils.Pairs.IntPair;

/**
 * Aug 10, 2014
 */

public class BitmapInvertedIndexReader implements InvertedIndexReader {
  public static final Logger LOGGER = LoggerFactory.getLogger(BitmapInvertedIndexReader.class);

  final private int numberOfBitmaps;
  private volatile SoftReference<SoftReference<ImmutableRoaringBitmap>[]> bitmaps = null;

  private RandomAccessFile _rndFile;
  private ByteBuffer buffer;
  public static final int INT_SIZE_IN_BYTES = Integer.SIZE / Byte.SIZE;

  private File file;

  /**
   * Constructs an inverted index with the specified size.
   * @param cardinality the number of bitmaps in the inverted index, which should be the same as the
   *          number of values in
   *          the dictionary.
   * @throws IOException
   */
  public BitmapInvertedIndexReader(File file, int cardinality, boolean isMmap) throws IOException {
    this.file = file;
    numberOfBitmaps = cardinality;
    load(file, isMmap);
  }

  /**
   * {@inheritDoc}
   * @see InvertedIndexReader#getImmutable(int)
   */
  @Override
  public ImmutableRoaringBitmap getImmutable(int idx) {
    SoftReference<ImmutableRoaringBitmap>[] bitmapArrayReference = null;
    // Return the bitmap if it's still on heap
    if (bitmaps != null) {
      bitmapArrayReference = bitmaps.get();
      if (bitmapArrayReference != null) {
        SoftReference<ImmutableRoaringBitmap> bitmapReference = bitmapArrayReference[idx];
        if (bitmapReference != null) {
          ImmutableRoaringBitmap value = bitmapReference.get();
          if (value != null) {
            return value;
          }
        }
      } else {
        bitmapArrayReference = new SoftReference[numberOfBitmaps];
        bitmaps = new SoftReference<SoftReference<ImmutableRoaringBitmap>[]>(bitmapArrayReference);
      }
    } else {
      bitmapArrayReference = new SoftReference[numberOfBitmaps];
      bitmaps = new SoftReference<SoftReference<ImmutableRoaringBitmap>[]>(bitmapArrayReference);
    }
    synchronized (this) {
      ImmutableRoaringBitmap value;
      if (bitmapArrayReference[idx] == null || bitmapArrayReference[idx].get() == null) {
        value = buildRoaringBitmapForIndex(idx);
        bitmapArrayReference[idx] = new SoftReference<ImmutableRoaringBitmap>(value);
      } else {
        value = bitmapArrayReference[idx].get();
      }
      return value;
    }

  }

  private synchronized ImmutableRoaringBitmap buildRoaringBitmapForIndex(final int index) {
    final int currentOffset = getOffset(index);
    final int nextOffset = getOffset(index + 1);
    final int bufferLength = nextOffset - currentOffset;

    // Slice the buffer appropriately for Roaring Bitmap
    buffer.position(currentOffset);
    final ByteBuffer bb = buffer.slice();
    bb.limit(bufferLength);

    ImmutableRoaringBitmap immutableRoaringBitmap = null;
    try {
      immutableRoaringBitmap = new ImmutableRoaringBitmap(bb);
    } catch (Exception e) {
      LOGGER.error(
          "Error creating immutableRoaringBitmap for dictionary id:{} currentOffset:{} bufferLength:{} slice position{} limit:{} file:{}",
          index, currentOffset, bufferLength, bb.position(), bb.limit(), file.getAbsolutePath());
    }
    return immutableRoaringBitmap;
  }

  private int getOffset(final int index) {
    return buffer.getInt(index * INT_SIZE_IN_BYTES);
  }

  private void load(File file, boolean isMmap) throws IOException {
    final DataInputStream dis =
        new DataInputStream(new BufferedInputStream(new FileInputStream(file)));

    dis.skipBytes(numberOfBitmaps * INT_SIZE_IN_BYTES);
    final int lastOffset = dis.readInt();
    dis.close();

    _rndFile = new RandomAccessFile(file, "r");
    if (isMmap) {
      buffer = MmapUtils.mmapFile(_rndFile, MapMode.READ_ONLY, 0, lastOffset, file,
          this.getClass().getSimpleName() + " buffer");
    } else {
      buffer = MmapUtils.allocateDirectByteBuffer(lastOffset, file,
          this.getClass().getSimpleName() + " buffer");
      _rndFile.getChannel().read(buffer);
    }
  }

  @Override
  public void close() throws IOException {
    MmapUtils.unloadByteBuffer(buffer);
    buffer = null;
    if (_rndFile != null) {
      _rndFile.close();
    }
  }

  @Override
  public IntPair getMinMaxRangeFor(int dicId) {
    throw new UnsupportedOperationException("not supported in inverted index type bitmap");
  }
}
