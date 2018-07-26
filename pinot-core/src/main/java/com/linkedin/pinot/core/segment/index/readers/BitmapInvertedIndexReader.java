/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
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

import com.linkedin.pinot.core.segment.memory.PinotDataBuffer;
import java.io.File;
import java.io.IOException;
import java.lang.ref.SoftReference;
import java.nio.ByteBuffer;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class BitmapInvertedIndexReader implements InvertedIndexReader<ImmutableRoaringBitmap> {
  public static final Logger LOGGER = LoggerFactory.getLogger(BitmapInvertedIndexReader.class);

  final private int numberOfBitmaps;
  private volatile SoftReference<SoftReference<ImmutableRoaringBitmap>[]> bitmaps = null;

  private PinotDataBuffer buffer;

  private File file;

  /**
   * Constructs an inverted index with the specified size.
   * @param cardinality the number of bitmaps in the inverted index, which should be the same as the
   *          number of values in
   *          the dictionary.
   * @throws IOException
   */
  public BitmapInvertedIndexReader(PinotDataBuffer indexDataBuffer, int cardinality) throws IOException {
    this.file = file;
    numberOfBitmaps = cardinality;
    load(indexDataBuffer);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public ImmutableRoaringBitmap getDocIds(int dictId) {
    SoftReference<ImmutableRoaringBitmap>[] bitmapArrayReference = null;
    // Return the bitmap if it's still on heap
    if (bitmaps != null) {
      bitmapArrayReference = bitmaps.get();
      if (bitmapArrayReference != null) {
        SoftReference<ImmutableRoaringBitmap> bitmapReference = bitmapArrayReference[dictId];
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
      if (bitmapArrayReference[dictId] == null || bitmapArrayReference[dictId].get() == null) {
        value = buildRoaringBitmapForIndex(dictId);
        bitmapArrayReference[dictId] = new SoftReference<ImmutableRoaringBitmap>(value);
      } else {
        value = bitmapArrayReference[dictId].get();
      }
      return value;
    }
  }

  private synchronized ImmutableRoaringBitmap buildRoaringBitmapForIndex(final int index) {
    final int currentOffset = getOffset(index);
    final int nextOffset = getOffset(index + 1);
    final int bufferLength = nextOffset - currentOffset;

    // Slice the buffer appropriately for Roaring Bitmap
    ByteBuffer bb = buffer.toDirectByteBuffer(currentOffset, bufferLength);
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
    return buffer.getInt(index * Integer.BYTES);
  }

  private void load(PinotDataBuffer indexDataBuffer) throws IOException {
    final int lastOffset = indexDataBuffer.getInt(numberOfBitmaps * Integer.BYTES);
    assert lastOffset == indexDataBuffer.size();
    this.buffer = indexDataBuffer;
  }

  @Override
  public void close() throws IOException {
    buffer.close();
  }
}
