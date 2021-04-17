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
package org.apache.pinot.segment.local.segment.creator.impl.inv;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import org.apache.pinot.segment.local.utils.CleanerUtil;
import org.roaringbitmap.RoaringBitmap;


/**
 * Writer for bitmap inverted index file.
 * <pre>
 * Layout for RoaringBitmap inverted index:
 * |-------------------------------------------------------------------------|
 * |                    Start offset of 1st bitmap                           |
 * |    End offset of 1st bitmap (exclusive) / Start offset of 2nd bitmap    |
 * |                                   ...                                   |
 * | End offset of 2nd last bitmap (exclusive) / Start offset of last bitmap |
 * |                  End offset of last bitmap (exclusive)                  |
 * |-------------------------------------------------------------------------|
 * |                           Data for 1st bitmap                           |
 * |                           Data for 2nd bitmap                           |
 * |                                   ...                                   |
 * |                           Data for last bitmap                          |
 * |-------------------------------------------------------------------------|
 * </pre>
 */
public final class BitmapInvertedIndexWriter implements Closeable {
  private final FileChannel _fileChannel;
  private final ByteBuffer _offsetBuffer;
  private final ByteBuffer _bitmapBuffer;

  public BitmapInvertedIndexWriter(File outputFile, int numBitmaps) throws IOException {
    _fileChannel = new RandomAccessFile(outputFile, "rw").getChannel();
    _offsetBuffer = _fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, Integer.MAX_VALUE);
    _bitmapBuffer = _offsetBuffer.duplicate().order(ByteOrder.LITTLE_ENDIAN);
    _bitmapBuffer.position((numBitmaps + 1) * Integer.BYTES);
  }

  public void add(RoaringBitmap bitmap) throws IOException {
    _offsetBuffer.putInt(_bitmapBuffer.position());
    bitmap.serialize(_bitmapBuffer);
  }

  public void add(byte[] bitmapBytes) {
    add(bitmapBytes, bitmapBytes.length);
  }

  public void add(byte[] bitmapBytes, int length) {
    _offsetBuffer.putInt(_bitmapBuffer.position());
    _bitmapBuffer.put(bitmapBytes, 0, length);
  }

  @Override
  public void close() throws IOException {
    int fileLength = _bitmapBuffer.position();
    _offsetBuffer.putInt(fileLength);
    _fileChannel.truncate(fileLength);
    _fileChannel.close();
    if (CleanerUtil.UNMAP_SUPPORTED) {
      CleanerUtil.BufferCleaner cleaner = CleanerUtil.getCleaner();
      cleaner.freeBuffer(_offsetBuffer);
    }
  }
}
