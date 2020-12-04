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
package org.apache.pinot.core.segment.creator.impl.inv;


import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;

import org.apache.commons.io.FileUtils;
import org.apache.pinot.core.segment.creator.DictionaryBasedInvertedIndexCreator;
import org.apache.pinot.core.segment.creator.impl.V1Constants;
import org.apache.pinot.core.util.CleanerUtil;
import org.roaringbitmap.RoaringBitmap;
import org.roaringbitmap.RoaringBitmapWriter;


/**
 * Implementation of {@link DictionaryBasedInvertedIndexCreator} that uses on-heap memory.
 */
public final class OnHeapBitmapInvertedIndexCreator implements DictionaryBasedInvertedIndexCreator {
  private final File _invertedIndexFile;
  private final RoaringBitmapWriter<RoaringBitmap>[] _bitmapWriters;
  private int _nextDocId;

  @SuppressWarnings("unchecked")
  public OnHeapBitmapInvertedIndexCreator(File indexDir, String columnName, int cardinality) {
    _invertedIndexFile = new File(indexDir, columnName + V1Constants.Indexes.BITMAP_INVERTED_INDEX_FILE_EXTENSION);
    _bitmapWriters = new RoaringBitmapWriter[cardinality];
    for (int i = 0; i < cardinality; i++) {
      _bitmapWriters[i] = RoaringBitmapWriter.writer().runCompress(false).get();
    }
  }

  @Override
  public void add(int dictId) {
    _bitmapWriters[dictId].add(_nextDocId++);
  }

  @Override
  public void add(int[] dictIds, int length) {
    for (int i = 0; i < length; i++) {
      _bitmapWriters[dictIds[i]].add(_nextDocId);
    }
    _nextDocId++;
  }

  @Override
  public void seal()
      throws IOException {
    int startOfBitmaps = (_bitmapWriters.length + 1) * Integer.BYTES;
    ByteBuffer bitmapBuffer = null;
    ByteBuffer offsetBuffer = null;
    try (FileChannel channel = new RandomAccessFile(_invertedIndexFile, "rw").getChannel()) {
      // map the offsets buffer
      offsetBuffer = channel.map(FileChannel.MapMode.READ_WRITE, 0, startOfBitmaps)
              .order(ByteOrder.BIG_ENDIAN);
      bitmapBuffer = channel.map(FileChannel.MapMode.READ_WRITE, startOfBitmaps, Integer.MAX_VALUE - startOfBitmaps)
              .order(ByteOrder.LITTLE_ENDIAN);
      // Write bitmap offsets
      offsetBuffer.putInt(startOfBitmaps);
      for (RoaringBitmapWriter<RoaringBitmap> writer : _bitmapWriters) {
        writer.get().serialize(bitmapBuffer);
        offsetBuffer.putInt(startOfBitmaps + bitmapBuffer.position());
      }
      channel.truncate(startOfBitmaps + bitmapBuffer.position());
    } catch (Exception e) {
      FileUtils.deleteQuietly(_invertedIndexFile);
      throw e;
    } finally {
      if (CleanerUtil.UNMAP_SUPPORTED) {
        CleanerUtil.BufferCleaner cleaner = CleanerUtil.getCleaner();
        if (bitmapBuffer != null) {
          cleaner.freeBuffer(bitmapBuffer);
        }
        if (offsetBuffer != null) {
          cleaner.freeBuffer(offsetBuffer);
        }
      }
    }
  }

  @Override
  public void close() {
  }
}
