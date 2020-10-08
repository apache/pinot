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

import com.google.common.base.Preconditions;
import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.core.segment.creator.DictionaryBasedInvertedIndexCreator;
import org.apache.pinot.core.segment.creator.impl.V1Constants;
import org.roaringbitmap.buffer.MutableRoaringBitmap;


/**
 * Implementation of {@link DictionaryBasedInvertedIndexCreator} that uses on-heap memory.
 */
public final class OnHeapBitmapInvertedIndexCreator implements DictionaryBasedInvertedIndexCreator {
  private final File _invertedIndexFile;
  private final MutableRoaringBitmap[] _bitmaps;
  private int _nextDocId;

  public OnHeapBitmapInvertedIndexCreator(File indexDir, String columnName, int cardinality) {
    _invertedIndexFile = new File(indexDir, columnName + V1Constants.Indexes.BITMAP_INVERTED_INDEX_FILE_EXTENSION);
    _bitmaps = new MutableRoaringBitmap[cardinality];
    for (int i = 0; i < cardinality; i++) {
      _bitmaps[i] = new MutableRoaringBitmap();
    }
  }

  @Override
  public void add(int dictId) {
    _bitmaps[dictId].add(_nextDocId++);
  }

  @Override
  public void add(int[] dictIds, int length) {
    for (int i = 0; i < length; i++) {
      _bitmaps[dictIds[i]].add(_nextDocId);
    }
    _nextDocId++;
  }

  @Override
  public void seal()
      throws IOException {
    try (DataOutputStream out = new DataOutputStream(
        new BufferedOutputStream(new FileOutputStream(_invertedIndexFile)))) {
      // Write bitmap offsets
      int bitmapOffset = (_bitmaps.length + 1) * Integer.BYTES;
      out.writeInt(bitmapOffset);
      for (MutableRoaringBitmap bitmap : _bitmaps) {
        bitmapOffset += bitmap.serializedSizeInBytes();
        // Check for int overflow
        Preconditions.checkState(bitmapOffset > 0, "Inverted index file: %s exceeds 2GB limit", _invertedIndexFile);
        out.writeInt(bitmapOffset);
      }

      // Write bitmap data
      for (MutableRoaringBitmap bitmap : _bitmaps) {
        bitmap.serialize(out);
      }
    } catch (Exception e) {
      FileUtils.deleteQuietly(_invertedIndexFile);
      throw e;
    }
  }

  @Override
  public void close() {
  }
}
