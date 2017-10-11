/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
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
package com.linkedin.pinot.core.segment.creator.impl.inv;

import com.google.common.base.Preconditions;
import com.linkedin.pinot.core.segment.creator.InvertedIndexCreator;
import com.linkedin.pinot.core.segment.creator.impl.V1Constants;
import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import org.apache.commons.io.FileUtils;
import org.roaringbitmap.buffer.MutableRoaringBitmap;


/**
 * Implementation of {@link InvertedIndexCreator} that uses on-heap memory.
 */
public final class OnHeapBitmapInvertedIndexCreator implements InvertedIndexCreator {
  private final String _columnName;
  private final File _invertedIndexFile;
  private final MutableRoaringBitmap[] _bitmaps;

  public OnHeapBitmapInvertedIndexCreator(File indexDir, String columnName, int cardinality) {
    _columnName = columnName;
    _invertedIndexFile = new File(indexDir, columnName + V1Constants.Indexes.BITMAP_INVERTED_INDEX_FILE_EXTENSION);
    _bitmaps = new MutableRoaringBitmap[cardinality];
    for (int i = 0; i < cardinality; i++) {
      _bitmaps[i] = new MutableRoaringBitmap();
    }
  }

  @Override
  public void addSV(int docId, int dictId) {
    _bitmaps[dictId].add(docId);
  }

  @Override
  public void addMV(int docId, int[] dictIds) {
    addMV(docId, dictIds, dictIds.length);
  }

  @Override
  public void addMV(int docId, int[] dictIds, int numDictIds) {
    for (int i = 0; i < numDictIds; i++) {
      _bitmaps[dictIds[i]].add(docId);
    }
  }

  @Override
  public void seal() throws IOException {
    try (DataOutputStream out = new DataOutputStream(
        new BufferedOutputStream(new FileOutputStream(_invertedIndexFile)))) {
      // Write all offsets
      int offset = (_bitmaps.length + 1) * V1Constants.Numbers.INTEGER_SIZE;
      out.writeInt(offset);
      for (MutableRoaringBitmap bitmap : _bitmaps) {
        offset += bitmap.serializedSizeInBytes();
        // Check for int overflow
        Preconditions.checkState(offset > 0, "Inverted index file exceeds 2GB limit for column: %s", _columnName);
        out.writeInt(offset);
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
