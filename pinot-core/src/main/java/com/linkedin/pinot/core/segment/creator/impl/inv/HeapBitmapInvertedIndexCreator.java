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
package com.linkedin.pinot.core.segment.creator.impl.inv;

import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Arrays;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.roaringbitmap.buffer.MutableRoaringBitmap;

import com.linkedin.pinot.common.data.FieldSpec;
import com.linkedin.pinot.core.segment.creator.InvertedIndexCreator;
import com.linkedin.pinot.core.segment.creator.impl.V1Constants;


/**
 * Nov 12, 2014
 */
public class HeapBitmapInvertedIndexCreator implements InvertedIndexCreator {
  private static final Logger LOGGER = LoggerFactory.getLogger(HeapBitmapInvertedIndexCreator.class);

  private final File invertedIndexFile;
  private final FieldSpec spec;
  private final MutableRoaringBitmap[] invertedIndex;
  long start = 0;

  public HeapBitmapInvertedIndexCreator(File indexDir, int cardinality, int numDocs, int totalNumberOfEntries, FieldSpec spec) {
    this.spec = spec;
    invertedIndexFile = new File(indexDir, spec.getName() + V1Constants.Indexes.BITMAP_INVERTED_INDEX_FILE_EXTENSION);
    invertedIndex = new MutableRoaringBitmap[cardinality];
    for (int i = 0; i < invertedIndex.length; ++i) {
      invertedIndex[i] = new MutableRoaringBitmap();
    }
    start = System.currentTimeMillis();
  }

  @Override
  public void add(int docId, int dictionaryId) {
    invertedIndex[dictionaryId].add(docId);
  }

  @Override
  public long totalTimeTakeSoFar() {
    return (System.currentTimeMillis() - start);
  }

  @Override
  public void seal() throws IOException {
    final DataOutputStream out =
        new DataOutputStream(new BufferedOutputStream(new FileOutputStream(invertedIndexFile)));
    // First, write out offsets of bitmaps. The information can be used to access a certain bitmap directly.
    // Totally (invertedIndex.length+1) offsets will be written out; the last offset is used to calculate the length of
    // the last bitmap, which might be needed when accessing bitmaps randomly.
    // If a bitmap's offset is k, then k bytes need to be skipped to reach the bitmap.
    int offset = 4 * (invertedIndex.length + 1); // The first bitmap's offset
    out.writeInt(offset);
    for (final MutableRoaringBitmap element : invertedIndex) { // the other bitmap's offset
      offset += element.serializedSizeInBytes();
      out.writeInt(offset);
    }
    // write out bitmaps one by one
    for (final MutableRoaringBitmap element : invertedIndex) {
      element.serialize(out);
    }
    out.close();
    LOGGER.debug("persisted bitmap inverted index for column : " + spec.getName() + " in "
        + invertedIndexFile.getAbsolutePath());
  }

  @Override
  public void add(int docId, int[] dictionaryIds) {
    add(docId, dictionaryIds, dictionaryIds.length);
  }

  @Override
  public void add(int docId, int[] dictionaryIds, int length) {
    if (spec.isSingleValueField()) {
      throw new RuntimeException("Method not applicable to single value fields");
    }
    Arrays.sort(dictionaryIds, 0, length);
    indexMultiValue(docId, dictionaryIds, length);
  }

  private void indexSingleValue(int entry, int docId) {
    if (entry == -1) {
      return;
    }
    invertedIndex[entry].add(docId);
  }

  private void indexMultiValue(int docId, int[] entries, int length) {
    for (int i = 0; i < length; i++) {
      final int entry = entries[i];
      if (entry != -1) {
        invertedIndex[entry].add(docId);
      }
    }
  }
}
