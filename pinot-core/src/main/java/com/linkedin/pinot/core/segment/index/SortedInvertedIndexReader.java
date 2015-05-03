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
package com.linkedin.pinot.core.segment.index;

import java.io.File;
import java.io.IOException;

import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.roaringbitmap.buffer.MutableRoaringBitmap;

import com.linkedin.pinot.core.index.reader.impl.FixedByteWidthRowColDataFileReader;


public class SortedInvertedIndexReader implements InvertedIndexReader {
  private final File indexFile;
  private final int cardinality;
  private final FixedByteWidthRowColDataFileReader indexReader;

  public SortedInvertedIndexReader(File file, int cardinality, boolean isMmap) throws IOException {
    indexFile = file;
    this.cardinality = cardinality;
    if (isMmap) {
      indexReader = FixedByteWidthRowColDataFileReader.forMmap(indexFile, cardinality, 2, new int[] { 4, 4 });
    } else {
      indexReader = FixedByteWidthRowColDataFileReader.forHeap(indexFile, cardinality, 2, new int[] { 4, 4 });
    }
  }

  @Override
  public ImmutableRoaringBitmap getImmutable(int idx) {
    if (idx >= cardinality) {
      return new MutableRoaringBitmap();
    }
    MutableRoaringBitmap rr = new MutableRoaringBitmap();
    int min = indexReader.getInt(idx, 0);
    int max = indexReader.getInt(idx, 1);
    for (int i = min; i <= max; i++) {
      rr.add(i);
    }
    return rr;
  }

  @Override
  public int[] getMinMaxRangeFor(int dictId) {
    int[] ret = new int[2];
    if (dictId >= cardinality) {
      return ret;
    }
    ret[0] = indexReader.getInt(dictId, 0);
    ret[1] = indexReader.getInt(dictId, 1);
    return ret;
  }

  @Override
  public void close() throws IOException {
    indexReader.close();
  }
}
