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
package com.linkedin.pinot.core.segment.index.readers;

import com.linkedin.pinot.common.utils.Pairs.IntPair;
import com.linkedin.pinot.core.io.reader.impl.FixedByteSingleValueMultiColReader;
import java.io.IOException;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.roaringbitmap.buffer.MutableRoaringBitmap;

public class SortedInvertedIndexReader implements InvertedIndexReader {
  private final int cardinality;
  private final FixedByteSingleValueMultiColReader indexReader;
  private final static IntPair EMPTY_PAIR = new IntPair(0, 0);

  /*
  public SortedInvertedIndexReader(File file, int cardinality, boolean isMmap) throws IOException {
    this.cardinality = cardinality;
    if (isMmap) {
      indexReader = FixedByteSingleValueMultiColReader.forMmap(file, cardinality, 2, new int[] {
          4, 4
      });
    } else {
      indexReader = FixedByteSingleValueMultiColReader.forHeap(file, cardinality, 2, new int[] {
          4, 4
      });
    }
  }
*/
  public SortedInvertedIndexReader(FixedByteSingleValueMultiColReader indexReader) {
    this.indexReader = indexReader;
    this.cardinality = indexReader.getNumberOfRows();
  }

  @Override
  public ImmutableRoaringBitmap getImmutable(int idx) {
    if (idx >= cardinality) {
      return new MutableRoaringBitmap();
    }
    MutableRoaringBitmap rr = new MutableRoaringBitmap();
    int min = indexReader.getInt(idx, 0);
    int max = indexReader.getInt(idx, 1);
    rr.add(min, max + 1);
    return rr;
  }

  @Override
  public IntPair getMinMaxRangeFor(int dictId) {
    if (dictId >= cardinality) {
      return EMPTY_PAIR;
    }
    return new IntPair(indexReader.getInt(dictId, 0), indexReader.getInt(dictId, 1));
  }

  @Override
  public void close() throws IOException {
    indexReader.close();
  }
}
