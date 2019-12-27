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
package org.apache.pinot.core.io.reader.impl.v1;

import com.google.common.base.Preconditions;
import java.io.IOException;
import org.apache.pinot.common.utils.Pairs;
import org.apache.pinot.core.common.Predicate;
import org.apache.pinot.core.io.reader.BaseSingleColumnSingleValueReader;
import org.apache.pinot.core.io.reader.ReaderContext;
import org.apache.pinot.core.io.util.FixedByteValueReaderWriter;
import org.apache.pinot.core.segment.memory.PinotDataBuffer;


public class SortedIndexSingleValueReaderImpl extends BaseSingleColumnSingleValueReader<SortedIndexSingleValueReaderImpl.Context> implements SortedIndexSingleValueReader<SortedIndexSingleValueReaderImpl.Context> {
  private final FixedByteValueReaderWriter _reader;
  private final int _cardinality;

  public SortedIndexSingleValueReaderImpl(PinotDataBuffer dataBuffer, int cardinality) {
    // 2 values per dictionary id
    Preconditions.checkState(dataBuffer.size() == 2 * cardinality * Integer.BYTES);
    _reader = new FixedByteValueReaderWriter(dataBuffer);
    _cardinality = cardinality;
  }

  @Override
  public int getInt(int row) {
    // Only one value in dictionary
    if (_cardinality == 1) {
      return 0;
    }

    return binarySearch(row, 0, _cardinality - 1);
  }

  @Override
  public int getInt(int row, Context context) {
    // Only one value in dictionary
    if (_cardinality == 1) {
      return 0;
    }

    int contextDictId = context._dictId;
    int contextStartOffset = context._startOffset;
    int contextEndOffset = context._endOffset;
    if (row >= contextStartOffset) {
      // Same value
      if (row <= contextEndOffset) {
        return contextDictId;
      }

      // Next value in dictionary
      int nextDictId = contextDictId + 1;
      int nextEndOffset = _reader.getInt(2 * nextDictId + 1);
      if (row <= nextEndOffset) {
        context._dictId = nextDictId;
        context._startOffset = contextEndOffset + 1;
        context._endOffset = nextEndOffset;
        return nextDictId;
      }
    }

    int dictId;
    if (row < contextStartOffset) {
      dictId = binarySearch(row, 0, contextDictId - 1);
    } else {
      dictId = binarySearch(row, contextDictId + 2, _cardinality - 1);
    }
    context._dictId = dictId;
    context._startOffset = _reader.getInt(2 * dictId);
    context._endOffset = _reader.getInt(2 * dictId + 1);
    return dictId;
  }

  private int binarySearch(int row, int low, int high) {
    while (low <= high) {
      int mid = (low + high) / 2;
      if (_reader.getInt(2 * mid) <= row) {
        low = mid + 1;
      } else {
        high = mid - 1;
      }
    }
    return high;
  }

  @Override
  public void readValues(int[] rows, int rowsStartIndex, int rowSize, int[] values, int valuesStartIndex) {
    int rowsEndIndex = rowsStartIndex + rowSize;
    if (_cardinality == 1) {
      for (int i = rowsStartIndex; i < rowsEndIndex; i++) {
        values[valuesStartIndex++] = 0;
      }
    } else {
      Context context = new Context();
      for (int i = rowsStartIndex; i < rowsEndIndex; i++) {
        values[valuesStartIndex++] = getInt(rows[i], context);
      }
    }
  }

  @Override
  public Context createContext() {
    return new Context();
  }

  @Override
  public Pairs.IntPair getDocIds(int dictId) {
    return new Pairs.IntPair(_reader.getInt(2 * dictId), _reader.getInt(2 * dictId + 1));
  }

  @Override
  public void close()
      throws IOException {
    _reader.close();
  }

  public static class Context implements ReaderContext {
    public int _dictId = -1;
    public int _startOffset = -1;
    // Inclusive
    public int _endOffset = -1;
  }

  @Override
  public Pairs.IntPair getDocIds(Object value) {
    // This should not be called from anywhere. If it happens, there is a bug
    // and that's why we throw illegal state exception
    throw new IllegalStateException("sorted inverted index reader supports lookup only on dictionary id");
  }
}
