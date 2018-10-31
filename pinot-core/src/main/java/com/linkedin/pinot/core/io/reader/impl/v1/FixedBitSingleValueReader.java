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
package com.linkedin.pinot.core.io.reader.impl.v1;

import com.linkedin.pinot.core.io.reader.BaseSingleColumnSingleValueReader;
import com.linkedin.pinot.core.io.reader.ReaderContext;
import com.linkedin.pinot.core.io.util.FixedBitIntReaderWriter;
import com.linkedin.pinot.core.segment.memory.PinotDataBuffer;


public final class FixedBitSingleValueReader extends BaseSingleColumnSingleValueReader {
  private final FixedBitIntReaderWriter _reader;

  public FixedBitSingleValueReader(PinotDataBuffer dataBuffer, int numRows, int numBitsPerValue) {
    _reader = new FixedBitIntReaderWriter(dataBuffer, numRows, numBitsPerValue);
  }

  @Override
  public int getInt(int row) {
    return _reader.readInt(row);
  }

  @Override
  public int getInt(int row, ReaderContext context) {
    return _reader.readInt(row);
  }

  @Override
  public void readValues(int[] rows, int rowsStartIndex, int rowSize, int[] values, int valuesStartIndex) {
    int rowsEndIndex = rowsStartIndex + rowSize;
    for (int i = rowsStartIndex; i < rowsEndIndex; i++) {
      values[valuesStartIndex++] = getInt(rows[i]);
    }
  }

  @Override
  public ReaderContext createContext() {
    return null;
  }

  @Override
  public void close() {
    _reader.close();
  }
}
