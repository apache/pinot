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
import com.linkedin.pinot.core.io.reader.impl.FixedBitSingleValueMultiColReader;
import com.linkedin.pinot.core.segment.memory.PinotDataBuffer;
import java.io.IOException;



public class FixedBitSingleValueReader extends BaseSingleColumnSingleValueReader {

  private final FixedBitSingleValueMultiColReader dataFileReader;
  private final int rows;

  public FixedBitSingleValueReader(PinotDataBuffer indexDataBuffer, int rows,
      int columnSize, boolean hasNulls) {
    dataFileReader = new FixedBitSingleValueMultiColReader(indexDataBuffer, rows, 1,
        new int[] { columnSize }, new boolean[] { hasNulls });
    this.rows = rows;
  }

  public FixedBitSingleValueMultiColReader getDataFileReader() {
    return dataFileReader;
  }

  public int getLength() {
    return rows;
  }

  @Override
  public void close() throws IOException {
    dataFileReader.close();
  }

  @Override
  public int getInt(int row) {
    return dataFileReader.getInt(row, 0);
  }

  @Override
  public void readValues(int[] rows, int rowStartPos, int rowSize, int[] values, int valuesStartPos) {
    dataFileReader.readValues(rows, 0, rowStartPos, rowSize, values, valuesStartPos);
  }

  @Override
  public ReaderContext createContext() {
    //no need of context for fixed bit
    return null;
  }
}
