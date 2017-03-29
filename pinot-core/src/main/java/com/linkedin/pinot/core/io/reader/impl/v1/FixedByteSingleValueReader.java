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
import com.linkedin.pinot.core.io.reader.impl.FixedByteSingleValueMultiColReader;
import com.linkedin.pinot.core.segment.memory.PinotDataBuffer;
import java.io.IOException;

/**
 * Nov 13, 2014
 */

public class FixedByteSingleValueReader extends BaseSingleColumnSingleValueReader {

  private final FixedByteSingleValueMultiColReader dataFileReader;
  private final int rows;

  public FixedByteSingleValueReader(PinotDataBuffer indexDataBuffer, int rows, int columnSizeInBytes,
      boolean hasNulls) {

    // TODO: check what hasNulls was used for
    dataFileReader = new FixedByteSingleValueMultiColReader(indexDataBuffer, rows, 1,
        new int[] { columnSizeInBytes});
    this.rows = rows;
  }

  public FixedByteSingleValueMultiColReader getDataFileReader() {
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
    dataFileReader.readIntValues(rows, 0, rowStartPos, rowSize, values, valuesStartPos);
  }

  @Override
  public ReaderContext createContext() {
    //no need of context
    return null;
  }
}
