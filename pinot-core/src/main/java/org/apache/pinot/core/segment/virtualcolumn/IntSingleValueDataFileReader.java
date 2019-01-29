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
package org.apache.pinot.core.segment.virtualcolumn;

import java.io.IOException;
import java.util.Arrays;
import org.apache.pinot.core.io.reader.BaseSingleColumnSingleValueReader;
import org.apache.pinot.core.io.reader.DataFileReader;
import org.apache.pinot.core.io.reader.impl.ChunkReaderContext;


/**
 * Forward index that contains a single integer value.
 */
public class IntSingleValueDataFileReader extends BaseSingleColumnSingleValueReader<ChunkReaderContext> implements DataFileReader<ChunkReaderContext> {
  private int _value;

  public IntSingleValueDataFileReader(int value) {
    _value = value;
  }

  @Override
  public int getInt(int row) {
    return _value;
  }

  @Override
  public int getInt(int rowId, ChunkReaderContext context) {
    return _value;
  }

  @Override
  public void close()
      throws IOException {
  }

  @Override
  public void readValues(int[] rows, int rowStartPos, int rowSize, int[] values, int valuesStartPos) {
    Arrays.fill(values, valuesStartPos, rowSize, 0);
  }

  @Override
  public ChunkReaderContext createContext() {
    return null;
  }
}
