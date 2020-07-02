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
package org.apache.pinot.core.io.reader.impl;

import java.io.IOException;
import java.util.Arrays;
import org.apache.pinot.common.utils.Pairs;
import org.apache.pinot.core.io.reader.BaseSingleColumnSingleValueReader;
import org.apache.pinot.core.io.reader.ReaderContext;
import org.apache.pinot.core.io.reader.impl.v1.SortedIndexReader;


/**
 * Sorted index for single-value column with constant values.
 */
public class ConstantSVSortedIndex extends BaseSingleColumnSingleValueReader<ReaderContext> implements SortedIndexReader<ReaderContext> {
  private final int _numDocs;

  public ConstantSVSortedIndex(int numDocs) {
    _numDocs = numDocs;
  }

  @Override
  public int getInt(int row) {
    return 0;
  }

  @Override
  public int getInt(int rowId, ReaderContext context) {
    return 0;
  }

  @Override
  public void readValues(int[] rows, int rowStartPos, int rowSize, int[] values, int valuesStartPos) {
    Arrays.fill(values, valuesStartPos, valuesStartPos + rowSize, 0);
  }

  @Override
  public ReaderContext createContext() {
    return null;
  }

  @Override
  public Pairs.IntPair getDocIds(int dictId) {
    return new Pairs.IntPair(0, _numDocs - 1);
  }

  @Override
  public Pairs.IntPair getDocIds(Object value) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void close()
      throws IOException {
  }
}
