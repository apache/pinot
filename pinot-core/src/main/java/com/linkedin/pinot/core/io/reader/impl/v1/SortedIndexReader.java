/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
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

import com.linkedin.pinot.common.utils.Pairs;
import com.linkedin.pinot.core.io.reader.ReaderContext;
import com.linkedin.pinot.core.io.reader.SingleColumnSingleValueReader;
import com.linkedin.pinot.core.segment.index.readers.InvertedIndexReader;
import java.io.IOException;


/**
 * Interface for sorted index readers.
 */
public interface SortedIndexReader<T extends ReaderContext> extends SingleColumnSingleValueReader<T>, InvertedIndexReader<Pairs.IntPair> {
  @Override
  int getInt(int row);

  @Override
  int getInt(int row, T context);

  @Override
  void readValues(int[] rows, int rowsStartIndex, int rowSize, int[] values, int valuesStartIndex);

  @Override
  T createContext();

  @Override
  Pairs.IntPair getDocIds(int dictId);

  @Override
  void close() throws IOException;
}
