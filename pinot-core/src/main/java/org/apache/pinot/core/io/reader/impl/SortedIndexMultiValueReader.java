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
import org.apache.pinot.common.utils.Pairs;
import org.apache.pinot.core.io.reader.ReaderContext;
import org.apache.pinot.core.io.reader.SingleColumnMultiValueReader;
import org.apache.pinot.core.segment.index.readers.InvertedIndexReader;


/**
 * Interface for sorted index multi-value readers.
 */
public interface SortedIndexMultiValueReader<T extends ReaderContext> extends SingleColumnMultiValueReader<T>, InvertedIndexReader<Pairs.IntPair> {
  @Override
  int getIntArray(int row, int[] intArray);

  @Override
  int getIntArray(int row, int[] intArray, T context);

  @Override
  T createContext();

  @Override
  Pairs.IntPair getDocIds(int dictId);

  @Override
  void close()
      throws IOException;
}
