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
import org.apache.pinot.core.io.reader.BaseSingleColumnMultiValueReader;
import org.apache.pinot.core.io.reader.impl.v1.FixedBitMultiValueReader;


/**
 * Reader for the multi-value column with the constant value
 */
public class ConstantMultiValueInvertedIndex extends BaseSingleColumnMultiValueReader<FixedBitMultiValueReader.Context> implements SortedIndexMultiValueReader<FixedBitMultiValueReader.Context> {
  private int _length;

  public ConstantMultiValueInvertedIndex(int length) {
    _length = length;
  }

  @Override
  public Pairs.IntPair getDocIds(int dictId) {
    if (dictId == 0) {
      return new Pairs.IntPair(0, _length);
    } else {
      return new Pairs.IntPair(-1, -1);
    }
  }

  @Override
  public int getIntArray(int row, int[] intArray) {
    return 0;
  }

  @Override
  public int getIntArray(int row, int[] intArray, FixedBitMultiValueReader.Context context) {
    return 0;
  }

  @Override
  public void close()
      throws IOException {
  }

  @Override
  public FixedBitMultiValueReader.Context createContext() {
    return null;
  }

  public void setLength(int length) {
    _length = length;
  }
}