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
import org.apache.pinot.core.io.reader.SingleColumnMultiValueReader;
import org.apache.pinot.core.io.reader.impl.v1.FixedBitMultiValueReader;
import org.apache.pinot.core.segment.index.readers.InvertedIndexReader;


/**
 * Reader for the multi-value column with the constant value
 */
public class ConstantMultiValueInvertedIndex implements SingleColumnMultiValueReader<FixedBitMultiValueReader.Context>, InvertedIndexReader<Pairs.IntPair> {
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
  public Pairs.IntPair getDocIds(Object value) {
    // This should not be called from anywhere. If it happens, there is a bug
    // and that's why we throw illegal state exception
    throw new IllegalStateException("sorted inverted index reader supports lookup only using dictionary id");
  }

  @Override
  public int getCharArray(int row, char[] charArray) {
    return 0;
  }

  @Override
  public int getShortArray(int row, short[] shortsArray) {
    return 0;
  }

  @Override
  public int getIntArray(int row, int[] intArray) {
    return 0;
  }

  @Override
  public int getLongArray(int row, long[] longArray) {
    return 0;
  }

  @Override
  public int getFloatArray(int row, float[] floatArray) {
    return 0;
  }

  @Override
  public int getDoubleArray(int row, double[] doubleArray) {
    return 0;
  }

  @Override
  public int getStringArray(int row, String[] stringArray) {
    return 0;
  }

  @Override
  public int getBytesArray(int row, byte[][] bytesArray) {
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