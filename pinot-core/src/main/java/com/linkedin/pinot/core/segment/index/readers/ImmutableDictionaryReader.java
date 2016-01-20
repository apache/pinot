/**
 * Copyright (C) 2014-2015 LinkedIn Corp. (pinot-core@linkedin.com)
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
package com.linkedin.pinot.core.segment.index.readers;

import com.linkedin.pinot.core.indexsegment.utils.ByteBufferBinarySearchUtil;
import com.linkedin.pinot.core.io.reader.impl.FixedByteSingleValueMultiColReader;
import java.io.File;
import java.io.IOException;


/**
 * Nov 13, 2014
 */

public abstract class ImmutableDictionaryReader implements Dictionary {

  protected final FixedByteSingleValueMultiColReader dataFileReader;
  private final ByteBufferBinarySearchUtil fileSearcher;
  private final int rows;

  protected ImmutableDictionaryReader(File dictFile, int rows, int columnSize, boolean isMmap) throws IOException {
    if (isMmap) {
      dataFileReader = FixedByteSingleValueMultiColReader.forMmap(dictFile, rows, 1, new int[] { columnSize });
    } else {
      dataFileReader = FixedByteSingleValueMultiColReader.forHeap(dictFile, rows, 1, new int[] { columnSize });
    }
    this.rows = rows;
    fileSearcher = new ByteBufferBinarySearchUtil(dataFileReader);
  }

  protected int intIndexOf(int actualValue) {
    return fileSearcher.binarySearch(0, actualValue);
  }

  protected int floatIndexOf(float actualValue) {
    return fileSearcher.binarySearch(0, actualValue);
  }

  protected int longIndexOf(long actualValue) {
    return fileSearcher.binarySearch(0, actualValue);
  }

  protected int doubleIndexOf(double actualValue) {
    return fileSearcher.binarySearch(0, actualValue);
  }

  protected int stringIndexOf(String actualValue) {
    return fileSearcher.binarySearch(0, actualValue);
  }

  @Override
  public abstract int indexOf(Object rawValue);

  @Override
  public abstract Object get(int dictionaryId);

  @Override
  public abstract long getLongValue(int dictionaryId);

  @Override
  public abstract double getDoubleValue(int dictionaryId);

  @Override
  public abstract String toString(int dictionaryId);

  public void close() throws IOException {
    dataFileReader.close();
  }

  @Override
  public int length() {
    return rows;
  }

  @Override
  public void readIntValues(int[] dictionaryIds, int startPos, int limit, int[] outValues, int outStartPos) {
    dataFileReader.readIntValues(dictionaryIds, 0, startPos, limit, outValues, outStartPos);
  }

  @Override
  public void readLongValues(int[] dictionaryIds, int startPos, int limit, long[] outValues, int outStartPos) {
    dataFileReader.readLongValues(dictionaryIds, 0, startPos, limit, outValues, outStartPos);
  }

  @Override
  public void readFloatValues(int[] dictionaryIds, int startPos, int limit, float[] outValues, int outStartPos) {
    dataFileReader.readFloatValues(dictionaryIds, 0, startPos, limit, outValues, outStartPos);
  }

  @Override
  public void readDoubleValues(int[] dictionaryIds, int startPos, int limit, double[] outValues, int outStartPos) {
    dataFileReader.readDoubleValues(dictionaryIds, 0, startPos, limit, outValues, outStartPos);
  }


  @Override
  public void readStringValues(int[] dictionaryIds, int startPos, int limit, String[] outValues, int outStartPos) {
    dataFileReader.readStringValues(dictionaryIds, 0, startPos, limit, outValues, outStartPos);
  }


}
