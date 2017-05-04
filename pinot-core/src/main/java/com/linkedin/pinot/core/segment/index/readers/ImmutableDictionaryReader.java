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
package com.linkedin.pinot.core.segment.index.readers;

import com.linkedin.pinot.common.data.FieldSpec;
import com.linkedin.pinot.core.indexsegment.utils.ByteBufferBinarySearchUtil;
import com.linkedin.pinot.core.io.reader.impl.FixedByteSingleValueMultiColReader;
import com.linkedin.pinot.core.segment.memory.PinotDataBuffer;
import java.io.IOException;


public abstract class ImmutableDictionaryReader implements Dictionary {

  protected final FixedByteSingleValueMultiColReader dataFileReader;
  private final ByteBufferBinarySearchUtil fileSearcher;
  private final int rows;

  protected ImmutableDictionaryReader(PinotDataBuffer dataBuffer, int rows, int columnSize) {
    dataFileReader = new FixedByteSingleValueMultiColReader(dataBuffer, rows, new int[] { columnSize });
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

  public void close() throws IOException {
    dataFileReader.close();
  }

  @Override
  public int length() {
    return rows;
  }

  @Override
  public void readIntValues(int[] dictionaryIds, int startPos, int limit, int[] outValues, int outStartPos) {
    readValues(dictionaryIds, startPos, limit, outValues, outStartPos, FieldSpec.DataType.INT);
  }

  @Override
  public void readLongValues(int[] dictionaryIds, int startPos, int limit, long[] outValues, int outStartPos) {
    readValues(dictionaryIds, startPos, limit, outValues, outStartPos, FieldSpec.DataType.LONG);
  }

  @Override
  public void readFloatValues(int[] dictionaryIds, int startPos, int limit, float[] outValues, int outStartPos) {
    readValues(dictionaryIds, startPos, limit, outValues, outStartPos, FieldSpec.DataType.FLOAT);
  }

  @Override
  public void readDoubleValues(int[] dictionaryIds, int startPos, int limit, double[] outValues, int outStartPos) {
    readValues(dictionaryIds, startPos, limit, outValues, outStartPos, FieldSpec.DataType.DOUBLE);
  }

  @Override
  public void readStringValues(int[] dictionaryIds, int startPos, int limit, String[] outValues, int outStartPos) {
    readValues(dictionaryIds, startPos, limit, outValues, outStartPos, FieldSpec.DataType.STRING);
  }

  protected void readValues(int[] dictionaryIds, int startPos, int limit, Object values, int outStartPos, FieldSpec.DataType type) {
    int endPos = startPos + limit;

    switch (type) {

      case INT: {
        int[] outValues = (int[]) values;
        for (int iter = startPos; iter < endPos; ++iter) {
          int dictId = dictionaryIds[iter];
          outValues[outStartPos++] = getIntValue(dictId);
        }

      }
      break;
      case LONG: {
        long[] outValues = (long[]) values;
        for (int iter = startPos; iter < endPos; ++iter) {
          int dictId = dictionaryIds[iter];
          outValues[outStartPos++] = getLongValue(dictId);
        }
      }
      break;
      case FLOAT: {
        float[] outValues = (float[]) values;
        for (int iter = startPos; iter < endPos; ++iter) {
          int dictId = dictionaryIds[iter];
          outValues[outStartPos++] = getFloatValue(dictId);
        }
      }
      break;
      case DOUBLE: {
        double[] outValues = (double[]) values;
        for (int iter = startPos; iter < endPos; ++iter) {
          int dictId = dictionaryIds[iter];
          outValues[outStartPos++] = getDoubleValue(dictId);
        }
      }
      break;
      case STRING: {
        String[] outValues = (String[]) values;
        for (int iter = startPos; iter < endPos; ++iter) {
          int dictId = dictionaryIds[iter];
          outValues[outStartPos++] = getStringValue(dictId);
        }
      }
      break;
    }

  }

}
