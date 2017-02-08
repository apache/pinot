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
package com.linkedin.pinot.core.realtime.impl.dictionary;

import com.linkedin.pinot.common.data.FieldSpec;
import com.linkedin.pinot.core.segment.index.readers.Dictionary;


public abstract class MutableDictionaryReader implements Dictionary {
  protected static final Integer INVALID_ID = -1;
  protected boolean hasNull = false;

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

  public boolean hasNull() {
    return hasNull;
  }

  public abstract Object getMinVal();

  public abstract Object getMaxVal();

  public abstract void index(Object rawValue);

  @Override
  public abstract int indexOf(Object rawValue);

  public abstract boolean contains(Object o);

  @Override
  public abstract Object get(int dictionaryId);

  public abstract boolean inRange(String lower, String upper, int indexOfValueToCompare, boolean includeLower,
      boolean includeUpper);

  @Override
  public abstract long getLongValue(int dictionaryId);

  @Override
  public abstract double getDoubleValue(int dictionaryId);

  @Override
  public abstract int getIntValue(int dictionaryId);

  @Override
  public abstract float getFloatValue(int dictionaryId);

  @Override
  public abstract String toString(int dictionaryId);

  public abstract void print();

  public abstract boolean isEmpty();
}
