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
package org.apache.pinot.core.common;

import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.utils.ByteArray;


public class RowBasedBlockValueFetcher {
  private final ValueFetcher[] _valueFetchers;

  public RowBasedBlockValueFetcher(BlockValSet[] blockValSets) {
    int numColumns = blockValSets.length;
    _valueFetchers = new ValueFetcher[numColumns];
    for (int i = 0; i < numColumns; i++) {
      _valueFetchers[i] = createFetcher(blockValSets[i]);
    }
  }

  public Object[] getRow(int docId) {
    int numColumns = _valueFetchers.length;
    Object[] row = new Object[numColumns];
    for (int i = 0; i < numColumns; i++) {
      row[i] = _valueFetchers[i].getValue(docId);
    }
    return row;
  }

  public void getRow(int docId, Object[] buffer, int startIndex) {
    for (ValueFetcher valueFetcher : _valueFetchers) {
      buffer[startIndex++] = valueFetcher.getValue(docId);
    }
  }

  private ValueFetcher createFetcher(BlockValSet blockValSet) {
    DataType storedType = blockValSet.getValueType().getStoredType();
    if (blockValSet.isSingleValue()) {
      switch (storedType) {
        case INT:
          return new IntSingleValueFetcher(blockValSet.getIntValuesSV());
        case LONG:
          return new LongSingleValueFetcher(blockValSet.getLongValuesSV());
        case FLOAT:
          return new FloatSingleValueFetcher(blockValSet.getFloatValuesSV());
        case DOUBLE:
          return new DoubleSingleValueFetcher(blockValSet.getDoubleValuesSV());
        case STRING:
          return new StringSingleValueFetcher(blockValSet.getStringValuesSV());
        case BYTES:
          return new BytesValueFetcher(blockValSet.getBytesValuesSV());
        default:
          throw new IllegalStateException("Unsupported value type: " + storedType + " for single-value column");
      }
    } else {
      switch (storedType) {
        case INT:
          return new IntMultiValueFetcher(blockValSet.getIntValuesMV());
        case LONG:
          return new LongMultiValueFetcher(blockValSet.getLongValuesMV());
        case FLOAT:
          return new FloatMultiValueFetcher(blockValSet.getFloatValuesMV());
        case DOUBLE:
          return new DoubleMultiValueFetcher(blockValSet.getDoubleValuesMV());
        case STRING:
          return new StringMultiValueFetcher(blockValSet.getStringValuesMV());
        default:
          throw new IllegalStateException("Unsupported value type: " + storedType + " for multi-value column");
      }
    }
  }

  private interface ValueFetcher {
    Object getValue(int docId);
  }

  private static class IntSingleValueFetcher implements ValueFetcher {
    private final int[] _values;

    IntSingleValueFetcher(int[] values) {
      _values = values;
    }

    public Integer getValue(int docId) {
      return _values[docId];
    }
  }

  private static class LongSingleValueFetcher implements ValueFetcher {
    private final long[] _values;

    LongSingleValueFetcher(long[] values) {
      _values = values;
    }

    public Long getValue(int docId) {
      return _values[docId];
    }
  }

  private static class FloatSingleValueFetcher implements ValueFetcher {
    private final float[] _values;

    FloatSingleValueFetcher(float[] values) {
      _values = values;
    }

    public Float getValue(int docId) {
      return _values[docId];
    }
  }

  private static class DoubleSingleValueFetcher implements ValueFetcher {
    private final double[] _values;

    DoubleSingleValueFetcher(double[] values) {
      _values = values;
    }

    public Double getValue(int docId) {
      return _values[docId];
    }
  }

  private static class StringSingleValueFetcher implements ValueFetcher {
    private final String[] _values;

    StringSingleValueFetcher(String[] values) {
      _values = values;
    }

    public String getValue(int docId) {
      return _values[docId];
    }
  }

  private static class BytesValueFetcher implements ValueFetcher {
    private final byte[][] _values;

    BytesValueFetcher(byte[][] values) {
      _values = values;
    }

    public ByteArray getValue(int docId) {
      return new ByteArray(_values[docId]);
    }
  }

  private static class IntMultiValueFetcher implements ValueFetcher {
    private final int[][] _values;

    IntMultiValueFetcher(int[][] values) {
      _values = values;
    }

    public int[] getValue(int docId) {
      return _values[docId];
    }
  }

  private static class LongMultiValueFetcher implements ValueFetcher {
    private final long[][] _values;

    LongMultiValueFetcher(long[][] values) {
      _values = values;
    }

    public long[] getValue(int docId) {
      return _values[docId];
    }
  }

  private static class FloatMultiValueFetcher implements ValueFetcher {
    private final float[][] _values;

    FloatMultiValueFetcher(float[][] values) {
      _values = values;
    }

    public float[] getValue(int docId) {
      return _values[docId];
    }
  }

  private static class DoubleMultiValueFetcher implements ValueFetcher {
    private final double[][] _values;

    DoubleMultiValueFetcher(double[][] values) {
      _values = values;
    }

    public double[] getValue(int docId) {
      return _values[docId];
    }
  }

  private static class StringMultiValueFetcher implements ValueFetcher {
    private final String[][] _values;

    StringMultiValueFetcher(String[][] values) {
      _values = values;
    }

    public String[] getValue(int docId) {
      return _values[docId];
    }
  }
}
