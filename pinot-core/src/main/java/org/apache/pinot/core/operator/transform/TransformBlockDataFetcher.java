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
package org.apache.pinot.core.operator.transform;

import com.google.common.base.Preconditions;
import java.io.Serializable;
import org.apache.pinot.core.common.BlockValSet;


public class TransformBlockDataFetcher {
  private final Fetcher[] _fetchers;

  public TransformBlockDataFetcher(BlockValSet[] blockValSets) {
    int numColumns = blockValSets.length;
    _fetchers = new Fetcher[numColumns];
    for (int i = 0; i < numColumns; i++) {
      _fetchers[i] = createFetcher(blockValSets[i]);
    }
  }

  public Serializable[] getRow(int docId) {
    return getRow(docId, new Serializable[_fetchers.length]);
  }

  public Serializable[] getRow(int docId, Serializable[] reuse) {
    int numColumns = _fetchers.length;
    for (int i = 0; i < numColumns; i++) {
      reuse[i] = _fetchers[i].getValue(docId);
    }
    return reuse;
  }

  private Fetcher createFetcher(BlockValSet blockValSet) {
    boolean singleValue = blockValSet.isSingleValue();
    switch (blockValSet.getValueType()) {
      case INT:
        return singleValue ? new IntSingleValueFetcher(blockValSet.getIntValuesSV())
            : new IntMultiValueFetcher(blockValSet.getIntValuesMV());
      case LONG:
        return singleValue ? new LongSingleValueFetcher(blockValSet.getLongValuesSV())
            : new LongMultiValueFetcher(blockValSet.getLongValuesMV());
      case FLOAT:
        return singleValue ? new FloatSingleValueFetcher(blockValSet.getFloatValuesSV())
            : new FloatMultiValueFetcher(blockValSet.getFloatValuesMV());
      case DOUBLE:
        return singleValue ? new DoubleSingleValueFetcher(blockValSet.getDoubleValuesSV())
            : new DoubleMultiValueFetcher(blockValSet.getDoubleValuesMV());
      case STRING:
        return singleValue ? new StringSingleValueFetcher(blockValSet.getStringValuesSV())
            : new StringMultiValueFetcher(blockValSet.getStringValuesMV());
      case BYTES:
        Preconditions.checkState(singleValue);
        return new BytesValueFetcher(blockValSet.getBytesValuesSV());
      default:
        throw new IllegalStateException();
    }
  }

  private interface Fetcher {
    Serializable getValue(int docId);
  }

  private class IntSingleValueFetcher implements Fetcher {
    private final int[] _values;

    IntSingleValueFetcher(int[] values) {
      _values = values;
    }

    public Serializable getValue(int docId) {
      return _values[docId];
    }
  }

  private class IntMultiValueFetcher implements Fetcher {
    private final int[][] _values;

    IntMultiValueFetcher(int[][] values) {
      _values = values;
    }

    public Serializable getValue(int docId) {
      return _values[docId];
    }
  }

  private class LongSingleValueFetcher implements Fetcher {
    private final long[] _values;

    LongSingleValueFetcher(long[] values) {
      _values = values;
    }

    public Serializable getValue(int docId) {
      return _values[docId];
    }
  }

  private class LongMultiValueFetcher implements Fetcher {
    private final long[][] _values;

    LongMultiValueFetcher(long[][] values) {
      _values = values;
    }

    public Serializable getValue(int docId) {
      return _values[docId];
    }
  }

  private class FloatSingleValueFetcher implements Fetcher {
    private final float[] _values;

    FloatSingleValueFetcher(float[] values) {
      _values = values;
    }

    public Serializable getValue(int docId) {
      return _values[docId];
    }
  }

  private class FloatMultiValueFetcher implements Fetcher {
    private final float[][] _values;

    FloatMultiValueFetcher(float[][] values) {
      _values = values;
    }

    public Serializable getValue(int docId) {
      return _values[docId];
    }
  }

  private class DoubleSingleValueFetcher implements Fetcher {
    private final double[] _values;

    DoubleSingleValueFetcher(double[] values) {
      _values = values;
    }

    public Serializable getValue(int docId) {
      return _values[docId];
    }
  }

  private class DoubleMultiValueFetcher implements Fetcher {
    private final double[][] _values;

    DoubleMultiValueFetcher(double[][] values) {
      _values = values;
    }

    public Serializable getValue(int docId) {
      return _values[docId];
    }
  }

  private class StringSingleValueFetcher implements Fetcher {
    private final String[] _values;

    StringSingleValueFetcher(String[] values) {
      _values = values;
    }

    public Serializable getValue(int docId) {
      return _values[docId];
    }
  }

  private class StringMultiValueFetcher implements Fetcher {
    private final String[][] _values;

    StringMultiValueFetcher(String[][] values) {
      _values = values;
    }

    public Serializable getValue(int docId) {
      return _values[docId];
    }
  }

  private class BytesValueFetcher implements Fetcher {
    private final byte[][] _values;

    BytesValueFetcher(byte[][] values) {
      _values = values;
    }

    public Serializable getValue(int docId) {
      return _values[docId];
    }
  }
}
