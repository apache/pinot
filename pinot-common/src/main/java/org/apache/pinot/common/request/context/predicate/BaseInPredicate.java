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
package org.apache.pinot.common.request.context.predicate;

import java.math.BigDecimal;
import java.util.List;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.spi.utils.BooleanUtils;
import org.apache.pinot.spi.utils.ByteArray;
import org.apache.pinot.spi.utils.BytesUtils;
import org.apache.pinot.spi.utils.TimestampUtils;


/**
 * Base predicate for IN and NOT_IN.
 */
public abstract class BaseInPredicate extends BasePredicate {
  protected final List<String> _values;

  // Cache the parsed values
  private int[] _intValues;
  private long[] _longValues;
  private float[] _floatValues;
  private double[] _doubleValues;
  private BigDecimal[] _bigDecimalValues;
  private int[] _booleanValues;
  private long[] _timestampValues;
  private long[] _localDateTimeValues;
  private long[] _localDateValues;
  private long[] _localTimeValues;
  private ByteArray[] _bytesValues;

  public BaseInPredicate(ExpressionContext lhs, List<String> values) {
    super(lhs);
    _values = values;
  }

  public List<String> getValues() {
    return _values;
  }

  public int[] getIntValues() {
    int[] intValues = _intValues;
    if (intValues == null) {
      int numValues = _values.size();
      intValues = new int[numValues];
      for (int i = 0; i < numValues; i++) {
        intValues[i] = Integer.parseInt(_values.get(i));
      }
      _intValues = intValues;
    }
    return intValues;
  }

  public long[] getLongValues() {
    long[] longValues = _longValues;
    if (longValues == null) {
      int numValues = _values.size();
      longValues = new long[numValues];
      for (int i = 0; i < numValues; i++) {
        longValues[i] = Long.parseLong(_values.get(i));
      }
      _longValues = longValues;
    }
    return longValues;
  }

  public float[] getFloatValues() {
    float[] floatValues = _floatValues;
    if (floatValues == null) {
      int numValues = _values.size();
      floatValues = new float[numValues];
      for (int i = 0; i < numValues; i++) {
        floatValues[i] = Float.parseFloat(_values.get(i));
      }
      _floatValues = floatValues;
    }
    return floatValues;
  }

  public double[] getDoubleValues() {
    double[] doubleValues = _doubleValues;
    if (doubleValues == null) {
      int numValues = _values.size();
      doubleValues = new double[numValues];
      for (int i = 0; i < numValues; i++) {
        doubleValues[i] = Double.parseDouble(_values.get(i));
      }
      _doubleValues = doubleValues;
    }
    return doubleValues;
  }

  public BigDecimal[] getBigDecimalValues() {
    BigDecimal[] bigDecimalValues = _bigDecimalValues;
    if (bigDecimalValues == null) {
      int numValues = _values.size();
      bigDecimalValues = new BigDecimal[numValues];
      for (int i = 0; i < numValues; i++) {
        bigDecimalValues[i] = new BigDecimal(_values.get(i));
      }
      _bigDecimalValues = bigDecimalValues;
    }
    return bigDecimalValues;
  }

  public int[] getBooleanValues() {
    int[] booleanValues = _booleanValues;
    if (booleanValues == null) {
      int numValues = _values.size();
      booleanValues = new int[numValues];
      for (int i = 0; i < numValues; i++) {
        booleanValues[i] = BooleanUtils.toInt(_values.get(i));
      }
      _booleanValues = booleanValues;
    }
    return booleanValues;
  }

  public long[] getTimestampValues() {
    long[] timestampValues = _timestampValues;
    if (timestampValues == null) {
      int numValues = _values.size();
      timestampValues = new long[numValues];
      for (int i = 0; i < numValues; i++) {
        timestampValues[i] = TimestampUtils.toMillisSinceEpoch(_values.get(i));
      }
      _timestampValues = timestampValues;
    }
    return timestampValues;
  }

  public long[] getLocalDateTimeValues() {
    long[] localDateTimeValues = _localDateTimeValues;
    if (localDateTimeValues == null) {
      int numValues = _values.size();
      localDateTimeValues = new long[numValues];
      for (int i = 0; i < numValues; i++) {
        localDateTimeValues[i] = TimestampUtils.toMillisSinceEpochInUTC(_values.get(i));
      }
      _localDateTimeValues = localDateTimeValues;
    }
    return localDateTimeValues;
  }

  public long[] getLocalDateValues() {
    long[] localDateValues = _localDateValues;
    if (localDateValues == null) {
      int numValues = _values.size();
      localDateValues = new long[numValues];
      for (int i = 0; i < numValues; i++) {
        localDateValues[i] = TimestampUtils.toDaysSinceEpoch(_values.get(i));
      }
      _localDateValues = localDateValues;
    }
    return localDateValues;
  }

  public long[] getLocalTimeValues() {
    long[] localTimeValues = _localTimeValues;
    if (localTimeValues == null) {
      int numValues = _values.size();
      localTimeValues = new long[numValues];
      for (int i = 0; i < numValues; i++) {
        localTimeValues[i] = TimestampUtils.toMillisOfDay(_values.get(i));
      }
      _localTimeValues = localTimeValues;
    }
    return localTimeValues;
  }

  public ByteArray[] getBytesValues() {
    ByteArray[] bigDecimalValues = _bytesValues;
    if (bigDecimalValues == null) {
      int numValues = _values.size();
      bigDecimalValues = new ByteArray[numValues];
      for (int i = 0; i < numValues; i++) {
        bigDecimalValues[i] = BytesUtils.toByteArray(_values.get(i));
      }
      _bytesValues = bigDecimalValues;
    }
    return bigDecimalValues;
  }
}
