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
package com.linkedin.pinot.core.operator.docvalsets;

import com.linkedin.pinot.common.data.FieldSpec;
import com.linkedin.pinot.core.common.BaseBlockValSet;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;


/**
 * Extension of {@link com.linkedin.pinot.core.common.BaseBlockValSet}, where all docs have the same value.
 *
 * <ul>
 *   <li> Constructed with one string value (literal in a transform expression). </li>
 *   <li> Get methods return an array with the desired data type using the input string value. </li>
 *   <li> Caller responsible for calling for ensure that the input value can be converted to their desired data type. </li>
 *   <li> Builds a cache of values lazily, and returns the same in subsequent calls. </li>
 * </ul>
 */
public class ConstantBlockValSet extends BaseBlockValSet {
  private final String _value;
  private int _numDocs;
  private Map<FieldSpec.DataType, Object> _valuesMap;

  /**
   * Constructor for the class.
   *
   * @param value String representation of the value.
   * @param numDocs Number of docs in the block
   */
  public ConstantBlockValSet(String value, int numDocs) {
    _value = value;
    _numDocs = numDocs;
    _valuesMap = new HashMap<>();
  }

  @Override
  public int[] getIntValuesSV() {
    Object values = _valuesMap.get(FieldSpec.DataType.INT);

    if (values != null) {
      return (int[]) values;
    } else {
      int value = Integer.parseInt(_value);
      int[] valuesArray = new int[_numDocs];
      Arrays.fill(valuesArray, value);
      _valuesMap.put(FieldSpec.DataType.INT, valuesArray);
      return valuesArray;
    }
  }

  @Override
  public long[] getLongValuesSV() {
    Object values = _valuesMap.get(FieldSpec.DataType.LONG);

    if (values != null) {
      return (long[]) values;
    } else {
      long value = Long.parseLong(_value);
      long[] valuesArray = new long[_numDocs];
      Arrays.fill(valuesArray, value);
      _valuesMap.put(FieldSpec.DataType.LONG, valuesArray);
      return valuesArray;
    }
  }

  @Override
  public float[] getFloatValuesSV() {
    Object values = _valuesMap.get(FieldSpec.DataType.FLOAT);

    if (values != null) {
      return (float[]) values;
    } else {
      float value = Float.parseFloat(_value);
      float[] valuesArray = new float[_numDocs];
      Arrays.fill(valuesArray, value);
      _valuesMap.put(FieldSpec.DataType.FLOAT, valuesArray);
      return valuesArray;
    }
  }

  @Override
  public double[] getDoubleValuesSV() {
    Object values = _valuesMap.get(FieldSpec.DataType.FLOAT);

    if (values != null) {
      return (double[]) values;
    } else {
      double value = Double.parseDouble(_value);
      double[] valuesArray = new double[_numDocs];
      Arrays.fill(valuesArray, value);
      _valuesMap.put(FieldSpec.DataType.DOUBLE, valuesArray);
      return valuesArray;
    }
  }

  @Override
  public String[] getStringValuesSV() {
    Object values = _valuesMap.get(FieldSpec.DataType.STRING);

    if (values != null) {
      return (String[]) values;
    } else {
      String[] valuesArray = new String[_numDocs];
      Arrays.fill(valuesArray, _value);
      _valuesMap.put(FieldSpec.DataType.STRING, valuesArray);
      return valuesArray;
    }
  }

  @Override
  public int getNumDocs() {
    return _numDocs;
  }
}
