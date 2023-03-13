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
package org.apache.pinot.core.operator.transform.function;

import java.math.BigDecimal;
import org.apache.pinot.common.function.TransformFunctionType;
import org.apache.pinot.core.operator.blocks.ValueBlock;


public class GreatestTransformFunction extends SelectTupleElementTransformFunction {

  public GreatestTransformFunction() {
    super(TransformFunctionType.GREATEST.getName());
  }

  @Override
  public int[] transformToIntValuesSV(ValueBlock valueBlock) {
    int numDocs = valueBlock.getNumDocs();
    if (_intValuesSV == null) {
      _intValuesSV = new int[numDocs];
    }
    int[] values = _arguments.get(0).transformToIntValuesSV(valueBlock);
    System.arraycopy(values, 0, _intValuesSV, 0, numDocs);
    for (int i = 1; i < _arguments.size(); i++) {
      values = _arguments.get(i).transformToIntValuesSV(valueBlock);
      for (int j = 0; j < numDocs & j < values.length; j++) {
        _intValuesSV[j] = Math.max(_intValuesSV[j], values[j]);
      }
    }
    return _intValuesSV;
  }

  @Override
  public long[] transformToLongValuesSV(ValueBlock valueBlock) {
    int numDocs = valueBlock.getNumDocs();
    if (_longValuesSV == null) {
      _longValuesSV = new long[numDocs];
    }
    long[] values = _arguments.get(0).transformToLongValuesSV(valueBlock);
    System.arraycopy(values, 0, _longValuesSV, 0, numDocs);
    for (int i = 1; i < _arguments.size(); i++) {
      values = _arguments.get(i).transformToLongValuesSV(valueBlock);
      for (int j = 0; j < numDocs & j < values.length; j++) {
        _longValuesSV[j] = Math.max(_longValuesSV[j], values[j]);
      }
    }
    return _longValuesSV;
  }

  @Override
  public float[] transformToFloatValuesSV(ValueBlock valueBlock) {
    int numDocs = valueBlock.getNumDocs();
    if (_floatValuesSV == null) {
      _floatValuesSV = new float[numDocs];
    }
    float[] values = _arguments.get(0).transformToFloatValuesSV(valueBlock);
    System.arraycopy(values, 0, _floatValuesSV, 0, numDocs);
    for (int i = 1; i < _arguments.size(); i++) {
      values = _arguments.get(i).transformToFloatValuesSV(valueBlock);
      for (int j = 0; j < numDocs & j < values.length; j++) {
        _floatValuesSV[j] = Math.max(_floatValuesSV[j], values[j]);
      }
    }
    return _floatValuesSV;
  }

  @Override
  public double[] transformToDoubleValuesSV(ValueBlock valueBlock) {
    int numDocs = valueBlock.getNumDocs();
    if (_doubleValuesSV == null) {
      _doubleValuesSV = new double[numDocs];
    }
    double[] values = _arguments.get(0).transformToDoubleValuesSV(valueBlock);
    System.arraycopy(values, 0, _doubleValuesSV, 0, numDocs);
    for (int i = 1; i < _arguments.size(); i++) {
      values = _arguments.get(i).transformToDoubleValuesSV(valueBlock);
      for (int j = 0; j < numDocs & j < values.length; j++) {
        _doubleValuesSV[j] = Math.max(_doubleValuesSV[j], values[j]);
      }
    }
    return _doubleValuesSV;
  }

  @Override
  public BigDecimal[] transformToBigDecimalValuesSV(ValueBlock valueBlock) {
    int numDocs = valueBlock.getNumDocs();
    if (_bigDecimalValuesSV == null) {
      _bigDecimalValuesSV = new BigDecimal[numDocs];
    }
    BigDecimal[] values = _arguments.get(0).transformToBigDecimalValuesSV(valueBlock);
    System.arraycopy(values, 0, _bigDecimalValuesSV, 0, numDocs);
    for (int i = 1; i < _arguments.size(); i++) {
      values = _arguments.get(i).transformToBigDecimalValuesSV(valueBlock);
      for (int j = 0; j < numDocs & j < values.length; j++) {
        _bigDecimalValuesSV[j] = _bigDecimalValuesSV[j].max(values[j]);
      }
    }
    return _bigDecimalValuesSV;
  }

  @Override
  public String[] transformToStringValuesSV(ValueBlock valueBlock) {
    int numDocs = valueBlock.getNumDocs();
    if (_stringValuesSV == null) {
      _stringValuesSV = new String[numDocs];
    }
    String[] values = _arguments.get(0).transformToStringValuesSV(valueBlock);
    System.arraycopy(values, 0, _stringValuesSV, 0, numDocs);
    for (int i = 1; i < _arguments.size(); i++) {
      values = _arguments.get(i).transformToStringValuesSV(valueBlock);
      for (int j = 0; j < numDocs & j < values.length; j++) {
        if (_stringValuesSV[j].compareTo(values[j]) < 0) {
          _stringValuesSV[j] = values[j];
        }
      }
    }
    return _stringValuesSV;
  }
}
