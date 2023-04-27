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
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pinot.common.function.TransformFunctionType;
import org.apache.pinot.core.operator.blocks.ValueBlock;
import org.roaringbitmap.RoaringBitmap;

/**
 * The <code>GreatestTransformFunction</code> implements the Greatest operator.
 *
 * Return the greatest results for the arguments
 *
 * Expected result:
 * greatest(columnA, columnB, columnC): largest among columnA, columnB, columnC
 *
 * Note that null values will be ignored for evaluation. If all values are null, we return null.
 */
public class GreatestTransformFunction extends SelectTupleElementTransformFunction {

  public GreatestTransformFunction() {
    super(TransformFunctionType.GREATEST.getName());
  }

  @Override
  public int[] transformToIntValuesSV(ValueBlock valueBlock) {
    int numDocs = valueBlock.getNumDocs();
    initIntValuesSV(numDocs);
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
  public Pair<int[], RoaringBitmap> transformToIntValuesSVWithNull(ValueBlock valueBlock) {
    int numDocs = valueBlock.getNumDocs();
    initIntValuesSV(numDocs);
    Pair<int[], RoaringBitmap> values = _arguments.get(0).transformToIntValuesSVWithNull(valueBlock);
    int[] curValues = values.getLeft();
    System.arraycopy(curValues, 0, _intValuesSV, 0, numDocs);
    RoaringBitmap nullBitmap = values.getRight();
    for (int i = 1; i < _arguments.size(); i++) {
      values = _arguments.get(i).transformToIntValuesSVWithNull(valueBlock);
      RoaringBitmap curNull = values.getRight();
      for (int j = 0; j < numDocs & j < values.getLeft().length; j++) {
        // If current value is not null, we process the data.
        if (curNull == null || !curNull.contains(j)) {
          // If existing maximum value is null, we set the value directly.
          if (nullBitmap != null && nullBitmap.contains(j)) {
            _intValuesSV[j] = values.getLeft()[j];
          } else {
            _intValuesSV[j] = Math.max(_intValuesSV[j], values.getLeft()[j]);
          }
        }
      }
      if (nullBitmap != null && curNull != null) {
        nullBitmap.and(curNull);
      } else {
        nullBitmap = null;
      }
    }
    return ImmutablePair.of(_intValuesSV, nullBitmap);
  }

  @Override
  public long[] transformToLongValuesSV(ValueBlock valueBlock) {
    int numDocs = valueBlock.getNumDocs();
    initLongValuesSV(numDocs);
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
  public Pair<long[], RoaringBitmap> transformToLongValuesSVWithNull(ValueBlock valueBlock) {
    int numDocs = valueBlock.getNumDocs();
    initLongValuesSV(numDocs);
    Pair<long[], RoaringBitmap> values = _arguments.get(0).transformToLongValuesSVWithNull(valueBlock);
    long[] curValues = values.getLeft();
    System.arraycopy(curValues, 0, _longValuesSV, 0, numDocs);
    RoaringBitmap nullBitmap = values.getRight();
    for (int i = 1; i < _arguments.size(); i++) {
      values = _arguments.get(i).transformToLongValuesSVWithNull(valueBlock);
      RoaringBitmap curNull = values.getRight();
      for (int j = 0; j < numDocs & j < values.getLeft().length; j++) {
        // If current value is not null, we process the data.
        if (curNull == null || !curNull.contains(j)) {
          // If existing maximum value is null, we set the value directly.
          if (nullBitmap != null && nullBitmap.contains(j)) {
            _longValuesSV[j] = values.getLeft()[j];
          } else {
            _longValuesSV[j] = Math.max(_longValuesSV[j], values.getLeft()[j]);
          }
        }
      }
      if (nullBitmap != null && curNull != null) {
        nullBitmap.and(curNull);
      } else {
        nullBitmap = null;
      }
    }
    return ImmutablePair.of(_longValuesSV, nullBitmap);
  }

  @Override
  public float[] transformToFloatValuesSV(ValueBlock valueBlock) {
    int numDocs = valueBlock.getNumDocs();
    initFloatValuesSV(numDocs);
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
  public Pair<float[], RoaringBitmap> transformToFloatValuesSVWithNull(ValueBlock valueBlock) {
    int numDocs = valueBlock.getNumDocs();
    initFloatValuesSV(numDocs);
    Pair<float[], RoaringBitmap> values = _arguments.get(0).transformToFloatValuesSVWithNull(valueBlock);
    float[] curValues = values.getLeft();
    System.arraycopy(curValues, 0, _floatValuesSV, 0, numDocs);
    RoaringBitmap nullBitmap = values.getRight();
    for (int i = 1; i < _arguments.size(); i++) {
      values = _arguments.get(i).transformToFloatValuesSVWithNull(valueBlock);
      RoaringBitmap curNull = values.getRight();
      for (int j = 0; j < numDocs & j < values.getLeft().length; j++) {
        // If current value is not null, we process the data.
        if (curNull != null || !curNull.contains(j)) {
          // If existing maximum value is null, we set the value directly.
          if (nullBitmap != null && nullBitmap.contains(j)) {
            _floatValuesSV[j] = values.getLeft()[j];
          } else {
            _floatValuesSV[j] = Math.max(_floatValuesSV[j], values.getLeft()[j]);
          }
        }
      }
      if (nullBitmap != null && curNull != null) {
        nullBitmap.and(curNull);
      } else {
        nullBitmap = null;
      }
    }
    return ImmutablePair.of(_floatValuesSV, nullBitmap);
  }

  @Override
  public double[] transformToDoubleValuesSV(ValueBlock valueBlock) {
    int numDocs = valueBlock.getNumDocs();
    initDoubleValuesSV(numDocs);
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
  public Pair<double[], RoaringBitmap> transformToDoubleValuesSVWithNull(ValueBlock valueBlock) {
    int numDocs = valueBlock.getNumDocs();
    initDoubleValuesSV(numDocs);
    Pair<double[], RoaringBitmap> values = _arguments.get(0).transformToDoubleValuesSVWithNull(valueBlock);
    double[] curValues = values.getLeft();
    System.arraycopy(curValues, 0, _doubleValuesSV, 0, numDocs);
    RoaringBitmap nullBitmap = values.getRight();
    for (int i = 1; i < _arguments.size(); i++) {
      values = _arguments.get(i).transformToDoubleValuesSVWithNull(valueBlock);
      RoaringBitmap curNull = values.getRight();
      for (int j = 0; j < numDocs & j < values.getLeft().length; j++) {
        // If current value is not null, we process the data.
        if (curNull == null || !curNull.contains(j)) {
          // If existing maximum value is null, we set the value directly.
          if (nullBitmap != null && nullBitmap.contains(j)) {
            _doubleValuesSV[j] = values.getLeft()[j];
          } else {
            _doubleValuesSV[j] = Math.max(_doubleValuesSV[j], values.getLeft()[j]);
          }
        }
      }
      if (nullBitmap != null && curNull != null) {
        nullBitmap.and(curNull);
      } else {
        nullBitmap = null;
      }
    }
    return ImmutablePair.of(_doubleValuesSV, nullBitmap);
  }

  @Override
  public BigDecimal[] transformToBigDecimalValuesSV(ValueBlock valueBlock) {
    int numDocs = valueBlock.getNumDocs();
    initBigDecimalValuesSV(numDocs);
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
  public Pair<BigDecimal[], RoaringBitmap> transformToBigDecimalValuesSVWithNull(ValueBlock valueBlock) {
    int numDocs = valueBlock.getNumDocs();
    initBigDecimalValuesSV(numDocs);
    Pair<BigDecimal[], RoaringBitmap> values = _arguments.get(0).transformToBigDecimalValuesSVWithNull(valueBlock);
    BigDecimal[] curValues = values.getLeft();
    System.arraycopy(curValues, 0, _bigDecimalValuesSV, 0, numDocs);
    RoaringBitmap nullBitmap = values.getRight();
    for (int i = 1; i < _arguments.size(); i++) {
      values = _arguments.get(i).transformToBigDecimalValuesSVWithNull(valueBlock);
      RoaringBitmap curNull = values.getRight();
      for (int j = 0; j < numDocs & j < values.getLeft().length; j++) {
        // If current value is not null, we process the data.
        if (curNull == null || !curNull.contains(j)) {
          // If existing maximum value is null, we set the value directly.
          if (nullBitmap != null && nullBitmap.contains(j)) {
            _bigDecimalValuesSV[j] = values.getLeft()[j];
          } else {
            _bigDecimalValuesSV[j] = _bigDecimalValuesSV[j].max(values.getLeft()[j]);
          }
        }
      }
      if (nullBitmap != null && curNull != null) {
        nullBitmap.and(curNull);
      } else {
        nullBitmap = null;
      }
    }
    return ImmutablePair.of(_bigDecimalValuesSV, nullBitmap);
  }

  @Override
  public String[] transformToStringValuesSV(ValueBlock valueBlock) {
    int numDocs = valueBlock.getNumDocs();
    initStringValuesSV(numDocs);
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

  @Override
  public Pair<String[], RoaringBitmap> transformToStringValuesSVWithNull(ValueBlock valueBlock) {
    int numDocs = valueBlock.getNumDocs();
    initStringValuesSV(numDocs);
    Pair<String[], RoaringBitmap> values = _arguments.get(0).transformToStringValuesSVWithNull(valueBlock);
    String[] curValues = values.getLeft();
    System.arraycopy(curValues, 0, _stringValuesSV, 0, numDocs);
    RoaringBitmap nullBitmap = values.getRight();
    for (int i = 1; i < _arguments.size(); i++) {
      values = _arguments.get(i).transformToStringValuesSVWithNull(valueBlock);
      RoaringBitmap curNull = values.getRight();
      for (int j = 0; j < numDocs & j < values.getLeft().length; j++) {
        // If current value is not null, we process the data.
        if (curNull == null || !curNull.contains(j)) {
          // If existing maximum value is null, we set the value directly.
          if (nullBitmap != null && nullBitmap.contains(j)) {
            _stringValuesSV[j] = values.getLeft()[j];
          } else if (_stringValuesSV[j].compareTo(values.getLeft()[j]) < 0) {
            _stringValuesSV[j] = values.getLeft()[j];
          }
        }
      }
      if (nullBitmap != null && curNull != null) {
        nullBitmap.and(curNull);
      } else {
        nullBitmap = null;
      }
    }
    return ImmutablePair.of(_stringValuesSV, nullBitmap);
  }
}
