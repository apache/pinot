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
import java.util.EnumMap;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import org.apache.pinot.core.operator.ColumnContext;
import org.apache.pinot.core.operator.blocks.ValueBlock;
import org.apache.pinot.core.operator.transform.TransformResultMetadata;
import org.apache.pinot.spi.data.FieldSpec;
import org.roaringbitmap.RoaringBitmap;


public abstract class SelectTupleElementTransformFunction
    extends ComputeDifferentlyWhenNullHandlingEnabledTransformFunction {

  private static final EnumSet<FieldSpec.DataType> SUPPORTED_DATATYPES = EnumSet.of(FieldSpec.DataType.INT,
      FieldSpec.DataType.LONG, FieldSpec.DataType.FLOAT, FieldSpec.DataType.DOUBLE, FieldSpec.DataType.BIG_DECIMAL,
      FieldSpec.DataType.TIMESTAMP, FieldSpec.DataType.STRING, FieldSpec.DataType.UNKNOWN);

  private static final EnumMap<FieldSpec.DataType, EnumSet<FieldSpec.DataType>> ACCEPTABLE_COMBINATIONS =
      createAcceptableCombinations();

  private final String _name;

  protected List<TransformFunction> _arguments;
  private TransformResultMetadata _resultMetadata;

  public SelectTupleElementTransformFunction(String name) {
    _name = name;
  }

  @Override
  public void init(List<TransformFunction> arguments, Map<String, ColumnContext> columnContextMap,
      boolean nullHandlingEnabled) {
    super.init(arguments, columnContextMap, nullHandlingEnabled);
    if (arguments.isEmpty()) {
      throw new IllegalArgumentException(_name + " takes at least one argument");
    }
    FieldSpec.DataType dataType = null;
    for (int i = 0; i < arguments.size(); i++) {
      TransformFunction argument = arguments.get(i);
      TransformResultMetadata metadata = argument.getResultMetadata();
      if (!metadata.isSingleValue()) {
        throw new IllegalArgumentException(argument.getName() + " at position " + i + " is not single value");
      }
      FieldSpec.DataType argumentType = metadata.getDataType();
      if (!SUPPORTED_DATATYPES.contains(argumentType)) {
        throw new IllegalArgumentException(argumentType + " not supported. Required one of " + SUPPORTED_DATATYPES);
      }
      if (dataType == null) {
        dataType = argumentType;
      } else if (dataType.isUnknown() || argumentType.isUnknown() || ACCEPTABLE_COMBINATIONS.get(dataType)
          .contains(argumentType)) {
        dataType = getLowestCommonDenominatorType(dataType, argumentType);
      } else {
        throw new IllegalArgumentException(
            "combination " + argumentType + " not supported. Required one of " + ACCEPTABLE_COMBINATIONS.get(dataType));
      }
    }
    _resultMetadata = new TransformResultMetadata(dataType, true, false);
    _arguments = arguments;
  }

  @Override
  public TransformResultMetadata getResultMetadata() {
    return _resultMetadata;
  }

  @Override
  public String getName() {
    return _name;
  }

  @Override
  public RoaringBitmap getNullBitmap(ValueBlock valueBlock) {
    RoaringBitmap bitmap = _arguments.get(0).getNullBitmap(valueBlock);
    if (bitmap == null || bitmap.isEmpty()) {
      return bitmap;
    }
    for (int i = 1; i < _arguments.size(); i++) {
      RoaringBitmap curBitmap = _arguments.get(i).getNullBitmap(valueBlock);
      if (curBitmap == null || curBitmap.isEmpty()) {
        return curBitmap;
      }
      bitmap.and(curBitmap);
      if (bitmap.isEmpty()) {
        return null;
      }
    }
    return bitmap;
  }

  private static FieldSpec.DataType getLowestCommonDenominatorType(FieldSpec.DataType left, FieldSpec.DataType right) {
    if (left == null || left == right) {
      return right;
    }
    if (left == FieldSpec.DataType.BIG_DECIMAL || right == FieldSpec.DataType.BIG_DECIMAL) {
      return FieldSpec.DataType.BIG_DECIMAL;
    }
    if (left == FieldSpec.DataType.DOUBLE || left == FieldSpec.DataType.FLOAT || right == FieldSpec.DataType.DOUBLE
        || right == FieldSpec.DataType.FLOAT) {
      return FieldSpec.DataType.DOUBLE;
    }
    return FieldSpec.DataType.LONG;
  }

  private static EnumMap<FieldSpec.DataType, EnumSet<FieldSpec.DataType>> createAcceptableCombinations() {
    EnumMap<FieldSpec.DataType, EnumSet<FieldSpec.DataType>> combinations = new EnumMap<>(FieldSpec.DataType.class);
    EnumSet<FieldSpec.DataType> numericTypes = EnumSet.of(FieldSpec.DataType.INT, FieldSpec.DataType.LONG,
        FieldSpec.DataType.FLOAT, FieldSpec.DataType.DOUBLE, FieldSpec.DataType.BIG_DECIMAL);
    for (FieldSpec.DataType numericType : numericTypes) {
      combinations.put(numericType, numericTypes);
    }
    combinations.put(FieldSpec.DataType.TIMESTAMP, EnumSet.of(FieldSpec.DataType.TIMESTAMP));
    combinations.put(FieldSpec.DataType.STRING, EnumSet.of(FieldSpec.DataType.STRING));
    return combinations;
  }

  @Override
  protected int[] transformToIntValuesSVNullHandlingDisabled(ValueBlock valueBlock) {
    int numDocs = valueBlock.getNumDocs();
    initIntValuesSV(numDocs);
    int[] values = _arguments.get(0).transformToIntValuesSV(valueBlock);
    System.arraycopy(values, 0, _intValuesSV, 0, numDocs);
    for (int i = 1; i < _arguments.size(); i++) {
      values = _arguments.get(i).transformToIntValuesSV(valueBlock);
      for (int j = 0; j < numDocs & j < values.length; j++) {
        _intValuesSV[j] = binaryFunction(_intValuesSV[j], values[j]);
      }
    }
    return _intValuesSV;
  }

  @Override
  protected int[] transformToIntValuesSVNullHandlingEnabled(ValueBlock valueBlock) {
    int numDocs = valueBlock.getNumDocs();
    initIntValuesSV(numDocs);
    int[] curValues = _arguments.get(0).transformToIntValuesSV(valueBlock);
    System.arraycopy(curValues, 0, _intValuesSV, 0, numDocs);
    RoaringBitmap nullBitmap = _arguments.get(0).getNullBitmap(valueBlock);
    for (int i = 1; i < _arguments.size(); i++) {
      curValues = _arguments.get(i).transformToIntValuesSV(valueBlock);
      RoaringBitmap curNull = _arguments.get(i).getNullBitmap(valueBlock);
      for (int j = 0; j < numDocs & j < curValues.length; j++) {
        // If current value is not null, we process the data.
        if (curNull == null || !curNull.contains(j)) {
          // If existing maximum value is null, we set the value directly.
          if (nullBitmap != null && nullBitmap.contains(j)) {
            _intValuesSV[j] = curValues[j];
          } else {
            _intValuesSV[j] = binaryFunction(_intValuesSV[j], curValues[j]);
          }
        }
      }
      if (nullBitmap != null && curNull != null) {
        nullBitmap.and(curNull);
      } else {
        nullBitmap = null;
      }
    }
    return _intValuesSV;
  }

  abstract protected int binaryFunction(int a, int b);

  @Override
  protected long[] transformToLongValuesSVNullHandlingDisabled(ValueBlock valueBlock) {
    int numDocs = valueBlock.getNumDocs();
    initLongValuesSV(numDocs);
    long[] values = _arguments.get(0).transformToLongValuesSV(valueBlock);
    System.arraycopy(values, 0, _longValuesSV, 0, numDocs);
    for (int i = 1; i < _arguments.size(); i++) {
      values = _arguments.get(i).transformToLongValuesSV(valueBlock);
      for (int j = 0; j < numDocs & j < values.length; j++) {
        _longValuesSV[j] = binaryFunction(_longValuesSV[j], values[j]);
      }
    }
    return _longValuesSV;
  }

  @Override
  protected long[] transformToLongValuesSVNullHandlingEnabled(ValueBlock valueBlock) {
    int numDocs = valueBlock.getNumDocs();
    initLongValuesSV(numDocs);
    long[] curValues = _arguments.get(0).transformToLongValuesSV(valueBlock);
    System.arraycopy(curValues, 0, _longValuesSV, 0, numDocs);
    RoaringBitmap nullBitmap = _arguments.get(0).getNullBitmap(valueBlock);
    for (int i = 1; i < _arguments.size(); i++) {
      curValues = _arguments.get(i).transformToLongValuesSV(valueBlock);
      RoaringBitmap curNull = _arguments.get(i).getNullBitmap(valueBlock);
      for (int j = 0; j < numDocs & j < curValues.length; j++) {
        // If current value is not null, we process the data.
        if (curNull == null || !curNull.contains(j)) {
          // If existing maximum value is null, we set the value directly.
          if (nullBitmap != null && nullBitmap.contains(j)) {
            _longValuesSV[j] = curValues[j];
          } else {
            _longValuesSV[j] = binaryFunction(_longValuesSV[j], curValues[j]);
          }
        }
      }
      if (nullBitmap != null && curNull != null) {
        nullBitmap.and(curNull);
      } else {
        nullBitmap = null;
      }
    }
    return _longValuesSV;
  }

  abstract protected long binaryFunction(long a, long b);

  @Override
  protected float[] transformToFloatValuesSVNullHandlingDisabled(ValueBlock valueBlock) {
    int numDocs = valueBlock.getNumDocs();
    initFloatValuesSV(numDocs);
    float[] values = _arguments.get(0).transformToFloatValuesSV(valueBlock);
    System.arraycopy(values, 0, _floatValuesSV, 0, numDocs);
    for (int i = 1; i < _arguments.size(); i++) {
      values = _arguments.get(i).transformToFloatValuesSV(valueBlock);
      for (int j = 0; j < numDocs & j < values.length; j++) {
        _floatValuesSV[j] = binaryFunction(_floatValuesSV[j], values[j]);
      }
    }
    return _floatValuesSV;
  }

  @Override
  protected float[] transformToFloatValuesSVNullHandlingEnabled(ValueBlock valueBlock) {
    int numDocs = valueBlock.getNumDocs();
    initFloatValuesSV(numDocs);
    float[] curValues = _arguments.get(0).transformToFloatValuesSV(valueBlock);
    System.arraycopy(curValues, 0, _floatValuesSV, 0, numDocs);
    RoaringBitmap nullBitmap = _arguments.get(0).getNullBitmap(valueBlock);
    for (int i = 1; i < _arguments.size(); i++) {
      curValues = _arguments.get(i).transformToFloatValuesSV(valueBlock);
      RoaringBitmap curNull = _arguments.get(i).getNullBitmap(valueBlock);
      for (int j = 0; j < numDocs & j < curValues.length; j++) {
        // If current value is not null, we process the data.
        if (curNull == null || !curNull.contains(j)) {
          // If existing maximum value is null, we set the value directly.
          if (nullBitmap != null && nullBitmap.contains(j)) {
            _floatValuesSV[j] = curValues[j];
          } else {
            _floatValuesSV[j] = binaryFunction(_floatValuesSV[j], curValues[j]);
          }
        }
      }
      if (nullBitmap != null && curNull != null) {
        nullBitmap.and(curNull);
      } else {
        nullBitmap = null;
      }
    }
    return _floatValuesSV;
  }

  abstract protected float binaryFunction(float a, float b);

  @Override
  protected double[] transformToDoubleValuesSVNullHandlingDisabled(ValueBlock valueBlock) {
    int numDocs = valueBlock.getNumDocs();
    initDoubleValuesSV(numDocs);
    double[] values = _arguments.get(0).transformToDoubleValuesSV(valueBlock);
    System.arraycopy(values, 0, _doubleValuesSV, 0, numDocs);
    for (int i = 1; i < _arguments.size(); i++) {
      values = _arguments.get(i).transformToDoubleValuesSV(valueBlock);
      for (int j = 0; j < numDocs & j < values.length; j++) {
        _doubleValuesSV[j] = binaryFunction(_doubleValuesSV[j], values[j]);
      }
    }
    return _doubleValuesSV;
  }

  @Override
  protected double[] transformToDoubleValuesSVNullHandlingEnabled(ValueBlock valueBlock) {
    int numDocs = valueBlock.getNumDocs();
    initDoubleValuesSV(numDocs);
    double[] curValues = _arguments.get(0).transformToDoubleValuesSV(valueBlock);
    System.arraycopy(curValues, 0, _doubleValuesSV, 0, numDocs);
    RoaringBitmap nullBitmap = _arguments.get(0).getNullBitmap(valueBlock);
    for (int i = 1; i < _arguments.size(); i++) {
      curValues = _arguments.get(i).transformToDoubleValuesSV(valueBlock);
      RoaringBitmap curNull = _arguments.get(i).getNullBitmap(valueBlock);
      for (int j = 0; j < numDocs & j < curValues.length; j++) {
        // If current value is not null, we process the data.
        if (curNull == null || !curNull.contains(j)) {
          // If existing maximum value is null, we set the value directly.
          if (nullBitmap != null && nullBitmap.contains(j)) {
            _doubleValuesSV[j] = curValues[j];
          } else {
            _doubleValuesSV[j] = binaryFunction(_doubleValuesSV[j], curValues[j]);
          }
        }
      }
      if (nullBitmap != null && curNull != null) {
        nullBitmap.and(curNull);
      } else {
        nullBitmap = null;
      }
    }
    return _doubleValuesSV;
  }

  abstract protected double binaryFunction(double a, double b);

  @Override
  protected BigDecimal[] transformToBigDecimalValuesSVNullHandlingDisabled(ValueBlock valueBlock) {
    int numDocs = valueBlock.getNumDocs();
    initBigDecimalValuesSV(numDocs);
    BigDecimal[] values = _arguments.get(0).transformToBigDecimalValuesSV(valueBlock);
    System.arraycopy(values, 0, _bigDecimalValuesSV, 0, numDocs);
    for (int i = 1; i < _arguments.size(); i++) {
      values = _arguments.get(i).transformToBigDecimalValuesSV(valueBlock);
      for (int j = 0; j < numDocs & j < values.length; j++) {
        _bigDecimalValuesSV[j] = binaryFunction(_bigDecimalValuesSV[j], values[j]);
      }
    }
    return _bigDecimalValuesSV;
  }

  @Override
  protected BigDecimal[] transformToBigDecimalValuesSVNullHandlingEnabled(ValueBlock valueBlock) {
    int numDocs = valueBlock.getNumDocs();
    initBigDecimalValuesSV(numDocs);
    BigDecimal[] curValues = _arguments.get(0).transformToBigDecimalValuesSV(valueBlock);
    System.arraycopy(curValues, 0, _bigDecimalValuesSV, 0, numDocs);
    RoaringBitmap nullBitmap = _arguments.get(0).getNullBitmap(valueBlock);
    for (int i = 1; i < _arguments.size(); i++) {
      curValues = _arguments.get(i).transformToBigDecimalValuesSV(valueBlock);
      RoaringBitmap curNull = _arguments.get(i).getNullBitmap(valueBlock);
      for (int j = 0; j < numDocs & j < curValues.length; j++) {
        // If current value is not null, we process the data.
        if (curNull == null || !curNull.contains(j)) {
          // If existing maximum value is null, we set the value directly.
          if (nullBitmap != null && nullBitmap.contains(j)) {
            _bigDecimalValuesSV[j] = curValues[j];
          } else {
            _bigDecimalValuesSV[j] = binaryFunction(_bigDecimalValuesSV[j], (curValues[j]));
          }
        }
      }
      if (nullBitmap != null && curNull != null) {
        nullBitmap.and(curNull);
      } else {
        nullBitmap = null;
      }
    }
    return _bigDecimalValuesSV;
  }

  abstract protected BigDecimal binaryFunction(BigDecimal a, BigDecimal b);

  @Override
  protected String[] transformToStringValuesSVNullHandlingDisabled(ValueBlock valueBlock) {
    int numDocs = valueBlock.getNumDocs();
    initStringValuesSV(numDocs);
    String[] values = _arguments.get(0).transformToStringValuesSV(valueBlock);
    System.arraycopy(values, 0, _stringValuesSV, 0, numDocs);
    for (int i = 1; i < _arguments.size(); i++) {
      values = _arguments.get(i).transformToStringValuesSV(valueBlock);
      for (int j = 0; j < numDocs & j < values.length; j++) {
        _stringValuesSV[j] = binaryFunction(_stringValuesSV[j], (values[j]));
      }
    }
    return _stringValuesSV;
  }

  @Override
  protected String[] transformToStringValuesSVNullHandlingEnabled(ValueBlock valueBlock) {
    int numDocs = valueBlock.getNumDocs();
    initStringValuesSV(numDocs);
    String[] curValues = _arguments.get(0).transformToStringValuesSV(valueBlock);
    System.arraycopy(curValues, 0, _stringValuesSV, 0, numDocs);
    RoaringBitmap nullBitmap = _arguments.get(0).getNullBitmap(valueBlock);
    for (int i = 1; i < _arguments.size(); i++) {
      curValues = _arguments.get(i).transformToStringValuesSV(valueBlock);
      RoaringBitmap curNull = _arguments.get(i).getNullBitmap(valueBlock);
      for (int j = 0; j < numDocs & j < curValues.length; j++) {
        // If current value is not null, we process the data.
        if (curNull == null || !curNull.contains(j)) {
          // If existing maximum value is null, we set the value directly.
          if (nullBitmap != null && nullBitmap.contains(j)) {
            _stringValuesSV[j] = curValues[j];
          } else {
            _stringValuesSV[j] = binaryFunction(_stringValuesSV[j], (curValues[j]));
          }
        }
      }
      if (nullBitmap != null && curNull != null) {
        nullBitmap.and(curNull);
      } else {
        nullBitmap = null;
      }
    }
    return _stringValuesSV;
  }

  abstract protected String binaryFunction(String a, String b);
}
