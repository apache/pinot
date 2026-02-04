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

import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.pinot.core.operator.ColumnContext;
import org.apache.pinot.core.operator.blocks.ValueBlock;
import org.apache.pinot.core.operator.transform.TransformResultMetadata;
import org.apache.pinot.segment.spi.index.reader.Dictionary;
import org.apache.pinot.spi.data.FieldSpec.DataType;


/**
 * This class implements the evalMv function for multi-valued columns. It takes 2 arguments, where the first argument
 * is a multi-valued column, and the second argument is a predicate string. The transform function will filter the
 * values from the multi-valued column based on the predicate.
 */
public class EvalMvTransformFunction extends BaseTransformFunction {
  public static final String FUNCTION_NAME = "evalMv";

  private TransformFunction _mainTransformFunction;
  private TransformResultMetadata _resultMetadata;
  private Dictionary _dictionary;
  private DataType _dataType;
  private EvalMvPredicateEvaluator _predicateEvaluator;

  private int[][] _dictIdsMV;

  @Override
  public String getName() {
    return FUNCTION_NAME;
  }

  @Override
  public void init(List<TransformFunction> arguments, Map<String, ColumnContext> columnContextMap) {
    super.init(arguments, columnContextMap);
    int numArguments = arguments.size();
    if (numArguments != 2) {
      throw new IllegalArgumentException("Exactly 2 arguments are required for evalMv transform function");
    }

    TransformFunction firstArgument = arguments.get(0);
    if (firstArgument instanceof LiteralTransformFunction || firstArgument.getResultMetadata().isSingleValue()) {
      throw new IllegalArgumentException(
          "The first argument of evalMv transform function must be a multi-valued column or a transform function");
    }
    _mainTransformFunction = firstArgument;
    _resultMetadata = _mainTransformFunction.getResultMetadata();
    _dictionary = _mainTransformFunction.getDictionary();
    _dataType = _resultMetadata.getDataType();

    TransformFunction predicateArgument = arguments.get(1);
    if (!(predicateArgument instanceof LiteralTransformFunction) || !predicateArgument.getResultMetadata()
        .isSingleValue() || predicateArgument.getResultMetadata().getDataType() != DataType.STRING) {
      throw new IllegalArgumentException(
          "The second argument of evalMv transform function must be a single-valued string literal");
    }
    String predicate = ((LiteralTransformFunction) predicateArgument).getStringLiteral();
    String expectedColumn =
        firstArgument instanceof IdentifierTransformFunction
            ? ((IdentifierTransformFunction) firstArgument).getColumnName()
            : null;
    _predicateEvaluator = EvalMvPredicateEvaluator.forPredicate(predicate, _dataType, _dictionary, expectedColumn);
  }

  @Override
  public TransformResultMetadata getResultMetadata() {
    return _resultMetadata;
  }

  @Nullable
  @Override
  public Dictionary getDictionary() {
    return _dictionary;
  }

  @Override
  public int[][] transformToDictIdsMV(ValueBlock valueBlock) {
    if (_dictionary == null) {
      return super.transformToDictIdsMV(valueBlock);
    }
    int length = valueBlock.getNumDocs();
    if (_dictIdsMV == null || _dictIdsMV.length < length) {
      _dictIdsMV = new int[length][];
    }
    int[][] unfilteredDictIds = _mainTransformFunction.transformToDictIdsMV(valueBlock);
    for (int i = 0; i < length; i++) {
      _dictIdsMV[i] = filterDictIds(unfilteredDictIds[i]);
    }
    return _dictIdsMV;
  }

  @Override
  public int[][] transformToIntValuesMV(ValueBlock valueBlock) {
    if (_dictionary != null || _dataType.getStoredType() != DataType.INT) {
      return super.transformToIntValuesMV(valueBlock);
    }
    int length = valueBlock.getNumDocs();
    initIntValuesMV(length);
    int[][] unfilteredValues = _mainTransformFunction.transformToIntValuesMV(valueBlock);
    for (int i = 0; i < length; i++) {
      _intValuesMV[i] = filterInts(unfilteredValues[i]);
    }
    return _intValuesMV;
  }

  @Override
  public long[][] transformToLongValuesMV(ValueBlock valueBlock) {
    if (_dictionary != null || _dataType.getStoredType() != DataType.LONG) {
      return super.transformToLongValuesMV(valueBlock);
    }
    int length = valueBlock.getNumDocs();
    initLongValuesMV(length);
    long[][] unfilteredValues = _mainTransformFunction.transformToLongValuesMV(valueBlock);
    for (int i = 0; i < length; i++) {
      _longValuesMV[i] = filterLongs(unfilteredValues[i]);
    }
    return _longValuesMV;
  }

  @Override
  public float[][] transformToFloatValuesMV(ValueBlock valueBlock) {
    if (_dictionary != null || _dataType.getStoredType() != DataType.FLOAT) {
      return super.transformToFloatValuesMV(valueBlock);
    }
    int length = valueBlock.getNumDocs();
    initFloatValuesMV(length);
    float[][] unfilteredValues = _mainTransformFunction.transformToFloatValuesMV(valueBlock);
    for (int i = 0; i < length; i++) {
      _floatValuesMV[i] = filterFloats(unfilteredValues[i]);
    }
    return _floatValuesMV;
  }

  @Override
  public double[][] transformToDoubleValuesMV(ValueBlock valueBlock) {
    if (_dictionary != null || _dataType.getStoredType() != DataType.DOUBLE) {
      return super.transformToDoubleValuesMV(valueBlock);
    }
    int length = valueBlock.getNumDocs();
    initDoubleValuesMV(length);
    double[][] unfilteredValues = _mainTransformFunction.transformToDoubleValuesMV(valueBlock);
    for (int i = 0; i < length; i++) {
      _doubleValuesMV[i] = filterDoubles(unfilteredValues[i]);
    }
    return _doubleValuesMV;
  }

  @Override
  public String[][] transformToStringValuesMV(ValueBlock valueBlock) {
    if (_dictionary != null || _dataType.getStoredType() != DataType.STRING) {
      return super.transformToStringValuesMV(valueBlock);
    }
    int length = valueBlock.getNumDocs();
    initStringValuesMV(length);
    String[][] unfilteredValues = _mainTransformFunction.transformToStringValuesMV(valueBlock);
    for (int i = 0; i < length; i++) {
      _stringValuesMV[i] = filterStrings(unfilteredValues[i]);
    }
    return _stringValuesMV;
  }

  @Override
  public byte[][][] transformToBytesValuesMV(ValueBlock valueBlock) {
    if (_dictionary != null || _dataType.getStoredType() != DataType.BYTES) {
      return super.transformToBytesValuesMV(valueBlock);
    }
    int length = valueBlock.getNumDocs();
    initBytesValuesMV(length);
    byte[][][] unfilteredValues = _mainTransformFunction.transformToBytesValuesMV(valueBlock);
    for (int i = 0; i < length; i++) {
      _bytesValuesMV[i] = filterBytes(unfilteredValues[i]);
    }
    return _bytesValuesMV;
  }

  private int[] filterDictIds(int[] source) {
    int numValues = source.length;
    int count = 0;
    for (int value : source) {
      if (_predicateEvaluator.matchesDictId(value)) {
        count++;
      }
    }
    if (count == numValues) {
      return source;
    }
    int[] filtered = new int[count];
    int idx = 0;
    for (int value : source) {
      if (_predicateEvaluator.matchesDictId(value)) {
        filtered[idx++] = value;
      }
    }
    return filtered;
  }

  private int[] filterInts(int[] source) {
    int numValues = source.length;
    int count = 0;
    for (int value : source) {
      if (_predicateEvaluator.matchesInt(value)) {
        count++;
      }
    }
    if (count == numValues) {
      return source;
    }
    int[] filtered = new int[count];
    int idx = 0;
    for (int value : source) {
      if (_predicateEvaluator.matchesInt(value)) {
        filtered[idx++] = value;
      }
    }
    return filtered;
  }

  private long[] filterLongs(long[] source) {
    int numValues = source.length;
    int count = 0;
    for (long value : source) {
      if (_predicateEvaluator.matchesLong(value)) {
        count++;
      }
    }
    if (count == numValues) {
      return source;
    }
    long[] filtered = new long[count];
    int idx = 0;
    for (long value : source) {
      if (_predicateEvaluator.matchesLong(value)) {
        filtered[idx++] = value;
      }
    }
    return filtered;
  }

  private float[] filterFloats(float[] source) {
    int numValues = source.length;
    int count = 0;
    for (float value : source) {
      if (_predicateEvaluator.matchesFloat(value)) {
        count++;
      }
    }
    if (count == numValues) {
      return source;
    }
    float[] filtered = new float[count];
    int idx = 0;
    for (float value : source) {
      if (_predicateEvaluator.matchesFloat(value)) {
        filtered[idx++] = value;
      }
    }
    return filtered;
  }

  private double[] filterDoubles(double[] source) {
    int numValues = source.length;
    int count = 0;
    for (double value : source) {
      if (_predicateEvaluator.matchesDouble(value)) {
        count++;
      }
    }
    if (count == numValues) {
      return source;
    }
    double[] filtered = new double[count];
    int idx = 0;
    for (double value : source) {
      if (_predicateEvaluator.matchesDouble(value)) {
        filtered[idx++] = value;
      }
    }
    return filtered;
  }

  private String[] filterStrings(String[] source) {
    int numValues = source.length;
    int count = 0;
    for (String value : source) {
      if (_predicateEvaluator.matchesString(value)) {
        count++;
      }
    }
    if (count == numValues) {
      return source;
    }
    String[] filtered = new String[count];
    int idx = 0;
    for (String value : source) {
      if (_predicateEvaluator.matchesString(value)) {
        filtered[idx++] = value;
      }
    }
    return filtered;
  }

  private byte[][] filterBytes(byte[][] source) {
    int numValues = source.length;
    int count = 0;
    for (byte[] value : source) {
      if (_predicateEvaluator.matchesBytes(value)) {
        count++;
      }
    }
    if (count == numValues) {
      return source;
    }
    byte[][] filtered = new byte[count][];
    int idx = 0;
    for (byte[] value : source) {
      if (_predicateEvaluator.matchesBytes(value)) {
        filtered[idx++] = value;
      }
    }
    return filtered;
  }
}
