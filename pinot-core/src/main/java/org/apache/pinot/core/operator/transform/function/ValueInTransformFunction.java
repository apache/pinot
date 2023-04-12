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

import it.unimi.dsi.fastutil.doubles.DoubleArrayList;
import it.unimi.dsi.fastutil.doubles.DoubleList;
import it.unimi.dsi.fastutil.doubles.DoubleOpenHashSet;
import it.unimi.dsi.fastutil.doubles.DoubleSet;
import it.unimi.dsi.fastutil.floats.FloatArrayList;
import it.unimi.dsi.fastutil.floats.FloatList;
import it.unimi.dsi.fastutil.floats.FloatOpenHashSet;
import it.unimi.dsi.fastutil.floats.FloatSet;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.longs.LongList;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import it.unimi.dsi.fastutil.longs.LongSet;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.pinot.core.operator.ColumnContext;
import org.apache.pinot.core.operator.blocks.ValueBlock;
import org.apache.pinot.core.operator.transform.TransformResultMetadata;
import org.apache.pinot.segment.spi.index.reader.Dictionary;
import org.apache.pinot.spi.data.FieldSpec.DataType;


public class ValueInTransformFunction extends BaseTransformFunction {
  public static final String FUNCTION_NAME = "valueIn";

  private TransformFunction _mainTransformFunction;
  private TransformResultMetadata _resultMetadata;
  private Dictionary _dictionary;

  private IntSet _dictIdSet;
  private int[][] _dictIdsMV;
  private IntSet _intValueSet;
  private LongSet _longValueSet;
  private FloatSet _floatValueSet;
  private DoubleSet _doubleValueSet;
  private Set<String> _stringValueSet;

  @Override
  public String getName() {
    return FUNCTION_NAME;
  }

  @Override
  public void init(List<TransformFunction> arguments, Map<String, ColumnContext> columnContextMap) {
    super.init(arguments, columnContextMap);
    // Check that there are more than 1 arguments
    int numArguments = arguments.size();
    if (numArguments < 2) {
      throw new IllegalArgumentException("At least 2 arguments are required for VALUE_IN transform function");
    }

    // Check that the first argument is a multi-valued column or transform function
    TransformFunction firstArgument = arguments.get(0);
    if (firstArgument instanceof LiteralTransformFunction || firstArgument.getResultMetadata().isSingleValue()) {
      throw new IllegalArgumentException(
          "The first argument of VALUE_IN transform function must be a multi-valued column or a transform function");
    }
    _mainTransformFunction = firstArgument;
    _resultMetadata = _mainTransformFunction.getResultMetadata();
    _dictionary = _mainTransformFunction.getDictionary();

    // Collect all values for the VALUE_IN transform function
    _stringValueSet = new HashSet<>(numArguments - 1);
    for (int i = 1; i < numArguments; i++) {
      _stringValueSet.add(((LiteralTransformFunction) arguments.get(i)).getStringLiteral());
    }
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
    int length = valueBlock.getNumDocs();
    if (_dictIdsMV == null || _dictIdsMV.length < length) {
      _dictIdsMV = new int[length][];
    }
    if (_dictIdSet == null) {
      _dictIdSet = new IntOpenHashSet();
      assert _dictionary != null;
      for (String inValue : _stringValueSet) {
        int dictId = _dictionary.indexOf(inValue);
        if (dictId >= 0) {
          _dictIdSet.add(dictId);
        }
      }
    }
    int[][] unFilteredDictIds = _mainTransformFunction.transformToDictIdsMV(valueBlock);
    for (int i = 0; i < length; i++) {
      _dictIdsMV[i] = filterInts(_dictIdSet, unFilteredDictIds[i]);
    }
    return _dictIdsMV;
  }

  @Override
  public int[][] transformToIntValuesMV(ValueBlock valueBlock) {
    if (_dictionary != null || _resultMetadata.getDataType().getStoredType() != DataType.INT) {
      return super.transformToIntValuesMV(valueBlock);
    }
    int length = valueBlock.getNumDocs();
    initIntValuesMV(length);
    if (_intValueSet == null) {
      _intValueSet = new IntOpenHashSet();
      for (String inValue : _stringValueSet) {
        _intValueSet.add(Integer.parseInt(inValue));
      }
    }
    int[][] unFilteredIntValues = _mainTransformFunction.transformToIntValuesMV(valueBlock);
    for (int i = 0; i < length; i++) {
      _intValuesMV[i] = filterInts(_intValueSet, unFilteredIntValues[i]);
    }
    return _intValuesMV;
  }

  @Override
  public long[][] transformToLongValuesMV(ValueBlock valueBlock) {
    if (_dictionary != null || _resultMetadata.getDataType().getStoredType() != DataType.LONG) {
      return super.transformToLongValuesMV(valueBlock);
    }
    int length = valueBlock.getNumDocs();
    initLongValuesMV(length);
    if (_longValueSet == null) {
      _longValueSet = new LongOpenHashSet();
      for (String inValue : _stringValueSet) {
        _longValueSet.add(Long.parseLong(inValue));
      }
    }
    long[][] unFilteredLongValues = _mainTransformFunction.transformToLongValuesMV(valueBlock);
    for (int i = 0; i < length; i++) {
      _longValuesMV[i] = filterLongs(_longValueSet, unFilteredLongValues[i]);
    }
    return _longValuesMV;
  }

  @Override
  public float[][] transformToFloatValuesMV(ValueBlock valueBlock) {
    if (_dictionary != null || _resultMetadata.getDataType().getStoredType() != DataType.FLOAT) {
      return super.transformToFloatValuesMV(valueBlock);
    }
    int length = valueBlock.getNumDocs();
    initFloatValuesMV(length);
    if (_floatValueSet == null) {
      _floatValueSet = new FloatOpenHashSet();
      for (String inValue : _stringValueSet) {
        _floatValueSet.add(Float.parseFloat(inValue));
      }
    }
    float[][] unFilteredFloatValues = _mainTransformFunction.transformToFloatValuesMV(valueBlock);
    for (int i = 0; i < length; i++) {
      _floatValuesMV[i] = filterFloats(_floatValueSet, unFilteredFloatValues[i]);
    }
    return _floatValuesMV;
  }

  @Override
  public double[][] transformToDoubleValuesMV(ValueBlock valueBlock) {
    if (_dictionary != null || _resultMetadata.getDataType().getStoredType() != DataType.DOUBLE) {
      return super.transformToDoubleValuesMV(valueBlock);
    }
    int length = valueBlock.getNumDocs();
    initDoubleValuesMV(length);
    if (_doubleValueSet == null) {
      _doubleValueSet = new DoubleOpenHashSet();
      for (String inValue : _stringValueSet) {
        _doubleValueSet.add(Double.parseDouble(inValue));
      }
    }
    double[][] unFilteredDoubleValues = _mainTransformFunction.transformToDoubleValuesMV(valueBlock);
    for (int i = 0; i < length; i++) {
      _doubleValuesMV[i] = filterDoubles(_doubleValueSet, unFilteredDoubleValues[i]);
    }
    return _doubleValuesMV;
  }

  @Override
  public String[][] transformToStringValuesMV(ValueBlock valueBlock) {
    if (_dictionary != null || _resultMetadata.getDataType().getStoredType() != DataType.STRING) {
      return super.transformToStringValuesMV(valueBlock);
    }
    int length = valueBlock.getNumDocs();
    initStringValuesMV(length);
    String[][] unFilteredStringValues = _mainTransformFunction.transformToStringValuesMV(valueBlock);
    for (int i = 0; i < length; i++) {
      _stringValuesMV[i] = filterStrings(_stringValueSet, unFilteredStringValues[i]);
    }
    return _stringValuesMV;
  }

  private static int[] filterInts(IntSet intSet, int[] source) {
    IntList intList = new IntArrayList();
    for (int value : source) {
      if (intSet.contains(value)) {
        intList.add(value);
      }
    }
    if (intList.size() == source.length) {
      return source;
    } else {
      return intList.toIntArray();
    }
  }

  private static long[] filterLongs(LongSet longSet, long[] source) {
    LongList longList = new LongArrayList();
    for (long value : source) {
      if (longSet.contains(value)) {
        longList.add(value);
      }
    }
    if (longList.size() == source.length) {
      return source;
    } else {
      return longList.toLongArray();
    }
  }

  private static float[] filterFloats(FloatSet floatSet, float[] source) {
    FloatList floatList = new FloatArrayList();
    for (float value : source) {
      if (floatSet.contains(value)) {
        floatList.add(value);
      }
    }
    if (floatList.size() == source.length) {
      return source;
    } else {
      return floatList.toFloatArray();
    }
  }

  private static double[] filterDoubles(DoubleSet doubleSet, double[] source) {
    DoubleList doubleList = new DoubleArrayList();
    for (double value : source) {
      if (doubleSet.contains(value)) {
        doubleList.add(value);
      }
    }
    if (doubleList.size() == source.length) {
      return source;
    } else {
      return doubleList.toDoubleArray();
    }
  }

  private static String[] filterStrings(Set<String> stringSet, String[] source) {
    List<String> stringList = new ArrayList<>();
    for (String value : source) {
      if (stringSet.contains(value)) {
        stringList.add(value);
      }
    }
    if (stringList.size() == source.length) {
      return source;
    } else {
      return stringList.toArray(new String[0]);
    }
  }
}
