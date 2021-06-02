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
import org.apache.pinot.core.operator.blocks.ProjectionBlock;
import org.apache.pinot.core.operator.transform.TransformResultMetadata;
import org.apache.pinot.core.plan.DocIdSetPlanNode;
import org.apache.pinot.segment.spi.datasource.DataSource;
import org.apache.pinot.segment.spi.index.reader.Dictionary;
import org.apache.pinot.spi.data.FieldSpec;


public class ValueInTransformFunction extends BaseTransformFunction {
  public static final String FUNCTION_NAME = "valueIn";

  private TransformFunction _mainTransformFunction;
  private TransformResultMetadata _resultMetadata;
  private Dictionary _dictionary;

  private IntSet _dictIdSet;
  private int[][] _dictIds;
  private IntSet _intValueSet;
  private int[][] _intValues;
  private LongSet _longValueSet;
  private long[][] _longValues;
  private FloatSet _floatValueSet;
  private float[][] _floatValues;
  private DoubleSet _doubleValueSet;
  private double[][] _doubleValues;
  private Set<String> _stringValueSet;
  private String[][] _stringValues;

  @Override
  public String getName() {
    return FUNCTION_NAME;
  }

  @Override
  public void init(List<TransformFunction> arguments, Map<String, DataSource> dataSourceMap) {
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
      _stringValueSet.add(((LiteralTransformFunction) arguments.get(i)).getLiteral());
    }
  }

  @Override
  public TransformResultMetadata getResultMetadata() {
    return _resultMetadata;
  }

  @Override
  public Dictionary getDictionary() {
    return _dictionary;
  }

  @Override
  public int[][] transformToDictIdsMV(ProjectionBlock projectionBlock) {
    if (_dictIdSet == null) {
      _dictIdSet = new IntOpenHashSet();
      assert _dictionary != null;
      for (String inValue : _stringValueSet) {
        int dictId = _dictionary.indexOf(inValue);
        if (dictId >= 0) {
          _dictIdSet.add(dictId);
        }
      }
      _dictIds = new int[DocIdSetPlanNode.MAX_DOC_PER_CALL][];
    }
    int[][] unFilteredDictIds = _mainTransformFunction.transformToDictIdsMV(projectionBlock);
    int length = projectionBlock.getNumDocs();
    for (int i = 0; i < length; i++) {
      _dictIds[i] = filterInts(_dictIdSet, unFilteredDictIds[i]);
    }
    return _dictIds;
  }

  @Override
  public int[][] transformToIntValuesMV(ProjectionBlock projectionBlock) {
    if (_dictionary != null || _resultMetadata.getDataType() != FieldSpec.DataType.INT) {
      return super.transformToIntValuesMV(projectionBlock);
    }

    if (_intValueSet == null) {
      _intValueSet = new IntOpenHashSet();
      for (String inValue : _stringValueSet) {
        _intValueSet.add(Integer.parseInt(inValue));
      }
      _intValues = new int[DocIdSetPlanNode.MAX_DOC_PER_CALL][];
    }
    int[][] unFilteredIntValues = _mainTransformFunction.transformToIntValuesMV(projectionBlock);
    int length = projectionBlock.getNumDocs();
    for (int i = 0; i < length; i++) {
      _intValues[i] = filterInts(_intValueSet, unFilteredIntValues[i]);
    }
    return _intValues;
  }

  @Override
  public long[][] transformToLongValuesMV(ProjectionBlock projectionBlock) {
    if (_dictionary != null || _resultMetadata.getDataType() != FieldSpec.DataType.LONG) {
      return super.transformToLongValuesMV(projectionBlock);
    }

    if (_longValueSet == null) {
      _longValueSet = new LongOpenHashSet();
      for (String inValue : _stringValueSet) {
        _longValueSet.add(Long.parseLong(inValue));
      }
      _longValues = new long[DocIdSetPlanNode.MAX_DOC_PER_CALL][];
    }
    long[][] unFilteredLongValues = _mainTransformFunction.transformToLongValuesMV(projectionBlock);
    int length = projectionBlock.getNumDocs();
    for (int i = 0; i < length; i++) {
      _longValues[i] = filterLongs(_longValueSet, unFilteredLongValues[i]);
    }
    return _longValues;
  }

  @Override
  public float[][] transformToFloatValuesMV(ProjectionBlock projectionBlock) {
    if (_dictionary != null || _resultMetadata.getDataType() != FieldSpec.DataType.FLOAT) {
      return super.transformToFloatValuesMV(projectionBlock);
    }

    if (_floatValueSet == null) {
      _floatValueSet = new FloatOpenHashSet();
      for (String inValue : _stringValueSet) {
        _floatValueSet.add(Float.parseFloat(inValue));
      }
      _floatValues = new float[DocIdSetPlanNode.MAX_DOC_PER_CALL][];
    }
    float[][] unFilteredFloatValues = _mainTransformFunction.transformToFloatValuesMV(projectionBlock);
    int length = projectionBlock.getNumDocs();
    for (int i = 0; i < length; i++) {
      _floatValues[i] = filterFloats(_floatValueSet, unFilteredFloatValues[i]);
    }
    return _floatValues;
  }

  @Override
  public double[][] transformToDoubleValuesMV(ProjectionBlock projectionBlock) {
    if (_dictionary != null || _resultMetadata.getDataType() != FieldSpec.DataType.DOUBLE) {
      return super.transformToDoubleValuesMV(projectionBlock);
    }

    if (_doubleValueSet == null) {
      _doubleValueSet = new DoubleOpenHashSet();
      for (String inValue : _stringValueSet) {
        _doubleValueSet.add(Double.parseDouble(inValue));
      }
      _doubleValues = new double[DocIdSetPlanNode.MAX_DOC_PER_CALL][];
    }
    double[][] unFilteredDoubleValues = _mainTransformFunction.transformToDoubleValuesMV(projectionBlock);
    int length = projectionBlock.getNumDocs();
    for (int i = 0; i < length; i++) {
      _doubleValues[i] = filterDoubles(_doubleValueSet, unFilteredDoubleValues[i]);
    }
    return _doubleValues;
  }

  @Override
  public String[][] transformToStringValuesMV(ProjectionBlock projectionBlock) {
    if (_dictionary != null || _resultMetadata.getDataType() != FieldSpec.DataType.STRING) {
      return super.transformToStringValuesMV(projectionBlock);
    }

    if (_stringValues == null) {
      _stringValues = new String[DocIdSetPlanNode.MAX_DOC_PER_CALL][];
    }
    String[][] unFilteredStringValues = _mainTransformFunction.transformToStringValuesMV(projectionBlock);
    int length = projectionBlock.getNumDocs();
    for (int i = 0; i < length; i++) {
      _stringValues[i] = filterStrings(_stringValueSet, unFilteredStringValues[i]);
    }
    return _stringValues;
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
