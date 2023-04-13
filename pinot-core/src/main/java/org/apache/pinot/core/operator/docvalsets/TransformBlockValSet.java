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
package org.apache.pinot.core.operator.docvalsets;

import java.math.BigDecimal;
import javax.annotation.Nullable;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.core.common.BlockValSet;
import org.apache.pinot.core.operator.blocks.ValueBlock;
import org.apache.pinot.core.operator.transform.TransformResultMetadata;
import org.apache.pinot.core.operator.transform.function.TransformFunction;
import org.apache.pinot.core.plan.DocIdSetPlanNode;
import org.apache.pinot.segment.spi.index.reader.Dictionary;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.trace.InvocationRecording;
import org.apache.pinot.spi.trace.InvocationScope;
import org.apache.pinot.spi.trace.Tracing;
import org.roaringbitmap.RoaringBitmap;


/**
 * The <code>TransformBlockValSet</code> class represents the block value set for a transform function in the transform
 * block.
 * <p>Caller is responsible for calling the correct method based on the data source metadata for the block value set.
 */
public class TransformBlockValSet implements BlockValSet {
  private final ValueBlock _valueBlock;
  private final TransformFunction _transformFunction;
  private final ExpressionContext _expression;

  private RoaringBitmap _nullBitmap = null;

  private int[] _numMVEntries;

  private final boolean _isNullHandlingEnabled;

  public TransformBlockValSet(ValueBlock valueBlock, TransformFunction transformFunction,
      ExpressionContext expression, boolean isNullHandlingEnabled) {
    _valueBlock = valueBlock;
    _transformFunction = transformFunction;
    _expression = expression;
    _isNullHandlingEnabled = isNullHandlingEnabled;
  }

  @Nullable
  @Override
  public RoaringBitmap getNullBitmap() {
    return _nullBitmap;
  }

  @Override
  public DataType getValueType() {
    return _transformFunction.getResultMetadata().getDataType();
  }

  @Override
  public boolean isSingleValue() {
    return _transformFunction.getResultMetadata().isSingleValue();
  }

  @Nullable
  @Override
  public Dictionary getDictionary() {
    return _transformFunction.getDictionary();
  }

  @Override
  public int[] getDictionaryIdsSV() {
    try (InvocationScope scope = Tracing.getTracer().createScope(TransformBlockValSet.class)) {
      recordTransformValues(scope, DataType.INT, true);
      return _transformFunction.transformToDictIdsSV(_valueBlock);
    }
  }

  @Override
  public int[] getIntValuesSV() {
    try (InvocationScope scope = Tracing.getTracer().createScope(TransformBlockValSet.class)) {
      recordTransformValues(scope, DataType.INT, true);
      if (!_isNullHandlingEnabled) {
        return _transformFunction.transformToIntValuesSV(_valueBlock);
      }
      Pair<int[], RoaringBitmap> result = _transformFunction.transformToIntValuesSVWithNull(_valueBlock);
      _nullBitmap = result.getRight();
      return result.getLeft();
    }
  }

  @Override
  public long[] getLongValuesSV() {
    try (InvocationScope scope = Tracing.getTracer().createScope(TransformBlockValSet.class)) {
      recordTransformValues(scope, DataType.LONG, true);
      if (!_isNullHandlingEnabled) {
        return _transformFunction.transformToLongValuesSV(_valueBlock);
      }
      Pair<long[], RoaringBitmap> result = _transformFunction.transformToLongValuesSVWithNull(_valueBlock);
      _nullBitmap = result.getRight();
      return result.getLeft();
    }
  }

  @Override
  public float[] getFloatValuesSV() {
    try (InvocationScope scope = Tracing.getTracer().createScope(TransformBlockValSet.class)) {
      recordTransformValues(scope, DataType.FLOAT, true);
      if (!_isNullHandlingEnabled) {
        return _transformFunction.transformToFloatValuesSV(_valueBlock);
      }
      Pair<float[], RoaringBitmap> result = _transformFunction.transformToFloatValuesSVWithNull(_valueBlock);
      _nullBitmap = result.getRight();
      return result.getLeft();
    }
  }

  @Override
  public double[] getDoubleValuesSV() {
    try (InvocationScope scope = Tracing.getTracer().createScope(TransformBlockValSet.class)) {
      recordTransformValues(scope, DataType.DOUBLE, true);
      if (!_isNullHandlingEnabled) {
        return _transformFunction.transformToDoubleValuesSV(_valueBlock);
      }
      Pair<double[], RoaringBitmap> result = _transformFunction.transformToDoubleValuesSVWithNull(_valueBlock);
      _nullBitmap = result.getRight();
      return result.getLeft();
    }
  }

  @Override
  public BigDecimal[] getBigDecimalValuesSV() {
    try (InvocationScope scope = Tracing.getTracer().createScope(TransformBlockValSet.class)) {
      recordTransformValues(scope, DataType.BIG_DECIMAL, true);
      if (!_isNullHandlingEnabled) {
        return _transformFunction.transformToBigDecimalValuesSV(_valueBlock);
      }
      Pair<BigDecimal[], RoaringBitmap> result = _transformFunction.transformToBigDecimalValuesSVWithNull(_valueBlock);
      _nullBitmap = result.getRight();
      return result.getLeft();
    }
  }

  @Override
  public String[] getStringValuesSV() {
    try (InvocationScope scope = Tracing.getTracer().createScope(TransformBlockValSet.class)) {
      recordTransformValues(scope, DataType.STRING, true);
      if (!_isNullHandlingEnabled) {
        return _transformFunction.transformToStringValuesSV(_valueBlock);
      }
      Pair<String[], RoaringBitmap> result = _transformFunction.transformToStringValuesSVWithNull(_valueBlock);
      _nullBitmap = result.getRight();
      return result.getLeft();
    }
  }

  @Override
  public byte[][] getBytesValuesSV() {
    try (InvocationScope scope = Tracing.getTracer().createScope(TransformBlockValSet.class)) {
      recordTransformValues(scope, DataType.BYTES, true);
      if (!_isNullHandlingEnabled) {
        return _transformFunction.transformToBytesValuesSV(_valueBlock);
      }
      Pair<byte[][], RoaringBitmap> result = _transformFunction.transformToBytesValuesSVWithNull(_valueBlock);
      _nullBitmap = result.getRight();
      return result.getLeft();
    }
  }

  @Override
  public int[][] getDictionaryIdsMV() {
    try (InvocationScope scope = Tracing.getTracer().createScope(TransformBlockValSet.class)) {
      recordTransformValues(scope, DataType.INT, false);
      if (!_isNullHandlingEnabled) {
        return _transformFunction.transformToDictIdsMV(_valueBlock);
      }
      Pair<int[][], RoaringBitmap> result = _transformFunction.transformToDictIdsMVWithNull(_valueBlock);
      _nullBitmap = result.getRight();
      return result.getLeft();
    }
  }

  @Override
  public int[][] getIntValuesMV() {
    try (InvocationScope scope = Tracing.getTracer().createScope(TransformBlockValSet.class)) {
      recordTransformValues(scope, DataType.INT, false);
      if (!_isNullHandlingEnabled) {
        return _transformFunction.transformToIntValuesMV(_valueBlock);
      }
      Pair<int[][], RoaringBitmap> result = _transformFunction.transformToIntValuesMVWithNull(_valueBlock);
      _nullBitmap = result.getRight();
      return result.getLeft();
    }
  }

  @Override
  public long[][] getLongValuesMV() {
    try (InvocationScope scope = Tracing.getTracer().createScope(TransformBlockValSet.class)) {
      recordTransformValues(scope, DataType.LONG, false);
      if (!_isNullHandlingEnabled) {
        return _transformFunction.transformToLongValuesMV(_valueBlock);
      }
      Pair<long[][], RoaringBitmap> result = _transformFunction.transformToLongValuesMVWithNull(_valueBlock);
      _nullBitmap = result.getRight();
      return result.getLeft();
    }
  }

  @Override
  public float[][] getFloatValuesMV() {
    try (InvocationScope scope = Tracing.getTracer().createScope(TransformBlockValSet.class)) {
      recordTransformValues(scope, DataType.FLOAT, false);
      if (!_isNullHandlingEnabled) {
        return _transformFunction.transformToFloatValuesMV(_valueBlock);
      }
      Pair<float[][], RoaringBitmap> result = _transformFunction.transformToFloatValuesMVWithNull(_valueBlock);
      _nullBitmap = result.getRight();
      return result.getLeft();
    }
  }

  @Override
  public double[][] getDoubleValuesMV() {
    try (InvocationScope scope = Tracing.getTracer().createScope(TransformBlockValSet.class)) {
      recordTransformValues(scope, DataType.DOUBLE, false);
      if (!_isNullHandlingEnabled) {
        return _transformFunction.transformToDoubleValuesMV(_valueBlock);
      }
      Pair<double[][], RoaringBitmap> result = _transformFunction.transformToDoubleValuesMVWithNull(_valueBlock);
      _nullBitmap = result.getRight();
      return result.getLeft();
    }
  }

  @Override
  public String[][] getStringValuesMV() {
    try (InvocationScope scope = Tracing.getTracer().createScope(TransformBlockValSet.class)) {
      recordTransformValues(scope, DataType.STRING, false);
      if (!_isNullHandlingEnabled) {
        return _transformFunction.transformToStringValuesMV(_valueBlock);
      }
      Pair<String[][], RoaringBitmap> result = _transformFunction.transformToStringValuesMVWithNull(_valueBlock);
      _nullBitmap = result.getRight();
      return result.getLeft();
    }
  }

  @Override
  public byte[][][] getBytesValuesMV() {
    try (InvocationScope scope = Tracing.getTracer().createScope(TransformBlockValSet.class)) {
      recordTransformValues(scope, DataType.BYTES, false);
      if (!_isNullHandlingEnabled) {
        return _transformFunction.transformToBytesValuesMV(_valueBlock);
      }
      Pair<byte[][][], RoaringBitmap> result = _transformFunction.transformToBytesValuesMVWithNull(_valueBlock);
      _nullBitmap = result.getRight();
      return result.getLeft();
    }
  }

  @Override
  public int[] getNumMVEntries() {
    if (_numMVEntries == null) {
      _numMVEntries = new int[DocIdSetPlanNode.MAX_DOC_PER_CALL];
    }
    int numDocs = _valueBlock.getNumDocs();
    TransformResultMetadata resultMetadata = _transformFunction.getResultMetadata();
    if (resultMetadata.hasDictionary()) {
      int[][] dictionaryIds = getDictionaryIdsMV();
      for (int i = 0; i < numDocs; i++) {
        _numMVEntries[i] = dictionaryIds[i].length;
      }
      return _numMVEntries;
    } else {
      switch (resultMetadata.getDataType().getStoredType()) {
        case INT:
          int[][] intValues = getIntValuesMV();
          for (int i = 0; i < numDocs; i++) {
            _numMVEntries[i] = intValues[i].length;
          }
          return _numMVEntries;
        case LONG:
          long[][] longValues = getLongValuesMV();
          for (int i = 0; i < numDocs; i++) {
            _numMVEntries[i] = longValues[i].length;
          }
          return _numMVEntries;
        case FLOAT:
          float[][] floatValues = getFloatValuesMV();
          for (int i = 0; i < numDocs; i++) {
            _numMVEntries[i] = floatValues[i].length;
          }
          return _numMVEntries;
        case DOUBLE:
          double[][] doubleValues = getDoubleValuesMV();
          for (int i = 0; i < numDocs; i++) {
            _numMVEntries[i] = doubleValues[i].length;
          }
          return _numMVEntries;
        case STRING:
          String[][] stringValues = getStringValuesMV();
          for (int i = 0; i < numDocs; i++) {
            _numMVEntries[i] = stringValues[i].length;
          }
          return _numMVEntries;
        default:
          throw new IllegalStateException();
      }
    }
  }

  private void recordTransformValues(InvocationRecording recording, DataType dataType, boolean singleValue) {
    if (recording.isEnabled()) {
      int numDocs = _valueBlock.getNumDocs();
      recording.setNumDocsScanned(numDocs);
      recording.setFunctionName(_transformFunction.getName());
      recording.setOutputDataType(dataType, singleValue);
    }
  }
}
