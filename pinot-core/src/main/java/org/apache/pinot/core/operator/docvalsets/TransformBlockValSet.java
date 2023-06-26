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

  private boolean _nullBitmapSet;
  private RoaringBitmap _nullBitmap;

  private int[] _numMVEntries;

  public TransformBlockValSet(ValueBlock valueBlock, TransformFunction transformFunction) {
    _valueBlock = valueBlock;
    _transformFunction = transformFunction;
  }

  @Nullable
  @Override
  public RoaringBitmap getNullBitmap() {
    if (!_nullBitmapSet) {
      _nullBitmap = _transformFunction.getNullBitmap(_valueBlock);
      _nullBitmapSet = true;
    }
    return _nullBitmap == null ? null : _nullBitmap.clone();
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
      return _transformFunction.transformToIntValuesSV(_valueBlock);
    }
  }

  @Override
  public long[] getLongValuesSV() {
    try (InvocationScope scope = Tracing.getTracer().createScope(TransformBlockValSet.class)) {
      recordTransformValues(scope, DataType.LONG, true);
      return _transformFunction.transformToLongValuesSV(_valueBlock);
    }
  }

  @Override
  public float[] getFloatValuesSV() {
    try (InvocationScope scope = Tracing.getTracer().createScope(TransformBlockValSet.class)) {
      recordTransformValues(scope, DataType.FLOAT, true);
      return _transformFunction.transformToFloatValuesSV(_valueBlock);
    }
  }

  @Override
  public double[] getDoubleValuesSV() {
    try (InvocationScope scope = Tracing.getTracer().createScope(TransformBlockValSet.class)) {
      recordTransformValues(scope, DataType.DOUBLE, true);
      return _transformFunction.transformToDoubleValuesSV(_valueBlock);
    }
  }

  @Override
  public BigDecimal[] getBigDecimalValuesSV() {
    try (InvocationScope scope = Tracing.getTracer().createScope(TransformBlockValSet.class)) {
      recordTransformValues(scope, DataType.BIG_DECIMAL, true);
      return _transformFunction.transformToBigDecimalValuesSV(_valueBlock);
    }
  }

  @Override
  public String[] getStringValuesSV() {
    try (InvocationScope scope = Tracing.getTracer().createScope(TransformBlockValSet.class)) {
      recordTransformValues(scope, DataType.STRING, true);
      return _transformFunction.transformToStringValuesSV(_valueBlock);
    }
  }

  @Override
  public byte[][] getBytesValuesSV() {
    try (InvocationScope scope = Tracing.getTracer().createScope(TransformBlockValSet.class)) {
      recordTransformValues(scope, DataType.BYTES, true);
      return _transformFunction.transformToBytesValuesSV(_valueBlock);
    }
  }

  @Override
  public int[][] getDictionaryIdsMV() {
    try (InvocationScope scope = Tracing.getTracer().createScope(TransformBlockValSet.class)) {
      recordTransformValues(scope, DataType.INT, false);
      return _transformFunction.transformToDictIdsMV(_valueBlock);
    }
  }

  @Override
  public int[][] getIntValuesMV() {
    try (InvocationScope scope = Tracing.getTracer().createScope(TransformBlockValSet.class)) {
      recordTransformValues(scope, DataType.INT, false);
      return _transformFunction.transformToIntValuesMV(_valueBlock);
    }
  }

  @Override
  public long[][] getLongValuesMV() {
    try (InvocationScope scope = Tracing.getTracer().createScope(TransformBlockValSet.class)) {
      recordTransformValues(scope, DataType.LONG, false);
      return _transformFunction.transformToLongValuesMV(_valueBlock);
    }
  }

  @Override
  public float[][] getFloatValuesMV() {
    try (InvocationScope scope = Tracing.getTracer().createScope(TransformBlockValSet.class)) {
      recordTransformValues(scope, DataType.FLOAT, false);
      return _transformFunction.transformToFloatValuesMV(_valueBlock);
    }
  }

  @Override
  public double[][] getDoubleValuesMV() {
    try (InvocationScope scope = Tracing.getTracer().createScope(TransformBlockValSet.class)) {
      recordTransformValues(scope, DataType.DOUBLE, false);
      return _transformFunction.transformToDoubleValuesMV(_valueBlock);
    }
  }

  @Override
  public String[][] getStringValuesMV() {
    try (InvocationScope scope = Tracing.getTracer().createScope(TransformBlockValSet.class)) {
      recordTransformValues(scope, DataType.STRING, false);
      return _transformFunction.transformToStringValuesMV(_valueBlock);
    }
  }

  @Override
  public byte[][][] getBytesValuesMV() {
    try (InvocationScope scope = Tracing.getTracer().createScope(TransformBlockValSet.class)) {
      recordTransformValues(scope, DataType.BYTES, false);
      return _transformFunction.transformToBytesValuesMV(_valueBlock);
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
