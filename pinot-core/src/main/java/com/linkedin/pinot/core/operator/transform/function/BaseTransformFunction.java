/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
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
package com.linkedin.pinot.core.operator.transform.function;

import com.linkedin.pinot.common.data.FieldSpec;
import com.linkedin.pinot.core.operator.blocks.ProjectionBlock;
import com.linkedin.pinot.core.operator.transform.TransformResultMetadata;
import com.linkedin.pinot.core.plan.DocIdSetPlanNode;
import com.linkedin.pinot.core.segment.index.readers.Dictionary;
import com.linkedin.pinot.core.util.ArrayCopyUtils;
import javax.annotation.Nonnull;


/**
 * Base class for transform function providing the default implementation for all data types.
 */
public abstract class BaseTransformFunction implements TransformFunction {
  protected static final TransformResultMetadata LONG_SV_NO_DICTIONARY_METADATA =
      new TransformResultMetadata(FieldSpec.DataType.LONG, true, false);
  protected static final TransformResultMetadata DOUBLE_SV_NO_DICTIONARY_METADATA =
      new TransformResultMetadata(FieldSpec.DataType.DOUBLE, true, false);
  protected static final TransformResultMetadata STRING_SV_NO_DICTIONARY_METADATA =
      new TransformResultMetadata(FieldSpec.DataType.STRING, true, false);

  private int[] _intValuesSV;
  private long[] _longValuesSV;
  private float[] _floatValuesSV;
  private double[] _doubleValuesSV;
  private String[] _stringValuesSV;
  private int[][] _intValuesMV;
  private long[][] _longValuesMV;
  private float[][] _floatValuesMV;
  private double[][] _doubleValuesMV;
  private String[][] _stringValuesMV;

  @Override
  public Dictionary getDictionary() {
    throw new UnsupportedOperationException();
  }

  @Override
  public int[] transformToDictIdsSV(@Nonnull ProjectionBlock projectionBlock) {
    throw new UnsupportedOperationException();
  }

  @Override
  public int[][] transformToDictIdsMV(@Nonnull ProjectionBlock projectionBlock) {
    throw new UnsupportedOperationException();
  }

  @Override
  public int[] transformToIntValuesSV(@Nonnull ProjectionBlock projectionBlock) {
    if (_intValuesSV == null) {
      _intValuesSV = new int[DocIdSetPlanNode.MAX_DOC_PER_CALL];
    }
    int length = projectionBlock.getNumDocs();
    switch (getResultMetadata().getDataType()) {
      case LONG:
        long[] longValues = transformToLongValuesSV(projectionBlock);
        ArrayCopyUtils.copy(longValues, _intValuesSV, length);
        return _intValuesSV;
      case FLOAT:
        float[] floatValues = transformToFloatValuesSV(projectionBlock);
        ArrayCopyUtils.copy(floatValues, _intValuesSV, length);
        return _intValuesSV;
      case DOUBLE:
        double[] doubleValues = transformToDoubleValuesSV(projectionBlock);
        ArrayCopyUtils.copy(doubleValues, _intValuesSV, length);
        return _intValuesSV;
      case STRING:
        String[] stringValues = transformToStringValuesSV(projectionBlock);
        ArrayCopyUtils.copy(stringValues, _intValuesSV, length);
        return _intValuesSV;
      default:
        throw new UnsupportedOperationException();
    }
  }

  @Override
  public long[] transformToLongValuesSV(@Nonnull ProjectionBlock projectionBlock) {
    if (_longValuesSV == null) {
      _longValuesSV = new long[DocIdSetPlanNode.MAX_DOC_PER_CALL];
    }
    int length = projectionBlock.getNumDocs();
    switch (getResultMetadata().getDataType()) {
      case INT:
        int[] intValues = transformToIntValuesSV(projectionBlock);
        ArrayCopyUtils.copy(intValues, _longValuesSV, length);
        return _longValuesSV;
      case FLOAT:
        float[] floatValues = transformToFloatValuesSV(projectionBlock);
        ArrayCopyUtils.copy(floatValues, _longValuesSV, length);
        return _longValuesSV;
      case DOUBLE:
        double[] doubleValues = transformToDoubleValuesSV(projectionBlock);
        ArrayCopyUtils.copy(doubleValues, _longValuesSV, length);
        return _longValuesSV;
      case STRING:
        String[] stringValues = transformToStringValuesSV(projectionBlock);
        ArrayCopyUtils.copy(stringValues, _longValuesSV, length);
        return _longValuesSV;
      default:
        throw new UnsupportedOperationException();
    }
  }

  @Override
  public float[] transformToFloatValuesSV(@Nonnull ProjectionBlock projectionBlock) {
    if (_floatValuesSV == null) {
      _floatValuesSV = new float[DocIdSetPlanNode.MAX_DOC_PER_CALL];
    }
    int length = projectionBlock.getNumDocs();
    switch (getResultMetadata().getDataType()) {
      case INT:
        int[] intValues = transformToIntValuesSV(projectionBlock);
        ArrayCopyUtils.copy(intValues, _floatValuesSV, length);
        return _floatValuesSV;
      case LONG:
        long[] longValues = transformToLongValuesSV(projectionBlock);
        ArrayCopyUtils.copy(longValues, _floatValuesSV, length);
        return _floatValuesSV;
      case DOUBLE:
        double[] doubleValues = transformToDoubleValuesSV(projectionBlock);
        ArrayCopyUtils.copy(doubleValues, _floatValuesSV, length);
        return _floatValuesSV;
      case STRING:
        String[] stringValues = transformToStringValuesSV(projectionBlock);
        ArrayCopyUtils.copy(stringValues, _floatValuesSV, length);
        return _floatValuesSV;
      default:
        throw new UnsupportedOperationException();
    }
  }

  @Override
  public double[] transformToDoubleValuesSV(@Nonnull ProjectionBlock projectionBlock) {
    if (_doubleValuesSV == null) {
      _doubleValuesSV = new double[DocIdSetPlanNode.MAX_DOC_PER_CALL];
    }
    int length = projectionBlock.getNumDocs();
    switch (getResultMetadata().getDataType()) {
      case INT:
        int[] intValues = transformToIntValuesSV(projectionBlock);
        ArrayCopyUtils.copy(intValues, _doubleValuesSV, length);
        return _doubleValuesSV;
      case LONG:
        long[] longValues = transformToLongValuesSV(projectionBlock);
        ArrayCopyUtils.copy(longValues, _doubleValuesSV, length);
        return _doubleValuesSV;
      case FLOAT:
        float[] floatValues = transformToFloatValuesSV(projectionBlock);
        ArrayCopyUtils.copy(floatValues, _doubleValuesSV, length);
        return _doubleValuesSV;
      case STRING:
        String[] stringValues = transformToStringValuesSV(projectionBlock);
        ArrayCopyUtils.copy(stringValues, _doubleValuesSV, length);
        return _doubleValuesSV;
      default:
        throw new UnsupportedOperationException();
    }
  }

  @Override
  public String[] transformToStringValuesSV(@Nonnull ProjectionBlock projectionBlock) {
    if (_stringValuesSV == null) {
      _stringValuesSV = new String[DocIdSetPlanNode.MAX_DOC_PER_CALL];
    }
    int length = projectionBlock.getNumDocs();
    switch (getResultMetadata().getDataType()) {
      case INT:
        int[] intValues = transformToIntValuesSV(projectionBlock);
        ArrayCopyUtils.copy(intValues, _stringValuesSV, length);
        return _stringValuesSV;
      case LONG:
        long[] longValues = transformToLongValuesSV(projectionBlock);
        ArrayCopyUtils.copy(longValues, _stringValuesSV, length);
        return _stringValuesSV;
      case FLOAT:
        float[] floatValues = transformToFloatValuesSV(projectionBlock);
        ArrayCopyUtils.copy(floatValues, _stringValuesSV, length);
        return _stringValuesSV;
      case DOUBLE:
        double[] doubleValues = transformToDoubleValuesSV(projectionBlock);
        ArrayCopyUtils.copy(doubleValues, _stringValuesSV, length);
        return _stringValuesSV;
      default:
        throw new UnsupportedOperationException();
    }
  }

  @Override
  public int[][] transformToIntValuesMV(@Nonnull ProjectionBlock projectionBlock) {
    if (_intValuesMV == null) {
      _intValuesMV = new int[DocIdSetPlanNode.MAX_DOC_PER_CALL][];
    }
    int length = projectionBlock.getNumDocs();
    switch (getResultMetadata().getDataType()) {
      case LONG:
        long[][] longValues = transformToLongValuesMV(projectionBlock);
        for (int i = 0; i < length; i++) {
          int numValues = longValues[i].length;
          _intValuesMV[i] = new int[numValues];
          ArrayCopyUtils.copy(longValues[i], _intValuesMV[i], numValues);
        }
        return _intValuesMV;
      case FLOAT:
        float[][] floatValues = transformToFloatValuesMV(projectionBlock);
        for (int i = 0; i < length; i++) {
          int numValues = floatValues[i].length;
          _intValuesMV[i] = new int[numValues];
          ArrayCopyUtils.copy(floatValues[i], _intValuesMV[i], numValues);
        }
        return _intValuesMV;
      case DOUBLE:
        double[][] doubleValues = transformToDoubleValuesMV(projectionBlock);
        for (int i = 0; i < length; i++) {
          int numValues = doubleValues[i].length;
          _intValuesMV[i] = new int[numValues];
          ArrayCopyUtils.copy(doubleValues[i], _intValuesMV[i], numValues);
        }
        return _intValuesMV;
      case STRING:
        String[][] stringValues = transformToStringValuesMV(projectionBlock);
        for (int i = 0; i < length; i++) {
          int numValues = stringValues[i].length;
          _intValuesMV[i] = new int[numValues];
          ArrayCopyUtils.copy(stringValues[i], _intValuesMV[i], numValues);
        }
        return _intValuesMV;
      default:
        throw new UnsupportedOperationException();
    }
  }

  @Override
  public long[][] transformToLongValuesMV(@Nonnull ProjectionBlock projectionBlock) {
    if (_longValuesMV == null) {
      _longValuesMV = new long[DocIdSetPlanNode.MAX_DOC_PER_CALL][];
    }
    int length = projectionBlock.getNumDocs();
    switch (getResultMetadata().getDataType()) {
      case INT:
        int[][] intValues = transformToIntValuesMV(projectionBlock);
        for (int i = 0; i < length; i++) {
          int numValues = intValues[i].length;
          _longValuesMV[i] = new long[numValues];
          ArrayCopyUtils.copy(intValues[i], _longValuesMV[i], numValues);
        }
        return _longValuesMV;
      case FLOAT:
        float[][] floatValues = transformToFloatValuesMV(projectionBlock);
        for (int i = 0; i < length; i++) {
          int numValues = floatValues[i].length;
          _longValuesMV[i] = new long[numValues];
          ArrayCopyUtils.copy(floatValues[i], _longValuesMV[i], numValues);
        }
        return _longValuesMV;
      case DOUBLE:
        double[][] doubleValues = transformToDoubleValuesMV(projectionBlock);
        for (int i = 0; i < length; i++) {
          int numValues = doubleValues[i].length;
          _longValuesMV[i] = new long[numValues];
          ArrayCopyUtils.copy(doubleValues[i], _longValuesMV[i], numValues);
        }
        return _longValuesMV;
      case STRING:
        String[][] stringValues = transformToStringValuesMV(projectionBlock);
        for (int i = 0; i < length; i++) {
          int numValues = stringValues[i].length;
          _longValuesMV[i] = new long[numValues];
          ArrayCopyUtils.copy(stringValues[i], _longValuesMV[i], numValues);
        }
        return _longValuesMV;
      default:
        throw new UnsupportedOperationException();
    }
  }

  @Override
  public float[][] transformToFloatValuesMV(@Nonnull ProjectionBlock projectionBlock) {
    if (_floatValuesMV == null) {
      _floatValuesMV = new float[DocIdSetPlanNode.MAX_DOC_PER_CALL][];
    }
    int length = projectionBlock.getNumDocs();
    switch (getResultMetadata().getDataType()) {
      case INT:
        int[][] intValues = transformToIntValuesMV(projectionBlock);
        for (int i = 0; i < length; i++) {
          int numValues = intValues[i].length;
          _floatValuesMV[i] = new float[numValues];
          ArrayCopyUtils.copy(intValues[i], _floatValuesMV[i], numValues);
        }
        return _floatValuesMV;
      case LONG:
        long[][] longValues = transformToLongValuesMV(projectionBlock);
        for (int i = 0; i < length; i++) {
          int numValues = longValues[i].length;
          _floatValuesMV[i] = new float[numValues];
          ArrayCopyUtils.copy(longValues[i], _floatValuesMV[i], numValues);
        }
        return _floatValuesMV;
      case DOUBLE:
        double[][] doubleValues = transformToDoubleValuesMV(projectionBlock);
        for (int i = 0; i < length; i++) {
          int numValues = doubleValues[i].length;
          _floatValuesMV[i] = new float[numValues];
          ArrayCopyUtils.copy(doubleValues[i], _floatValuesMV[i], numValues);
        }
        return _floatValuesMV;
      case STRING:
        String[][] stringValues = transformToStringValuesMV(projectionBlock);
        for (int i = 0; i < length; i++) {
          int numValues = stringValues[i].length;
          _floatValuesMV[i] = new float[numValues];
          ArrayCopyUtils.copy(stringValues[i], _floatValuesMV[i], numValues);
        }
        return _floatValuesMV;
      default:
        throw new UnsupportedOperationException();
    }
  }

  @Override
  public double[][] transformToDoubleValuesMV(@Nonnull ProjectionBlock projectionBlock) {
    if (_doubleValuesMV == null) {
      _doubleValuesMV = new double[DocIdSetPlanNode.MAX_DOC_PER_CALL][];
    }
    int length = projectionBlock.getNumDocs();
    switch (getResultMetadata().getDataType()) {
      case INT:
        int[][] intValues = transformToIntValuesMV(projectionBlock);
        for (int i = 0; i < length; i++) {
          int numValues = intValues[i].length;
          _doubleValuesMV[i] = new double[numValues];
          ArrayCopyUtils.copy(intValues[i], _doubleValuesMV[i], numValues);
        }
        return _doubleValuesMV;
      case LONG:
        long[][] longValues = transformToLongValuesMV(projectionBlock);
        for (int i = 0; i < length; i++) {
          int numValues = longValues[i].length;
          _doubleValuesMV[i] = new double[numValues];
          ArrayCopyUtils.copy(longValues[i], _doubleValuesMV[i], numValues);
        }
        return _doubleValuesMV;
      case FLOAT:
        float[][] floatValues = transformToFloatValuesMV(projectionBlock);
        for (int i = 0; i < length; i++) {
          int numValues = floatValues[i].length;
          _doubleValuesMV[i] = new double[numValues];
          ArrayCopyUtils.copy(floatValues[i], _doubleValuesMV[i], numValues);
        }
        return _doubleValuesMV;
      case STRING:
        String[][] stringValues = transformToStringValuesMV(projectionBlock);
        for (int i = 0; i < length; i++) {
          int numValues = stringValues[i].length;
          _doubleValuesMV[i] = new double[numValues];
          ArrayCopyUtils.copy(stringValues[i], _doubleValuesMV[i], numValues);
        }
        return _doubleValuesMV;
      default:
        throw new UnsupportedOperationException();
    }
  }

  @Override
  public String[][] transformToStringValuesMV(@Nonnull ProjectionBlock projectionBlock) {
    if (_stringValuesMV == null) {
      _stringValuesMV = new String[DocIdSetPlanNode.MAX_DOC_PER_CALL][];
    }
    int length = projectionBlock.getNumDocs();
    switch (getResultMetadata().getDataType()) {
      case INT:
        int[][] intValues = transformToIntValuesMV(projectionBlock);
        for (int i = 0; i < length; i++) {
          int numValues = intValues[i].length;
          _stringValuesMV[i] = new String[numValues];
          ArrayCopyUtils.copy(intValues[i], _stringValuesMV[i], numValues);
        }
        return _stringValuesMV;
      case LONG:
        long[][] longValues = transformToLongValuesMV(projectionBlock);
        for (int i = 0; i < length; i++) {
          int numValues = longValues[i].length;
          _stringValuesMV[i] = new String[numValues];
          ArrayCopyUtils.copy(longValues[i], _stringValuesMV[i], numValues);
        }
        return _stringValuesMV;
      case FLOAT:
        float[][] floatValues = transformToFloatValuesMV(projectionBlock);
        for (int i = 0; i < length; i++) {
          int numValues = floatValues[i].length;
          _stringValuesMV[i] = new String[numValues];
          ArrayCopyUtils.copy(floatValues[i], _stringValuesMV[i], numValues);
        }
        return _stringValuesMV;
      case DOUBLE:
        double[][] doubleValues = transformToDoubleValuesMV(projectionBlock);
        for (int i = 0; i < length; i++) {
          int numValues = doubleValues[i].length;
          _stringValuesMV[i] = new String[numValues];
          ArrayCopyUtils.copy(doubleValues[i], _stringValuesMV[i], numValues);
        }
        return _stringValuesMV;
      default:
        throw new UnsupportedOperationException();
    }
  }
}
