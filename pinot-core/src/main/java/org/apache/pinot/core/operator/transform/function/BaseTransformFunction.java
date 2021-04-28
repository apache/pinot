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

import com.google.common.base.Preconditions;
import org.apache.pinot.core.operator.blocks.ProjectionBlock;
import org.apache.pinot.core.operator.transform.TransformResultMetadata;
import org.apache.pinot.core.plan.DocIdSetPlanNode;
import org.apache.pinot.segment.spi.index.reader.Dictionary;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.utils.ArrayCopyUtils;


/**
 * Base class for transform function providing the default implementation for all data types.
 */
public abstract class BaseTransformFunction implements TransformFunction {
  protected static final TransformResultMetadata INT_SV_NO_DICTIONARY_METADATA =
      new TransformResultMetadata(DataType.INT, true, false);
  protected static final TransformResultMetadata LONG_SV_NO_DICTIONARY_METADATA =
      new TransformResultMetadata(DataType.LONG, true, false);
  protected static final TransformResultMetadata FLOAT_SV_NO_DICTIONARY_METADATA =
      new TransformResultMetadata(DataType.FLOAT, true, false);
  protected static final TransformResultMetadata DOUBLE_SV_NO_DICTIONARY_METADATA =
      new TransformResultMetadata(DataType.DOUBLE, true, false);
  protected static final TransformResultMetadata BOOLEAN_SV_NO_DICTIONARY_METADATA =
      new TransformResultMetadata(DataType.BOOLEAN, true, false);
  protected static final TransformResultMetadata TIMESTAMP_SV_NO_DICTIONARY_METADATA =
      new TransformResultMetadata(DataType.TIMESTAMP, true, false);
  protected static final TransformResultMetadata STRING_SV_NO_DICTIONARY_METADATA =
      new TransformResultMetadata(DataType.STRING, true, false);
  protected static final TransformResultMetadata STRING_MV_NO_DICTIONARY_METADATA =
      new TransformResultMetadata(DataType.STRING, false, false);
  protected static final TransformResultMetadata BYTES_SV_NO_DICTIONARY_METADATA =
      new TransformResultMetadata(DataType.BYTES, true, false);

  private int[] _intValuesSV;
  private long[] _longValuesSV;
  private float[] _floatValuesSV;
  private double[] _doubleValuesSV;
  private String[] _stringValuesSV;
  private byte[][] _byteValuesSV;
  private int[][] _intValuesMV;
  private long[][] _longValuesMV;
  private float[][] _floatValuesMV;
  private double[][] _doubleValuesMV;
  private String[][] _stringValuesMV;

  @Override
  public Dictionary getDictionary() {
    return null;
  }

  @Override
  public int[] transformToDictIdsSV(ProjectionBlock projectionBlock) {
    throw new UnsupportedOperationException();
  }

  @Override
  public int[][] transformToDictIdsMV(ProjectionBlock projectionBlock) {
    throw new UnsupportedOperationException();
  }

  @Override
  public int[] transformToIntValuesSV(ProjectionBlock projectionBlock) {
    if (_intValuesSV == null) {
      _intValuesSV = new int[DocIdSetPlanNode.MAX_DOC_PER_CALL];
    }

    int length = projectionBlock.getNumDocs();
    Dictionary dictionary = getDictionary();
    if (dictionary != null) {
      int[] dictIds = transformToDictIdsSV(projectionBlock);
      dictionary.readIntValues(dictIds, length, _intValuesSV);
    } else {
      switch (getResultMetadata().getDataType().getStoredType()) {
        case LONG:
          long[] longValues = transformToLongValuesSV(projectionBlock);
          ArrayCopyUtils.copy(longValues, _intValuesSV, length);
          break;
        case FLOAT:
          float[] floatValues = transformToFloatValuesSV(projectionBlock);
          ArrayCopyUtils.copy(floatValues, _intValuesSV, length);
          break;
        case DOUBLE:
          double[] doubleValues = transformToDoubleValuesSV(projectionBlock);
          ArrayCopyUtils.copy(doubleValues, _intValuesSV, length);
          break;
        case STRING:
          String[] stringValues = transformToStringValuesSV(projectionBlock);
          ArrayCopyUtils.copy(stringValues, _intValuesSV, length);
          break;
        default:
          throw new IllegalStateException();
      }
    }
    return _intValuesSV;
  }

  @Override
  public long[] transformToLongValuesSV(ProjectionBlock projectionBlock) {
    if (_longValuesSV == null) {
      _longValuesSV = new long[DocIdSetPlanNode.MAX_DOC_PER_CALL];
    }

    int length = projectionBlock.getNumDocs();
    Dictionary dictionary = getDictionary();
    if (dictionary != null) {
      int[] dictIds = transformToDictIdsSV(projectionBlock);
      dictionary.readLongValues(dictIds, length, _longValuesSV);
    } else {
      switch (getResultMetadata().getDataType().getStoredType()) {
        case INT:
          int[] intValues = transformToIntValuesSV(projectionBlock);
          ArrayCopyUtils.copy(intValues, _longValuesSV, length);
          break;
        case FLOAT:
          float[] floatValues = transformToFloatValuesSV(projectionBlock);
          ArrayCopyUtils.copy(floatValues, _longValuesSV, length);
          break;
        case DOUBLE:
          double[] doubleValues = transformToDoubleValuesSV(projectionBlock);
          ArrayCopyUtils.copy(doubleValues, _longValuesSV, length);
          break;
        case STRING:
          String[] stringValues = transformToStringValuesSV(projectionBlock);
          ArrayCopyUtils.copy(stringValues, _longValuesSV, length);
          break;
        default:
          throw new IllegalStateException();
      }
    }
    return _longValuesSV;
  }

  @Override
  public float[] transformToFloatValuesSV(ProjectionBlock projectionBlock) {
    if (_floatValuesSV == null) {
      _floatValuesSV = new float[DocIdSetPlanNode.MAX_DOC_PER_CALL];
    }

    int length = projectionBlock.getNumDocs();
    Dictionary dictionary = getDictionary();
    if (dictionary != null) {
      int[] dictIds = transformToDictIdsSV(projectionBlock);
      dictionary.readFloatValues(dictIds, length, _floatValuesSV);
    } else {
      switch (getResultMetadata().getDataType().getStoredType()) {
        case INT:
          int[] intValues = transformToIntValuesSV(projectionBlock);
          ArrayCopyUtils.copy(intValues, _floatValuesSV, length);
          break;
        case LONG:
          long[] longValues = transformToLongValuesSV(projectionBlock);
          ArrayCopyUtils.copy(longValues, _floatValuesSV, length);
          break;
        case DOUBLE:
          double[] doubleValues = transformToDoubleValuesSV(projectionBlock);
          ArrayCopyUtils.copy(doubleValues, _floatValuesSV, length);
          break;
        case STRING:
          String[] stringValues = transformToStringValuesSV(projectionBlock);
          ArrayCopyUtils.copy(stringValues, _floatValuesSV, length);
          break;
        default:
          throw new IllegalStateException();
      }
    }
    return _floatValuesSV;
  }

  @Override
  public double[] transformToDoubleValuesSV(ProjectionBlock projectionBlock) {
    if (_doubleValuesSV == null) {
      _doubleValuesSV = new double[DocIdSetPlanNode.MAX_DOC_PER_CALL];
    }

    int length = projectionBlock.getNumDocs();
    Dictionary dictionary = getDictionary();
    if (dictionary != null) {
      int[] dictIds = transformToDictIdsSV(projectionBlock);
      dictionary.readDoubleValues(dictIds, length, _doubleValuesSV);
    } else {
      switch (getResultMetadata().getDataType().getStoredType()) {
        case INT:
          int[] intValues = transformToIntValuesSV(projectionBlock);
          ArrayCopyUtils.copy(intValues, _doubleValuesSV, length);
          break;
        case LONG:
          long[] longValues = transformToLongValuesSV(projectionBlock);
          ArrayCopyUtils.copy(longValues, _doubleValuesSV, length);
          break;
        case FLOAT:
          float[] floatValues = transformToFloatValuesSV(projectionBlock);
          ArrayCopyUtils.copy(floatValues, _doubleValuesSV, length);
          break;
        case STRING:
          String[] stringValues = transformToStringValuesSV(projectionBlock);
          ArrayCopyUtils.copy(stringValues, _doubleValuesSV, length);
          break;
        default:
          throw new IllegalStateException();
      }
    }
    return _doubleValuesSV;
  }

  @Override
  public String[] transformToStringValuesSV(ProjectionBlock projectionBlock) {
    if (_stringValuesSV == null) {
      _stringValuesSV = new String[DocIdSetPlanNode.MAX_DOC_PER_CALL];
    }

    int length = projectionBlock.getNumDocs();
    Dictionary dictionary = getDictionary();
    if (dictionary != null) {
      int[] dictIds = transformToDictIdsSV(projectionBlock);
      dictionary.readStringValues(dictIds, length, _stringValuesSV);
    } else {
      switch (getResultMetadata().getDataType().getStoredType()) {
        case INT:
          int[] intValues = transformToIntValuesSV(projectionBlock);
          ArrayCopyUtils.copy(intValues, _stringValuesSV, length);
          break;
        case LONG:
          long[] longValues = transformToLongValuesSV(projectionBlock);
          ArrayCopyUtils.copy(longValues, _stringValuesSV, length);
          break;
        case FLOAT:
          float[] floatValues = transformToFloatValuesSV(projectionBlock);
          ArrayCopyUtils.copy(floatValues, _stringValuesSV, length);
          break;
        case DOUBLE:
          double[] doubleValues = transformToDoubleValuesSV(projectionBlock);
          ArrayCopyUtils.copy(doubleValues, _stringValuesSV, length);
          break;
        case BYTES:
          byte[][] bytesValues = transformToBytesValuesSV(projectionBlock);
          ArrayCopyUtils.copy(bytesValues, _stringValuesSV, length);
        default:
          throw new IllegalStateException();
      }
    }
    return _stringValuesSV;
  }

  @Override
  public byte[][] transformToBytesValuesSV(ProjectionBlock projectionBlock) {
    if (_byteValuesSV == null) {
      _byteValuesSV = new byte[DocIdSetPlanNode.MAX_DOC_PER_CALL][];
    }

    int length = projectionBlock.getNumDocs();
    Dictionary dictionary = getDictionary();
    if (dictionary != null) {
      int[] dictIds = transformToDictIdsSV(projectionBlock);
      dictionary.readIntValues(dictIds, length, _intValuesSV);
    } else {
      Preconditions.checkState(getResultMetadata().getDataType().getStoredType() == DataType.STRING);
      String[] stringValues = transformToStringValuesSV(projectionBlock);
      ArrayCopyUtils.copy(stringValues, _byteValuesSV, length);
    }
    return _byteValuesSV;
  }

  @Override
  public int[][] transformToIntValuesMV(ProjectionBlock projectionBlock) {
    if (_intValuesMV == null) {
      _intValuesMV = new int[DocIdSetPlanNode.MAX_DOC_PER_CALL][];
    }

    int length = projectionBlock.getNumDocs();
    Dictionary dictionary = getDictionary();
    if (dictionary != null) {
      int[][] dictIdsMV = transformToDictIdsMV(projectionBlock);
      for (int i = 0; i < length; i++) {
        int[] dictIds = dictIdsMV[i];
        int numValues = dictIds.length;
        int[] intValues = new int[numValues];
        dictionary.readIntValues(dictIds, numValues, intValues);
        _intValuesMV[i] = intValues;
      }
    } else {
      switch (getResultMetadata().getDataType().getStoredType()) {
        case LONG:
          long[][] longValuesMV = transformToLongValuesMV(projectionBlock);
          for (int i = 0; i < length; i++) {
            long[] longValues = longValuesMV[i];
            int numValues = longValues.length;
            int[] intValues = new int[numValues];
            ArrayCopyUtils.copy(longValues, intValues, numValues);
            _intValuesMV[i] = intValues;
          }
          break;
        case FLOAT:
          float[][] floatValuesMV = transformToFloatValuesMV(projectionBlock);
          for (int i = 0; i < length; i++) {
            float[] floatValues = floatValuesMV[i];
            int numValues = floatValues.length;
            int[] intValues = new int[numValues];
            ArrayCopyUtils.copy(floatValues, intValues, numValues);
            _intValuesMV[i] = intValues;
          }
          break;
        case DOUBLE:
          double[][] doubleValuesMV = transformToDoubleValuesMV(projectionBlock);
          for (int i = 0; i < length; i++) {
            double[] doubleValues = doubleValuesMV[i];
            int numValues = doubleValues.length;
            int[] intValues = new int[numValues];
            ArrayCopyUtils.copy(doubleValues, intValues, numValues);
            _intValuesMV[i] = intValues;
          }
          break;
        case STRING:
          String[][] stringValuesMV = transformToStringValuesMV(projectionBlock);
          for (int i = 0; i < length; i++) {
            String[] stringValues = stringValuesMV[i];
            int numValues = stringValues.length;
            int[] intValues = new int[numValues];
            ArrayCopyUtils.copy(stringValues, intValues, numValues);
            _intValuesMV[i] = intValues;
          }
          break;
        default:
          throw new IllegalStateException();
      }
    }
    return _intValuesMV;
  }

  @Override
  public long[][] transformToLongValuesMV(ProjectionBlock projectionBlock) {
    if (_longValuesMV == null) {
      _longValuesMV = new long[DocIdSetPlanNode.MAX_DOC_PER_CALL][];
    }

    int length = projectionBlock.getNumDocs();
    Dictionary dictionary = getDictionary();
    if (dictionary != null) {
      int[][] dictIdsMV = transformToDictIdsMV(projectionBlock);
      for (int i = 0; i < length; i++) {
        int[] dictIds = dictIdsMV[i];
        int numValues = dictIds.length;
        long[] longValues = new long[numValues];
        dictionary.readLongValues(dictIds, numValues, longValues);
        _longValuesMV[i] = longValues;
      }
    } else {
      switch (getResultMetadata().getDataType().getStoredType()) {
        case INT:
          int[][] intValuesMV = transformToIntValuesMV(projectionBlock);
          for (int i = 0; i < length; i++) {
            int[] intValues = intValuesMV[i];
            int numValues = intValues.length;
            long[] longValues = new long[numValues];
            ArrayCopyUtils.copy(intValues, longValues, numValues);
            _longValuesMV[i] = longValues;
          }
          break;
        case FLOAT:
          float[][] floatValuesMV = transformToFloatValuesMV(projectionBlock);
          for (int i = 0; i < length; i++) {
            float[] floatValues = floatValuesMV[i];
            int numValues = floatValues.length;
            long[] longValues = new long[numValues];
            ArrayCopyUtils.copy(floatValues, longValues, numValues);
            _longValuesMV[i] = longValues;
          }
          break;
        case DOUBLE:
          double[][] doubleValuesMV = transformToDoubleValuesMV(projectionBlock);
          for (int i = 0; i < length; i++) {
            double[] doubleValues = doubleValuesMV[i];
            int numValues = doubleValues.length;
            long[] longValues = new long[numValues];
            ArrayCopyUtils.copy(doubleValues, longValues, numValues);
            _longValuesMV[i] = longValues;
          }
          break;
        case STRING:
          String[][] stringValuesMV = transformToStringValuesMV(projectionBlock);
          for (int i = 0; i < length; i++) {
            String[] stringValues = stringValuesMV[i];
            int numValues = stringValues.length;
            long[] longValues = new long[numValues];
            ArrayCopyUtils.copy(stringValues, longValues, numValues);
            _longValuesMV[i] = longValues;
          }
          break;
        default:
          throw new IllegalStateException();
      }
    }
    return _longValuesMV;
  }

  @Override
  public float[][] transformToFloatValuesMV(ProjectionBlock projectionBlock) {
    if (_floatValuesMV == null) {
      _floatValuesMV = new float[DocIdSetPlanNode.MAX_DOC_PER_CALL][];
    }

    int length = projectionBlock.getNumDocs();
    Dictionary dictionary = getDictionary();
    if (dictionary != null) {
      int[][] dictIdsMV = transformToDictIdsMV(projectionBlock);
      for (int i = 0; i < length; i++) {
        int[] dictIds = dictIdsMV[i];
        int numValues = dictIds.length;
        float[] floatValues = new float[numValues];
        dictionary.readFloatValues(dictIds, numValues, floatValues);
        _floatValuesMV[i] = floatValues;
      }
    } else {
      switch (getResultMetadata().getDataType().getStoredType()) {
        case INT:
          int[][] intValuesMV = transformToIntValuesMV(projectionBlock);
          for (int i = 0; i < length; i++) {
            int[] intValues = intValuesMV[i];
            int numValues = intValues.length;
            float[] floatValues = new float[numValues];
            ArrayCopyUtils.copy(intValues, floatValues, numValues);
            _floatValuesMV[i] = floatValues;
          }
          break;
        case LONG:
          long[][] longValuesMV = transformToLongValuesMV(projectionBlock);
          for (int i = 0; i < length; i++) {
            long[] longValues = longValuesMV[i];
            int numValues = longValues.length;
            float[] floatValues = new float[numValues];
            ArrayCopyUtils.copy(longValues, floatValues, numValues);
            _floatValuesMV[i] = floatValues;
          }
          break;
        case DOUBLE:
          double[][] doubleValuesMV = transformToDoubleValuesMV(projectionBlock);
          for (int i = 0; i < length; i++) {
            double[] doubleValues = doubleValuesMV[i];
            int numValues = doubleValues.length;
            float[] floatValues = new float[numValues];
            ArrayCopyUtils.copy(doubleValues, floatValues, numValues);
            _floatValuesMV[i] = floatValues;
          }
          break;
        case STRING:
          String[][] stringValuesMV = transformToStringValuesMV(projectionBlock);
          for (int i = 0; i < length; i++) {
            String[] stringValues = stringValuesMV[i];
            int numValues = stringValues.length;
            float[] floatValues = new float[numValues];
            ArrayCopyUtils.copy(stringValues, floatValues, numValues);
            _floatValuesMV[i] = floatValues;
          }
          break;
        default:
          throw new IllegalStateException();
      }
    }
    return _floatValuesMV;
  }

  @Override
  public double[][] transformToDoubleValuesMV(ProjectionBlock projectionBlock) {
    if (_doubleValuesMV == null) {
      _doubleValuesMV = new double[DocIdSetPlanNode.MAX_DOC_PER_CALL][];
    }

    int length = projectionBlock.getNumDocs();
    Dictionary dictionary = getDictionary();
    if (dictionary != null) {
      int[][] dictIdsMV = transformToDictIdsMV(projectionBlock);
      for (int i = 0; i < length; i++) {
        int[] dictIds = dictIdsMV[i];
        int numValues = dictIds.length;
        double[] doubleValues = new double[numValues];
        dictionary.readDoubleValues(dictIds, numValues, doubleValues);
        _doubleValuesMV[i] = doubleValues;
      }
    } else {
      switch (getResultMetadata().getDataType().getStoredType()) {
        case INT:
          int[][] intValuesMV = transformToIntValuesMV(projectionBlock);
          for (int i = 0; i < length; i++) {
            int[] intValues = intValuesMV[i];
            int numValues = intValues.length;
            double[] doubleValues = new double[numValues];
            ArrayCopyUtils.copy(intValues, doubleValues, numValues);
            _doubleValuesMV[i] = doubleValues;
          }
          break;
        case LONG:
          long[][] longValuesMV = transformToLongValuesMV(projectionBlock);
          for (int i = 0; i < length; i++) {
            long[] longValues = longValuesMV[i];
            int numValues = longValues.length;
            double[] doubleValues = new double[numValues];
            ArrayCopyUtils.copy(longValues, doubleValues, numValues);
            _doubleValuesMV[i] = doubleValues;
          }
          break;
        case FLOAT:
          float[][] floatValuesMV = transformToFloatValuesMV(projectionBlock);
          for (int i = 0; i < length; i++) {
            float[] floatValues = floatValuesMV[i];
            int numValues = floatValues.length;
            double[] doubleValues = new double[numValues];
            ArrayCopyUtils.copy(floatValues, doubleValues, numValues);
            _doubleValuesMV[i] = doubleValues;
          }
          break;
        case STRING:
          String[][] stringValuesMV = transformToStringValuesMV(projectionBlock);
          for (int i = 0; i < length; i++) {
            String[] stringValues = stringValuesMV[i];
            int numValues = stringValues.length;
            double[] doubleValues = new double[numValues];
            ArrayCopyUtils.copy(stringValues, doubleValues, numValues);
            _doubleValuesMV[i] = doubleValues;
          }
          break;
        default:
          throw new IllegalStateException();
      }
    }
    return _doubleValuesMV;
  }

  @Override
  public String[][] transformToStringValuesMV(ProjectionBlock projectionBlock) {
    if (_stringValuesMV == null) {
      _stringValuesMV = new String[DocIdSetPlanNode.MAX_DOC_PER_CALL][];
    }

    int length = projectionBlock.getNumDocs();
    Dictionary dictionary = getDictionary();
    if (dictionary != null) {
      int[][] dictIdsMV = transformToDictIdsMV(projectionBlock);
      for (int i = 0; i < length; i++) {
        int[] dictIds = dictIdsMV[i];
        int numValues = dictIds.length;
        String[] stringValues = new String[numValues];
        dictionary.readStringValues(dictIds, numValues, stringValues);
        _stringValuesMV[i] = stringValues;
      }
    } else {
      switch (getResultMetadata().getDataType().getStoredType()) {
        case INT:
          int[][] intValuesMV = transformToIntValuesMV(projectionBlock);
          for (int i = 0; i < length; i++) {
            int[] intValues = intValuesMV[i];
            int numValues = intValues.length;
            String[] stringValues = new String[numValues];
            ArrayCopyUtils.copy(intValues, stringValues, numValues);
            _stringValuesMV[i] = stringValues;
          }
          break;
        case LONG:
          long[][] longValuesMV = transformToLongValuesMV(projectionBlock);
          for (int i = 0; i < length; i++) {
            long[] longValues = longValuesMV[i];
            int numValues = longValues.length;
            String[] stringValues = new String[numValues];
            ArrayCopyUtils.copy(longValues, stringValues, numValues);
            _stringValuesMV[i] = stringValues;
          }
          break;
        case FLOAT:
          float[][] floatValuesMV = transformToFloatValuesMV(projectionBlock);
          for (int i = 0; i < length; i++) {
            float[] floatValues = floatValuesMV[i];
            int numValues = floatValues.length;
            String[] stringValues = new String[numValues];
            ArrayCopyUtils.copy(floatValues, stringValues, numValues);
            _stringValuesMV[i] = stringValues;
          }
          break;
        case DOUBLE:
          double[][] doubleValuesMV = transformToDoubleValuesMV(projectionBlock);
          for (int i = 0; i < length; i++) {
            double[] doubleValues = doubleValuesMV[i];
            int numValues = doubleValues.length;
            String[] stringValues = new String[numValues];
            ArrayCopyUtils.copy(doubleValues, stringValues, numValues);
            _stringValuesMV[i] = stringValues;
          }
          break;
        default:
          throw new IllegalStateException();
      }
    }
    return _stringValuesMV;
  }
}
