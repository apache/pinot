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
import java.math.BigDecimal;
import java.util.List;
import java.util.Map;
import org.apache.pinot.core.operator.blocks.ProjectionBlock;
import org.apache.pinot.core.operator.transform.TransformResultMetadata;
import org.apache.pinot.core.plan.DocIdSetPlanNode;
import org.apache.pinot.segment.spi.datasource.DataSource;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.utils.ByteArray;


/**
 * <code>BinaryOperatorTransformFunction</code> abstracts common functions for binary operators (=, !=, >=, >, <=, <)
 * The results are in boolean format and stored as an integer array with 1 represents true and 0 represents false.
 */
public abstract class BinaryOperatorTransformFunction extends BaseTransformFunction {

  protected TransformFunction _leftTransformFunction;
  protected TransformFunction _rightTransformFunction;
  protected FieldSpec.DataType _leftDataType;
  protected FieldSpec.DataType _rightDataType;
  protected int[] _results;

  @Override
  public void init(List<TransformFunction> arguments, Map<String, DataSource> dataSourceMap) {
    // Check that there are exact 2 arguments
    Preconditions
        .checkArgument(arguments.size() == 2, "Exact 2 arguments are required for binary operator transform function");
    _leftTransformFunction = arguments.get(0);
    _rightTransformFunction = arguments.get(1);
    _leftDataType = _leftTransformFunction.getResultMetadata().getDataType();
    _rightDataType = _rightTransformFunction.getResultMetadata().getDataType();
    // Data type check: left and right types should be compatible.
    if (_leftDataType == FieldSpec.DataType.BYTES || _rightDataType == FieldSpec.DataType.BYTES) {
      Preconditions.checkState(_leftDataType == FieldSpec.DataType.BYTES && _rightDataType == FieldSpec.DataType.BYTES,
          String.format(
              "Unsupported data type for comparison: [Left Transform Function [%s] result type is [%s], Right Transform Function [%s] result type is [%s]]",
              _leftTransformFunction.getName(), _leftDataType, _rightTransformFunction.getName(), _rightDataType));
    }
  }

  @Override
  public TransformResultMetadata getResultMetadata() {
    return INT_SV_NO_DICTIONARY_METADATA;
  }

  @Override
  public int[] transformToIntValuesSV(ProjectionBlock projectionBlock) {
    fillResultArray(projectionBlock);
    return _results;
  }

  protected void fillResultArray(ProjectionBlock projectionBlock) {
    if (_results == null) {
      _results = new int[DocIdSetPlanNode.MAX_DOC_PER_CALL];
    }
    int length = projectionBlock.getNumDocs();
    switch (_leftDataType) {
      case INT:
        int[] leftIntValues = _leftTransformFunction.transformToIntValuesSV(projectionBlock);
        switch (_rightDataType) {
          case INT:
            int[] rightIntValues = _rightTransformFunction.transformToIntValuesSV(projectionBlock);
            for (int i = 0; i < length; i++) {
              _results[i] = getBinaryFuncResult(Integer.compare(leftIntValues[i], rightIntValues[i]));
            }
            break;
          case LONG:
            long[] rightLongValues = _rightTransformFunction.transformToLongValuesSV(projectionBlock);
            for (int i = 0; i < length; i++) {
              _results[i] = getBinaryFuncResult(Long.compare(leftIntValues[i], rightLongValues[i]));
            }
            break;
          case FLOAT:
            float[] rightFloatValues = _rightTransformFunction.transformToFloatValuesSV(projectionBlock);
            for (int i = 0; i < length; i++) {
              _results[i] = getBinaryFuncResult(Float.compare(leftIntValues[i], rightFloatValues[i]));
            }
            break;
          case DOUBLE:
            double[] rightDoubleValues = _rightTransformFunction.transformToDoubleValuesSV(projectionBlock);
            for (int i = 0; i < length; i++) {
              _results[i] = getBinaryFuncResult(Double.compare(leftIntValues[i], rightDoubleValues[i]));
            }
            break;
          case STRING:
            String[] rightStringValues = _rightTransformFunction.transformToStringValuesSV(projectionBlock);
            for (int i = 0; i < length; i++) {
              try {
                _results[i] = getBinaryFuncResult(
                    BigDecimal.valueOf(leftIntValues[i]).compareTo(new BigDecimal(rightStringValues[i])));
              } catch (NumberFormatException e) {
                _results[i] = 0;
              }
            }
            break;
          default:
            throw new IllegalStateException(String.format(
                "Unsupported data type for comparison: [Left Transform Function [%s] result type is [%s], Right Transform Function [%s] result type is [%s]]",
                _leftTransformFunction.getName(), _leftDataType, _rightTransformFunction.getName(), _rightDataType));
        }
        break;
      case LONG:
        long[] leftLongValues = _leftTransformFunction.transformToLongValuesSV(projectionBlock);
        switch (_rightDataType) {
          case INT:
            int[] rightIntValues = _rightTransformFunction.transformToIntValuesSV(projectionBlock);
            for (int i = 0; i < length; i++) {
              _results[i] = getBinaryFuncResult(Long.compare(leftLongValues[i], rightIntValues[i]));
            }
            break;
          case LONG:
            long[] rightLongValues = _rightTransformFunction.transformToLongValuesSV(projectionBlock);
            for (int i = 0; i < length; i++) {
              _results[i] = getBinaryFuncResult(Long.compare(leftLongValues[i], rightLongValues[i]));
            }
            break;
          case FLOAT:
            float[] rightFloatValues = _rightTransformFunction.transformToFloatValuesSV(projectionBlock);
            for (int i = 0; i < length; i++) {
              _results[i] = getBinaryFuncResult(
                  BigDecimal.valueOf(leftLongValues[i]).compareTo(BigDecimal.valueOf(rightFloatValues[i])));
            }
            break;
          case DOUBLE:
            double[] rightDoubleValues = _rightTransformFunction.transformToDoubleValuesSV(projectionBlock);
            for (int i = 0; i < length; i++) {
              _results[i] = getBinaryFuncResult(
                  BigDecimal.valueOf(leftLongValues[i]).compareTo(BigDecimal.valueOf(rightDoubleValues[i])));
            }
            break;
          case STRING:
            String[] rightStringValues = _rightTransformFunction.transformToStringValuesSV(projectionBlock);
            for (int i = 0; i < length; i++) {
              try {
                _results[i] = getBinaryFuncResult(
                    BigDecimal.valueOf(leftLongValues[i]).compareTo(new BigDecimal(rightStringValues[i])));
              } catch (NumberFormatException e) {
                _results[i] = 0;
              }
            }
            break;
          default:
            throw new IllegalStateException(String.format(
                "Unsupported data type for comparison: [Left Transform Function [%s] result type is [%s], Right Transform Function [%s] result type is [%s]]",
                _leftTransformFunction.getName(), _leftDataType, _rightTransformFunction.getName(), _rightDataType));
        }
        break;
      case FLOAT:
        float[] leftFloatValues = _leftTransformFunction.transformToFloatValuesSV(projectionBlock);
        switch (_rightDataType) {
          case INT:
            int[] rightIntValues = _rightTransformFunction.transformToIntValuesSV(projectionBlock);
            for (int i = 0; i < length; i++) {
              _results[i] = getBinaryFuncResult(Double.compare(leftFloatValues[i], rightIntValues[i]));
            }
            break;
          case LONG:
            long[] rightLongValues = _rightTransformFunction.transformToLongValuesSV(projectionBlock);
            for (int i = 0; i < length; i++) {
              _results[i] = getBinaryFuncResult(
                  BigDecimal.valueOf(leftFloatValues[i]).compareTo(BigDecimal.valueOf(rightLongValues[i])));
            }
            break;
          case FLOAT:
            float[] rightFloatValues = _rightTransformFunction.transformToFloatValuesSV(projectionBlock);
            for (int i = 0; i < length; i++) {
              _results[i] = getBinaryFuncResult(Float.compare(leftFloatValues[i], rightFloatValues[i]));
            }
            break;
          case DOUBLE:
            double[] rightDoubleValues = _rightTransformFunction.transformToDoubleValuesSV(projectionBlock);
            for (int i = 0; i < length; i++) {
              _results[i] = getBinaryFuncResult(Double.compare(leftFloatValues[i], rightDoubleValues[i]));
            }
            break;
          case STRING:
            String[] rightStringValues = _rightTransformFunction.transformToStringValuesSV(projectionBlock);
            for (int i = 0; i < length; i++) {
              try {
                _results[i] = getBinaryFuncResult(
                    BigDecimal.valueOf(leftFloatValues[i]).compareTo(new BigDecimal(rightStringValues[i])));
              } catch (NumberFormatException e) {
                _results[i] = 0;
              }
            }
            break;
          default:
            throw new IllegalStateException(String.format(
                "Unsupported data type for comparison: [Left Transform Function [%s] result type is [%s], Right Transform Function [%s] result type is [%s]]",
                _leftTransformFunction.getName(), _leftDataType, _rightTransformFunction.getName(), _rightDataType));
        }
        break;
      case DOUBLE:
        double[] leftDoubleValues = _leftTransformFunction.transformToDoubleValuesSV(projectionBlock);
        switch (_rightDataType) {
          case INT:
            int[] rightIntValues = _rightTransformFunction.transformToIntValuesSV(projectionBlock);
            for (int i = 0; i < length; i++) {
              _results[i] = getBinaryFuncResult(Double.compare(leftDoubleValues[i], rightIntValues[i]));
            }
            break;
          case LONG:
            long[] rightLongValues = _rightTransformFunction.transformToLongValuesSV(projectionBlock);
            for (int i = 0; i < length; i++) {
              _results[i] = getBinaryFuncResult(
                  BigDecimal.valueOf(leftDoubleValues[i]).compareTo(BigDecimal.valueOf(rightLongValues[i])));
            }
            break;
          case FLOAT:
            float[] rightFloatValues = _rightTransformFunction.transformToFloatValuesSV(projectionBlock);
            for (int i = 0; i < length; i++) {
              _results[i] = getBinaryFuncResult(Double.compare(leftDoubleValues[i], rightFloatValues[i]));
            }
            break;
          case DOUBLE:
            double[] rightDoubleValues = _rightTransformFunction.transformToDoubleValuesSV(projectionBlock);
            for (int i = 0; i < length; i++) {
              _results[i] = getBinaryFuncResult(Double.compare(leftDoubleValues[i], rightDoubleValues[i]));
            }
            break;
          case STRING:
            String[] rightStringValues = _rightTransformFunction.transformToStringValuesSV(projectionBlock);
            for (int i = 0; i < length; i++) {
              try {
                _results[i] = getBinaryFuncResult(
                    BigDecimal.valueOf(leftDoubleValues[i]).compareTo(new BigDecimal(rightStringValues[i])));
              } catch (NumberFormatException e) {
                _results[i] = 0;
              }
            }
            break;
          default:
            throw new IllegalStateException(String.format(
                "Unsupported data type for comparison: [Left Transform Function [%s] result type is [%s], Right Transform Function [%s] result type is [%s]]",
                _leftTransformFunction.getName(), _leftDataType, _rightTransformFunction.getName(), _rightDataType));
        }
        break;
      case STRING:
        String[] leftStringValues = _leftTransformFunction.transformToStringValuesSV(projectionBlock);
        switch (_rightDataType) {
          case INT:
            int[] rightIntValues = _rightTransformFunction.transformToIntValuesSV(projectionBlock);
            for (int i = 0; i < length; i++) {
              try {
                _results[i] = getBinaryFuncResult(
                    new BigDecimal(leftStringValues[i]).compareTo(BigDecimal.valueOf(rightIntValues[i])));
              } catch (NumberFormatException e) {
                _results[i] = 0;
              }
            }
            break;
          case LONG:
            long[] rightLongValues = _rightTransformFunction.transformToLongValuesSV(projectionBlock);
            for (int i = 0; i < length; i++) {
              try {
                _results[i] = getBinaryFuncResult(
                    new BigDecimal(leftStringValues[i]).compareTo(BigDecimal.valueOf(rightLongValues[i])));
              } catch (NumberFormatException e) {
                _results[i] = 0;
              }
            }
            break;
          case FLOAT:
            float[] rightFloatValues = _rightTransformFunction.transformToFloatValuesSV(projectionBlock);
            for (int i = 0; i < length; i++) {
              try {
                _results[i] = getBinaryFuncResult(
                    new BigDecimal(leftStringValues[i]).compareTo(BigDecimal.valueOf(rightFloatValues[i])));
              } catch (NumberFormatException e) {
                _results[i] = 0;
              }
            }
            break;
          case DOUBLE:
            double[] rightDoubleValues = _rightTransformFunction.transformToDoubleValuesSV(projectionBlock);
            for (int i = 0; i < length; i++) {
              try {
                _results[i] = getBinaryFuncResult(
                    new BigDecimal(leftStringValues[i]).compareTo(BigDecimal.valueOf(rightDoubleValues[i])));
              } catch (NumberFormatException e) {
                _results[i] = 0;
              }
            }
            break;
          case STRING:
            String[] rightStringValues = _rightTransformFunction.transformToStringValuesSV(projectionBlock);
            for (int i = 0; i < length; i++) {
              _results[i] = getBinaryFuncResult(leftStringValues[i].compareTo(rightStringValues[i]));
            }
            break;
          default:
            throw new IllegalStateException(String.format(
                "Unsupported data type for comparison: [Left Transform Function [%s] result type is [%s], Right Transform Function [%s] result type is [%s]]",
                _leftTransformFunction.getName(), _leftDataType, _rightTransformFunction.getName(), _rightDataType));
        }
        break;
      case BYTES:
        byte[][] leftBytesValues = _leftTransformFunction.transformToBytesValuesSV(projectionBlock);
        byte[][] rightBytesValues = _rightTransformFunction.transformToBytesValuesSV(projectionBlock);
        for (int i = 0; i < length; i++) {
          _results[i] =
              getBinaryFuncResult((new ByteArray(leftBytesValues[i])).compareTo(new ByteArray(rightBytesValues[i])));
        }
        break;
      // NOTE: Multi-value columns are not comparable, so we should not reach here
      default:
        throw new IllegalStateException();
    }
  }

  abstract int getBinaryFuncResult(int result);
}
