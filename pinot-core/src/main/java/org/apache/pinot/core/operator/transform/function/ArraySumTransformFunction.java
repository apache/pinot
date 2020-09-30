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
import org.apache.pinot.core.common.DataSource;
import org.apache.pinot.core.operator.blocks.ProjectionBlock;
import org.apache.pinot.core.operator.transform.TransformResultMetadata;
import org.apache.pinot.core.plan.DocIdSetPlanNode;
import org.apache.pinot.core.util.ArrayCopyUtils;
import org.apache.pinot.spi.data.FieldSpec;


/**
 * The ArraySumTransformFunction class implements array_sum function for multi-valued columns
 *
 * Sample queries:
 * SELECT COUNT(*) FROM table WHERE array_sum(mvColumn) > 2
 * SELECT COUNT(*) FROM table GROUP BY array_sum(mvColumn)
 * SELECT SUM(array_sum(mvColumn)) FROM table
 */
public class ArraySumTransformFunction extends BaseTransformFunction {
  public static final String FUNCTION_NAME = "array_sum";

  private long[] _longResults;
  private double[] _doubleResults;
  private TransformFunction _argument;
  private TransformResultMetadata _resultMetadata;

  @Override
  public String getName() {
    return FUNCTION_NAME;
  }

  @Override
  public void init(List<TransformFunction> arguments, Map<String, DataSource> dataSourceMap) {
    // Check that there is only 1 argument
    if (arguments.size() != 1) {
      throw new IllegalArgumentException("Exactly 1 argument is required for ARRAY_AVERAGE transform function");
    }

    // Check that the argument is a multi-valued column or transform function
    TransformFunction firstArgument = arguments.get(0);
    if (firstArgument instanceof LiteralTransformFunction || firstArgument.getResultMetadata().isSingleValue()) {
      throw new IllegalArgumentException(
          "The argument of ARRAY_AVERAGE transform function must be a multi-valued column or a transform function");
    }
    FieldSpec.DataType resultDataType;
    switch (firstArgument.getResultMetadata().getDataType()) {
      case INT:
      case LONG:
        resultDataType = FieldSpec.DataType.LONG;
        break;
      case FLOAT:
      case DOUBLE:
        resultDataType = FieldSpec.DataType.DOUBLE;
        break;
      default:
        throw new IllegalArgumentException(
            "The argument of ARRAY_AVERAGE transform function must be numeric");
    }
    _resultMetadata = new TransformResultMetadata(resultDataType, true, false);
    _argument = firstArgument;
  }

  @Override
  public TransformResultMetadata getResultMetadata() {
    return _resultMetadata;
  }

  @Override
  public long[] transformToLongValuesSV(ProjectionBlock projectionBlock) {
    if (_longResults == null) {
      _longResults = new long[DocIdSetPlanNode.MAX_DOC_PER_CALL];
    }
    int length = projectionBlock.getNumDocs();
    long sumRes;
    switch (_argument.getResultMetadata().getDataType()) {
      case INT:
      case LONG:
        long[][] longValuesMV = _argument.transformToLongValuesMV(projectionBlock);
        for (int i = 0; i < length; i++) {
          sumRes = 0;
          for (int j = 0; j < longValuesMV[i].length; j++) {
            sumRes += longValuesMV[i][j];
          }
          _longResults[i] = sumRes;
        }
        break;
      case FLOAT:
      case DOUBLE:
        double[] doubleValues = transformToDoubleValuesSV(projectionBlock);
        ArrayCopyUtils.copy(doubleValues, _longResults, length);
        break;
      default:
        throw new IllegalStateException();
    }
    return _longResults;
  }

  @Override
  public double[] transformToDoubleValuesSV(ProjectionBlock projectionBlock) {
    if (_doubleResults == null) {
      _doubleResults = new double[DocIdSetPlanNode.MAX_DOC_PER_CALL];
    }

    int length = projectionBlock.getNumDocs();
    double sumRes;
    switch (_argument.getResultMetadata().getDataType()) {
      case INT:
      case LONG:
        long[] longValues = transformToLongValuesSV(projectionBlock);
        ArrayCopyUtils.copy(longValues, _doubleResults, length);
        break;
      case FLOAT:
      case DOUBLE:
        double[][] doubleValuesMV = _argument.transformToDoubleValuesMV(projectionBlock);
        for (int i = 0; i < length; i++) {
          sumRes = 0;
          for (int j = 0; j < doubleValuesMV[i].length; j++) {
            sumRes += doubleValuesMV[i][j];
          }
          _doubleResults[i] = sumRes;
        }
        break;
      default:
        throw new IllegalStateException();
    }
    return _doubleResults;
  }
}
