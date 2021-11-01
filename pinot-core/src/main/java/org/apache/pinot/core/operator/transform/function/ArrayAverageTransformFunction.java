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
import org.apache.pinot.core.operator.blocks.ProjectionBlock;
import org.apache.pinot.core.operator.transform.TransformResultMetadata;
import org.apache.pinot.core.plan.DocIdSetPlanNode;
import org.apache.pinot.segment.spi.datasource.DataSource;


/**
 * The ArrayAverageTransformFunction class implements arrayAverage function for multi-valued columns
 *
 * Sample queries:
 * SELECT COUNT(*) FROM table WHERE arrayAverage(mvColumn) > 2
 * SELECT COUNT(*) FROM table GROUP BY arrayAverage(mvColumn)
 * SELECT SUM(arrayAverage(mvColumn)) FROM table
 */
public class ArrayAverageTransformFunction extends BaseTransformFunction {
  public static final String FUNCTION_NAME = "arrayAverage";

  private double[] _results;
  private TransformFunction _argument;

  @Override
  public String getName() {
    return FUNCTION_NAME;
  }

  @Override
  public void init(List<TransformFunction> arguments, Map<String, DataSource> dataSourceMap) {
    // Check that there is only 1 argument
    if (arguments.size() != 1) {
      throw new IllegalArgumentException("Exactly 1 argument is required for ArrayAverage transform function");
    }

    // Check that the argument is a multi-valued column or transform function
    TransformFunction firstArgument = arguments.get(0);
    if (firstArgument instanceof LiteralTransformFunction || firstArgument.getResultMetadata().isSingleValue()) {
      throw new IllegalArgumentException(
          "The argument of ArrayAverage transform function must be a multi-valued column or a transform function");
    }
    if (!firstArgument.getResultMetadata().getDataType().getStoredType().isNumeric()) {
      throw new IllegalArgumentException("The argument of ArrayAverage transform function must be numeric");
    }
    _argument = firstArgument;
  }

  @Override
  public TransformResultMetadata getResultMetadata() {
    return DOUBLE_SV_NO_DICTIONARY_METADATA;
  }

  // TODO DDC
  @Override
  public double[] transformToDoubleValuesSV(ProjectionBlock projectionBlock) {
    if (_results == null) {
      _results = new double[DocIdSetPlanNode.MAX_DOC_PER_CALL];
    }

    int numDocs = projectionBlock.getNumDocs();
    switch (_argument.getResultMetadata().getDataType().getStoredType()) {
      case INT:
        int[][] intValuesMV = _argument.transformToIntValuesMV(projectionBlock);
        for (int i = 0; i < numDocs; i++) {
          double sumRes = 0;
          for (int value : intValuesMV[i]) {
            sumRes += value;
          }
          _results[i] = sumRes / intValuesMV[i].length;
        }
        break;
      case LONG:
        long[][] longValuesMV = _argument.transformToLongValuesMV(projectionBlock);
        for (int i = 0; i < numDocs; i++) {
          double sumRes = 0;
          for (long value : longValuesMV[i]) {
            sumRes += value;
          }
          _results[i] = sumRes / longValuesMV[i].length;
        }
        break;
      case FLOAT:
        float[][] floatValuesMV = _argument.transformToFloatValuesMV(projectionBlock);
        for (int i = 0; i < numDocs; i++) {
          double sumRes = 0;
          for (float value : floatValuesMV[i]) {
            sumRes += value;
          }
          _results[i] = sumRes / floatValuesMV[i].length;
        }
        break;
      case DOUBLE:
        double[][] doubleValuesMV = _argument.transformToDoubleValuesMV(projectionBlock);
        for (int i = 0; i < numDocs; i++) {
          double sumRes = 0;
          for (double value : doubleValuesMV[i]) {
            sumRes += value;
          }
          _results[i] = sumRes / doubleValuesMV[i].length;
        }
        break;
      default:
        throw new IllegalStateException();
    }
    return _results;
  }
}
