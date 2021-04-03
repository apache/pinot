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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.apache.pinot.core.operator.blocks.ProjectionBlock;
import org.apache.pinot.core.operator.transform.TransformResultMetadata;
import org.apache.pinot.core.plan.DocIdSetPlanNode;
import org.apache.pinot.segment.spi.datasource.DataSource;


public class AdditionTransformFunction extends BaseTransformFunction {
  public static final String FUNCTION_NAME = "add";

  private double _literalSum = 0.0;
  private List<TransformFunction> _transformFunctions = new ArrayList<>();
  private double[] _sums;

  @Override
  public String getName() {
    return FUNCTION_NAME;
  }

  @Override
  public void init(List<TransformFunction> arguments, Map<String, DataSource> dataSourceMap) {
    // Check that there are more than 1 arguments
    if (arguments.size() < 2) {
      throw new IllegalArgumentException("At least 2 arguments are required for ADD transform function");
    }

    for (TransformFunction argument : arguments) {
      if (argument instanceof LiteralTransformFunction) {
        _literalSum += Double.parseDouble(((LiteralTransformFunction) argument).getLiteral());
      } else {
        if (!argument.getResultMetadata().isSingleValue()) {
          throw new IllegalArgumentException("All the arguments of ADD transform function must be single-valued");
        }
        _transformFunctions.add(argument);
      }
    }
  }

  @Override
  public TransformResultMetadata getResultMetadata() {
    return DOUBLE_SV_NO_DICTIONARY_METADATA;
  }

  @Override
  public double[] transformToDoubleValuesSV(ProjectionBlock projectionBlock) {
    if (_sums == null) {
      _sums = new double[DocIdSetPlanNode.MAX_DOC_PER_CALL];
    }

    int length = projectionBlock.getNumDocs();
    Arrays.fill(_sums, 0, length, _literalSum);
    for (TransformFunction transformFunction : _transformFunctions) {
      double[] values = transformFunction.transformToDoubleValuesSV(projectionBlock);
      for (int i = 0; i < length; i++) {
        _sums[i] += values[i];
      }
    }
    return _sums;
  }
}
