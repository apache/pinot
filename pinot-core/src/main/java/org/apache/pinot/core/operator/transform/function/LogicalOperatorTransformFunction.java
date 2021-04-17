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
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.apache.pinot.core.operator.blocks.ProjectionBlock;
import org.apache.pinot.core.operator.transform.TransformResultMetadata;
import org.apache.pinot.core.plan.DocIdSetPlanNode;
import org.apache.pinot.segment.spi.datasource.DataSource;
import org.apache.pinot.spi.utils.ArrayCopyUtils;


/**
 * <code>LogicalOperatorTransformFunction</code> abstracts common functions for logical operators (AND, OR).
 * The results are in boolean format and stored as an integer array with 1 represents true and 0 represents false.
 */
public abstract class LogicalOperatorTransformFunction extends BaseTransformFunction {

  protected List<TransformFunction> _arguments;
  protected int[] _results;

  @Override
  public void init(List<TransformFunction> arguments, Map<String, DataSource> dataSourceMap) {
    _arguments = arguments;
    Preconditions.checkState(arguments.size() > 1,
        String.format("Expect more than 1 argument for logical operator [%s], args [%s].", getName(),
            Arrays.toString(arguments.toArray())));
    for (TransformFunction argument : arguments) {
      Preconditions.checkState(
          argument.getResultMetadata().getDataType().isNumeric() && argument.getResultMetadata().isSingleValue(),
          String.format(
              "Unsupported data type for logical operator [%s] arguments, only supports single-valued number. Invalid argument: expression [%s], result type [%s]",
              getName(), argument.getName(), argument.getResultMetadata()));
    }
  }

  @Override
  public TransformResultMetadata getResultMetadata() {
    return INT_SV_NO_DICTIONARY_METADATA;
  }

  @Override
  public int[] transformToIntValuesSV(ProjectionBlock projectionBlock) {
    if (_results == null) {
      _results = new int[DocIdSetPlanNode.MAX_DOC_PER_CALL];
    }
    int length = projectionBlock.getNumDocs();
    ArrayCopyUtils.copy(_arguments.get(0).transformToIntValuesSV(projectionBlock), _results, length);
    for (int i = 1; i < _arguments.size(); i++) {
      final TransformFunction transformFunction = _arguments.get(i);
      int[] results = transformFunction.transformToIntValuesSV(projectionBlock);
      for (int j = 0; j < length; j++) {
        _results[j] = getLogicalFuncResult(_results[j], results[j]);
      }
    }
    return _results;
  }

  abstract int getLogicalFuncResult(int arg1, int arg2);
}
