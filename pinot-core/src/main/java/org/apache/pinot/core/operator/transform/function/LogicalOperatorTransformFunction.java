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
 * The results are BOOLEAN type.
 */
public abstract class LogicalOperatorTransformFunction extends BaseTransformFunction {
  protected List<TransformFunction> _arguments;
  protected int[] _results;

  @Override
  public void init(List<TransformFunction> arguments, Map<String, DataSource> dataSourceMap) {
    _arguments = arguments;
    int numArguments = arguments.size();
    Preconditions.checkState(numArguments > 1, String
        .format("Expect more than 1 argument for logical operator [%s], args [%s].", getName(),
            Arrays.toString(arguments.toArray())));
    for (int i = 0; i < numArguments; i++) {
      TransformResultMetadata argumentMetadata = arguments.get(i).getResultMetadata();
      Preconditions
          .checkState(argumentMetadata.isSingleValue() && argumentMetadata.getDataType().getStoredType().isNumeric(),
              String.format("Unsupported argument of index: %d, expecting single-valued boolean/number", i));
    }
  }

  @Override
  public TransformResultMetadata getResultMetadata() {
    return BOOLEAN_SV_NO_DICTIONARY_METADATA;
  }

  @Override
  public int[] transformToIntValuesSV(ProjectionBlock projectionBlock) {
    if (_results == null) {
      _results = new int[DocIdSetPlanNode.MAX_DOC_PER_CALL];
    }
    int numDocs = projectionBlock.getNumDocs();
    ArrayCopyUtils.copy(_arguments.get(0).transformToIntValuesSV(projectionBlock), _results, numDocs);
    int numArguments = _arguments.size();
    for (int i = 1; i < numArguments; i++) {
      TransformFunction transformFunction = _arguments.get(i);
      int[] results = transformFunction.transformToIntValuesSV(projectionBlock);
      for (int j = 0; j < numDocs; j++) {
        _results[j] = getLogicalFuncResult(_results[j], results[j]);
      }
    }
    return _results;
  }

  abstract int getLogicalFuncResult(int left, int right);
}
