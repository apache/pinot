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
import org.apache.pinot.core.common.DataSource;
import org.apache.pinot.core.operator.blocks.ProjectionBlock;
import org.apache.pinot.core.operator.transform.TransformResultMetadata;
import org.apache.pinot.core.plan.DocIdSetPlanNode;

import java.util.List;
import java.util.Map;

import static java.util.Locale.ENGLISH;


/**
 * A group of commonly used string transformation which has only one single parameter,
 * including lower and upper.
 * <p>
 * Example usage of {@code "lower"}:
 * <blockquote><pre>
 *     SELECT foo FROM myTable WHERE lower(bar) = 'lower_case'
 *     SELECT foo FROM myTable WHERE upper(bar) = 'UPPER_CASE''
 * </pre></blockquote>
 * </p>
 */
public abstract class SingleParamStringTransformFunction extends BaseTransformFunction {
  private TransformFunction _transformFunction;
  protected String[] _results;

  @Override
  public void init(List<TransformFunction> arguments, Map<String, DataSource> dataSourceMap) {
    Preconditions
        .checkArgument(arguments.size() == 1, "Exactly 1 argument is required for transform function: %s", getName());
    TransformFunction transformFunction = arguments.get(0);
    Preconditions.checkArgument(!(transformFunction instanceof LiteralTransformFunction),
        "Argument cannot be literal for transform function: %s", getName());
    Preconditions.checkArgument(transformFunction.getResultMetadata().isSingleValue(),
        "Argument must be single-valued for transform function: %s", getName());

    _transformFunction = transformFunction;
  }

  @Override
  public TransformResultMetadata getResultMetadata() {
    return STRING_SV_NO_DICTIONARY_METADATA;
  }

  @Override
  public String[] transformToStringValuesSV(ProjectionBlock projectionBlock) {
    if (_results == null) {
      _results = new String[DocIdSetPlanNode.MAX_DOC_PER_CALL];
    }

    String[] values = _transformFunction.transformToStringValuesSV(projectionBlock);
    applyStringOperator(values, projectionBlock.getNumDocs());
    return _results;
  }

  abstract protected void applyStringOperator(String[] values, int length);

  public static class UpperTransformFunction extends SingleParamStringTransformFunction {
    public static final String FUNCTION_NAME = "upper";

    @Override
    public String getName() {
      return FUNCTION_NAME;
    }

    @Override
    protected void applyStringOperator(String[] values, int length) {
      for (int i = 0; i < length; i++) {
        _results[i] = values[i].toUpperCase(ENGLISH);
      }
    }
  }

  public static class LowerTransformFunction extends SingleParamStringTransformFunction {
    public static final String FUNCTION_NAME = "lower";

    @Override
    public String getName() {
      return FUNCTION_NAME;
    }

    @Override
    protected void applyStringOperator(String[] values, int length) {
      for (int i = 0; i < length; i++) {
        _results[i] = values[i].toLowerCase(ENGLISH);
      }
    }
  }
}
