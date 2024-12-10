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
package org.apache.pinot.tsdb.m3ql.plan;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import java.util.List;
import org.apache.pinot.tsdb.m3ql.operator.TransformNullOperator;
import org.apache.pinot.tsdb.spi.operator.BaseTimeSeriesOperator;
import org.apache.pinot.tsdb.spi.plan.BaseTimeSeriesPlanNode;


public class TransformNullPlanNode extends BaseTimeSeriesPlanNode {
  public static final Double DEFAULT_VALUE = 0.0;
  private final Double _defaultValue;

  @JsonCreator
  public TransformNullPlanNode(@JsonProperty("id") String id, @JsonProperty("defaultValue") Double defaultValue,
      @JsonProperty("inputs") List<BaseTimeSeriesPlanNode> inputs) {
    super(id, inputs);
    _defaultValue = defaultValue;
  }

  public Double getDefaultValue() {
    return _defaultValue;
  }

  @Override
  public BaseTimeSeriesPlanNode withInputs(List<BaseTimeSeriesPlanNode> newInputs) {
    return new TransformNullPlanNode(_id, _defaultValue, newInputs);
  }

  @Override
  public String getKlass() {
    return TransformNullPlanNode.class.getName();
  }

  @Override
  public String getExplainName() {
    return "TRANSFORM_NULL";
  }

  @Override
  public BaseTimeSeriesOperator run() {
    Preconditions.checkState(_inputs.size() == 1,
        "TransformNullPlanNode should have only 1 child, got: %s", _inputs.size());
    BaseTimeSeriesOperator childOperator = _inputs.get(0).run();
    return new TransformNullOperator(_defaultValue, ImmutableList.of(childOperator));
  }
}
