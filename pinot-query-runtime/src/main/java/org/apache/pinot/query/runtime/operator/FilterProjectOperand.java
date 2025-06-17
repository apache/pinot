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
package org.apache.pinot.query.runtime.operator;

import java.util.ArrayList;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.query.planner.PlannerUtils;
import org.apache.pinot.query.runtime.operator.operands.TransformOperand;
import org.apache.pinot.query.runtime.operator.operands.TransformOperandFactory;


public class FilterProjectOperand {

  public enum FilterProjectOperandsType {
    FILTER,
    PROJECT
  }

  private final FilterProjectOperandsType _type;
  @Nullable
  final TransformOperand _filter;
  @Nullable
  final List<TransformOperand> _project;

  public FilterProjectOperand(PlannerUtils.FilterProjectRex rex, DataSchema inputSchema) {
    if (rex.getType() == PlannerUtils.FilterProjectRexType.FILTER) {
      _type = FilterProjectOperandsType.FILTER;
      _filter = TransformOperandFactory.getTransformOperand(rex.getFilter(), inputSchema);
      _project = null;
    } else {
      _type = FilterProjectOperandsType.PROJECT;
      _filter = null;
      List<TransformOperand> projects = new ArrayList<>();
      rex.getProjectAndResultSchema().getProject().forEach((x) ->
          projects.add(TransformOperandFactory.getTransformOperand(x, inputSchema)));
      _project = projects;
    }
  }

  public TransformOperand getFilter() {
    return _filter;
  }

  public List<TransformOperand> getProject() {
    return _project;
  }

  public FilterProjectOperandsType getType() {
    return _type;
  }
}
