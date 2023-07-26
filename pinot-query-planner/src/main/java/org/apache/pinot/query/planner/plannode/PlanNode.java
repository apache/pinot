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
package org.apache.pinot.query.planner.plannode;

import java.io.Serializable;
import java.util.List;
import org.apache.pinot.common.utils.DataSchema;


/**
 * PlanNode is a serializable version of the {@link org.apache.calcite.rel.RelNode}.
 *
 * TODO: PlanNode currently uses java.io.Serializable as its serialization format.
 * We should experiment with other type of serialization format for better performance.
 * Essentially what we need is a way to exclude the planner context from the RelNode but only keeps the
 * constructed relational content because we will no longer revisit the planner after PlanFragment is created.
 */
public interface PlanNode extends Serializable {

  int getPlanFragmentId();

  void setPlanFragmentId(int planFragmentId);

  List<PlanNode> getInputs();

  void addInput(PlanNode planNode);

  DataSchema getDataSchema();

  void setDataSchema(DataSchema dataSchema);

  String explain();

  <T, C> T visit(PlanNodeVisitor<T, C> visitor, C context);
}
