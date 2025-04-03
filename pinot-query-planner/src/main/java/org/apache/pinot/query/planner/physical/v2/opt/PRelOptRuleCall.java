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
package org.apache.pinot.query.planner.physical.v2.opt;

import java.util.Deque;
import org.apache.pinot.query.context.PhysicalPlannerContext;
import org.apache.pinot.query.planner.physical.v2.PRelNode;


public class PRelOptRuleCall {
  public final PhysicalPlannerContext _physicalPlannerContext;
  public final PRelNode _currentNode;
  public final Deque<PRelNode> _parents;

  public PRelOptRuleCall(PRelNode currentNode, Deque<PRelNode> parents, PhysicalPlannerContext physicalPlannerContext) {
    _currentNode = currentNode;
    _parents = parents;
    _physicalPlannerContext = physicalPlannerContext;
  }
}
