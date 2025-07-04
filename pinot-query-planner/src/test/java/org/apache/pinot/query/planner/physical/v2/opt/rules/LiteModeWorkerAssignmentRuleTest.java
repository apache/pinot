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
package org.apache.pinot.query.planner.physical.v2.opt.rules;

import java.util.List;
import org.apache.pinot.query.planner.physical.v2.PRelNode;
import org.apache.pinot.query.planner.physical.v2.PinotDataDistribution;
import org.mockito.Mockito;

import static org.mockito.Mockito.doReturn;


public class LiteModeWorkerAssignmentRuleTest {

  private PRelNode create(List<PRelNode> inputs, boolean isLeafStage, List<String> workers) {
    // Setup mock pinot data distribution.
    PinotDataDistribution mockPDD = Mockito.mock(PinotDataDistribution.class);
    doReturn(workers).when(mockPDD).getWorkers();
    // Setup mock PRelNode.
    PRelNode current = Mockito.mock(PRelNode.class);
    doReturn(isLeafStage).when(current).isLeafStage();
    doReturn(mockPDD).when(current).getPinotDataDistributionOrThrow();
    doReturn(inputs).when(current).getPRelInputs();
    return current;
  }
}
