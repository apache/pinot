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
package org.apache.pinot.query.planner.serde;

import org.apache.pinot.common.proto.Plan;
import org.testng.annotations.Test;

import static org.testng.Assert.expectThrows;


public class PlanNodeDeserializerTest {

  /**
   * A {@link Plan.PlanNode} whose {@code node} oneof is unset deserializes to the {@code default} branch
   * and throws. This pins the mixed-version contract for the new {@code RuntimeFilterNode} variant (proto
   * tag 19): an older server that does not know the variant lands in this same default branch and fails
   * the query gracefully (a per-query error, caught by the query runner) rather than misbehaving. The
   * default-OFF {@code pinot.broker.enable.runtime.filter.join} flag is the guard that prevents this from
   * happening on a not-yet-fully-upgraded cluster.
   */
  @Test
  public void testUnknownNodeCaseThrowsIllegalState() {
    Plan.PlanNode protoNode = Plan.PlanNode.newBuilder().setStageId(0).build();
    expectThrows(IllegalStateException.class, () -> PlanNodeDeserializer.process(protoNode));
  }
}
