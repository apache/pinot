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
package org.apache.pinot.query.planner.explain;

import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.calcite.rel.RelDistribution;
import org.apache.pinot.calcite.rel.logical.PinotRelExchangeType;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.query.planner.physical.v2.ExchangeStrategy;
import org.apache.pinot.query.planner.plannode.ExchangeNode;
import org.apache.pinot.query.planner.plannode.ExplainedNode;
import org.apache.pinot.query.planner.plannode.PlanNode;
import org.apache.pinot.query.planner.plannode.PlanNode.NodeHint;
import org.apache.pinot.query.planner.plannode.ProjectNode;
import org.testng.annotations.Test;

import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;


/**
 * Tests for {@link PlanNodeMerger}, the logic that decides whether two explain plan nodes describe the same plan and
 * can be merged into one.
 */
public class PlanNodeMergerTest {
  private static final DataSchema SCHEMA = new DataSchema(new String[]{"col"},
      new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.INT});

  private static ExplainedNode leaf(String title) {
    return new ExplainedNode(0, SCHEMA, null, List.of(), title, Map.of());
  }

  private static ExchangeNode exchange(Set<String> tableNames) {
    return new ExchangeNode(0, SCHEMA, List.of(leaf("Scan")),
        PinotRelExchangeType.getDefaultExchangeType(), RelDistribution.Type.BROADCAST_DISTRIBUTED, null, false, null,
        false, false, tableNames, ExchangeStrategy.BROADCAST_EXCHANGE, "absHashCodeMurmur3");
  }

  @Test
  public void exchangesWithSameTableNamesMerge() {
    PlanNode merged = PlanNodeMerger.mergePlans(exchange(Set.of("t1")), exchange(Set.of("t1")), false);
    assertNotNull(merged, "Exchange nodes describing the same tables must be mergeable");
  }

  @Test
  public void exchangesWithDifferentTableNamesDoNotMerge() {
    PlanNode merged = PlanNodeMerger.mergePlans(exchange(Set.of("t1")), exchange(Set.of("t2")), false);
    assertNull(merged, "Exchange nodes over different tables must not be merged");
  }

  @Test
  public void differentNodeTypesDoNotMerge() {
    ProjectNode project = new ProjectNode(0, SCHEMA, NodeHint.EMPTY, List.of(leaf("Scan")),
        List.of());
    assertNull(PlanNodeMerger.mergePlans(project, leaf("Scan"), false));
  }
}
