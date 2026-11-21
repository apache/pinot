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
import java.util.Set;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelDistributionTraitDef;
import org.apache.calcite.rel.RelDistributions;
import org.apache.calcite.rel.logical.LogicalValues;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.pinot.query.context.PhysicalPlannerContext;
import org.apache.pinot.query.planner.physical.v2.DistHashFunction;
import org.apache.pinot.query.planner.physical.v2.ExchangeStrategy;
import org.apache.pinot.query.planner.physical.v2.HashDistributionDesc;
import org.apache.pinot.query.planner.physical.v2.PRelNode;
import org.apache.pinot.query.planner.physical.v2.PinotDataDistribution;
import org.apache.pinot.query.planner.physical.v2.nodes.PhysicalExchange;
import org.apache.pinot.query.planner.physical.v2.nodes.PhysicalUnion;
import org.apache.pinot.query.planner.physical.v2.nodes.PhysicalValues;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;


/// Verifies worker alignment with independent physical plans, including partition-compatible identity exchanges.
public class WorkerExchangeAssignmentRuleTest {
  private static final List<String> PARENT_WORKERS = List.of("0@worker-a", "0@worker-b");
  private static final List<String> REVERSED_WORKERS = List.of("0@worker-b", "0@worker-a");

  @Test
  public void testMatchingPartitioningAndWorkerOrderNeedsNoExchange() {
    assertNull(exchangeFor(4, PARENT_WORKERS));
  }

  @Test
  public void testMatchingPartitionsWithReorderedWorkersUsesIdentityExchange() {
    assertIdentityExchange(exchangeFor(4, REVERSED_WORKERS), 4);
  }

  @Test
  public void testColocatedPartitionCountsWithReorderedWorkersUsesIdentityExchange() {
    assertIdentityExchange(exchangeFor(8, REVERSED_WORKERS), 8);
  }

  private static PRelNode exchangeFor(int inputPartitions, List<String> inputWorkers) {
    JavaTypeFactoryImpl types = new JavaTypeFactoryImpl();
    VolcanoPlanner planner = new VolcanoPlanner();
    planner.addRelTraitDef(ConventionTraitDef.INSTANCE);
    planner.addRelTraitDef(RelDistributionTraitDef.INSTANCE);
    RelOptCluster cluster = RelOptCluster.create(planner, new RexBuilder(types));
    RelTraitSet traits = cluster.traitSetOf(Convention.NONE).plus(RelDistributions.hash(List.of(0)));
    RelDataType rowType = types.builder().add("key", SqlTypeName.INTEGER).build();
    LogicalValues emptyValues = LogicalValues.createEmpty(cluster, rowType);
    PinotDataDistribution leftDistribution = distribution(PARENT_WORKERS, 4);
    PinotDataDistribution rightDistribution = distribution(inputWorkers, inputPartitions);
    PhysicalValues left = new PhysicalValues(cluster, List.of(), rowType, emptyValues.getTuples(), traits, 1,
        leftDistribution);
    PhysicalValues right = new PhysicalValues(cluster, List.of(), rowType, emptyValues.getTuples(), traits, 2,
        rightDistribution);
    PhysicalUnion parent = new PhysicalUnion(cluster, traits, List.of(), true, List.of(left, right), 3,
        leftDistribution);

    PRelNode result = new WorkerExchangeAssignmentRule(new PhysicalPlannerContext())
        .meetDistributionConstraint(right, rightDistribution, parent);
    if (result != null) {
      assertSame(result.getPRelInput(0), right, "Alignment must preserve the original input plan");
      assertEquals(right.getPinotDataDistributionOrThrow().getWorkers(), inputWorkers);
    }
    return result;
  }

  private static PinotDataDistribution distribution(List<String> workers, int partitions) {
    HashDistributionDesc descriptor = new HashDistributionDesc(List.of(0), DistHashFunction.MURMUR, partitions);
    return new PinotDataDistribution(RelDistribution.Type.HASH_DISTRIBUTED, workers, workers.hashCode(),
        Set.of(descriptor), null);
  }

  private static void assertIdentityExchange(PRelNode node, int retainedPartitions) {
    assertNotNull(node);
    assertTrue(node instanceof PhysicalExchange);
    PhysicalExchange exchange = (PhysicalExchange) node;
    assertEquals(exchange.getExchangeStrategy(), ExchangeStrategy.IDENTITY_EXCHANGE);
    assertTrue(exchange.getDistributionKeys().isEmpty(), "Compatible partitions must not be hashed again");
    PinotDataDistribution distribution = exchange.getPinotDataDistributionOrThrow();
    assertEquals(distribution.getWorkers(), PARENT_WORKERS);
    assertEquals(distribution.getWorkerHash(), (long) PARENT_WORKERS.hashCode());
    assertEquals(distribution.getHashDistributionDesc(), Set.of(
        new HashDistributionDesc(List.of(0), DistHashFunction.MURMUR, retainedPartitions)));
  }
}
