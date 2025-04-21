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
package org.apache.pinot.query.planner.physical.v2;

import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.Minus;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.core.Union;
import org.apache.calcite.rel.core.Values;
import org.apache.calcite.rel.core.Window;
import org.apache.pinot.calcite.rel.logical.PinotLogicalAggregate;
import org.apache.pinot.calcite.rel.traits.TraitAssignment;
import org.apache.pinot.common.config.provider.TableCache;
import org.apache.pinot.query.context.PhysicalPlannerContext;
import org.apache.pinot.query.planner.physical.v2.nodes.PhysicalAggregate;
import org.apache.pinot.query.planner.physical.v2.nodes.PhysicalFilter;
import org.apache.pinot.query.planner.physical.v2.nodes.PhysicalJoin;
import org.apache.pinot.query.planner.physical.v2.nodes.PhysicalProject;
import org.apache.pinot.query.planner.physical.v2.nodes.PhysicalSort;
import org.apache.pinot.query.planner.physical.v2.nodes.PhysicalTableScan;
import org.apache.pinot.query.planner.physical.v2.nodes.PhysicalUnion;
import org.apache.pinot.query.planner.physical.v2.nodes.PhysicalValues;
import org.apache.pinot.query.planner.physical.v2.nodes.PhysicalWindow;
import org.apache.pinot.query.planner.physical.v2.opt.PhysicalOptRuleSet;
import org.apache.pinot.query.planner.plannode.AggregateNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Converts a tree of RelNode to a tree of PRelNode, running the configured Physical Optimizers in the process.
 */
public class RelToPRelConverter {
  private static final Logger LOGGER = LoggerFactory.getLogger(RelToPRelConverter.class);

  private RelToPRelConverter() {
  }

  public static PRelNode toPRelNode(RelNode relNode, PhysicalPlannerContext context, TableCache tableCache) {
    // Step-1: Convert each RelNode to a PRelNode
    PRelNode rootPRelNode = create(relNode, context.getNodeIdGenerator());
    // Step-2: Assign traits
    rootPRelNode = TraitAssignment.assign(rootPRelNode, context);
    // Step-3: Run physical optimizer rules.
    var pRelTransformers = PhysicalOptRuleSet.create(context, tableCache);
    for (var pRelTransformer : pRelTransformers) {
      rootPRelNode = pRelTransformer.execute(rootPRelNode);
    }
    return rootPRelNode;
  }

  public static PRelNode create(RelNode relNode, Supplier<Integer> nodeIdGenerator) {
    List<PRelNode> inputs = new ArrayList<>();
    for (RelNode input : relNode.getInputs()) {
      inputs.add(create(input, nodeIdGenerator));
    }
    if (relNode instanceof TableScan) {
      Preconditions.checkState(inputs.isEmpty(), "Expected no inputs to table scan. Found: %s", inputs);
      return new PhysicalTableScan((TableScan) relNode, nodeIdGenerator.get(), null, null);
    } else if (relNode instanceof Filter) {
      Preconditions.checkState(inputs.size() == 1, "Expected exactly 1 input of filter. Found: %s", inputs);
      Filter filter = (Filter) relNode;
      return new PhysicalFilter(filter.getCluster(), filter.getTraitSet(), filter.getHints(), filter.getCondition(),
          nodeIdGenerator.get(), inputs.get(0), null, false);
    } else if (relNode instanceof Project) {
      Preconditions.checkState(inputs.size() == 1, "Expected exactly 1 input of project. Found: %s", inputs);
      Project project = (Project) relNode;
      return new PhysicalProject(project.getCluster(), project.getTraitSet(), project.getHints(), project.getProjects(),
          project.getRowType(), project.getVariablesSet(), nodeIdGenerator.get(), inputs.get(0), null, false);
    } else if (relNode instanceof PinotLogicalAggregate) {
      Preconditions.checkState(inputs.size() == 1, "Expected exactly 1 input of agg. Found: %s", inputs);
      PinotLogicalAggregate aggRel = (PinotLogicalAggregate) relNode;
      return new PhysicalAggregate(aggRel.getCluster(), aggRel.getTraitSet(), aggRel.getHints(), aggRel.getGroupSet(),
          aggRel.getGroupSets(), aggRel.getAggCallList(), nodeIdGenerator.get(), inputs.get(0), null, false,
          AggregateNode.AggType.DIRECT, false, List.of(), 0);
    } else if (relNode instanceof Join) {
      Preconditions.checkState(relNode.getInputs().size() == 2, "Expected exactly 2 inputs to join. Found: %s", inputs);
      Join join = (Join) relNode;
      return new PhysicalJoin(join.getCluster(), join.getTraitSet(), join.getHints(), join.getCondition(),
          join.getVariablesSet(), join.getJoinType(), nodeIdGenerator.get(), inputs.get(0), inputs.get(1), null);
    } else if (relNode instanceof Union) {
      Union union = (Union) relNode;
      return new PhysicalUnion(union.getCluster(), union.getTraitSet(), union.getHints(), union.all, inputs,
          nodeIdGenerator.get(), null);
    } else if (relNode instanceof Minus) {
      Minus minus = (Minus) relNode;
      return new PhysicalUnion(minus.getCluster(), minus.getTraitSet(), minus.getHints(), minus.all, inputs,
          nodeIdGenerator.get(), null);
    } else if (relNode instanceof Sort) {
      Preconditions.checkState(inputs.size() == 1, "Expected exactly 1 input of sort. Found: %s", inputs);
      Sort sort = (Sort) relNode;
      return new PhysicalSort(sort.getCluster(), sort.getTraitSet(), sort.getHints(), sort.getCollation(), sort.offset,
          sort.fetch, inputs.get(0), nodeIdGenerator.get(), null, false);
    } else if (relNode instanceof Values) {
      Preconditions.checkState(inputs.isEmpty(), "Expected no inputs to values. Found: %s", inputs);
      Values values = (Values) relNode;
      return new PhysicalValues(values.getCluster(), values.getHints(), values.getRowType(), values.getTuples(),
          values.getTraitSet(), nodeIdGenerator.get(), null);
    } else if (relNode instanceof Window) {
      Preconditions.checkState(inputs.size() == 1, "Expected exactly 1 input of window. Found: %s", inputs);
      Window window = (Window) relNode;
      return new PhysicalWindow(window.getCluster(), window.getTraitSet(), window.getHints(), window.getConstants(),
          window.getRowType(), window.groups, nodeIdGenerator.get(), inputs.get(0), null);
    }
    throw new IllegalStateException("Unexpected relNode type: " + relNode.getClass().getName());
  }
}
