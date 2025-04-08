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
package org.apache.pinot.query.planner.physical.v2.mapping;

import java.util.List;
import javax.annotation.Nullable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.SetOp;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.core.Values;
import org.apache.calcite.rel.core.Window;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.commons.collections4.CollectionUtils;


/**
 * Generates {@link PinotDistMapping} for a given source and destination RelNode.
 */
public class DistMappingGenerator {
  private DistMappingGenerator() {
  }

  /**
   * Source to destination mapping.
   */
  public static PinotDistMapping compute(RelNode source, RelNode destination,
      @Nullable List<RelNode> leadingSiblings) {
    if (destination instanceof Project) {
      Project project = (Project) destination;
      return computeExactInputRefMapping(project, source.getRowType().getFieldCount());
    } else if (destination instanceof Window) {
      // Window preserves input fields, and appends a field for each window expr.
      Window window = (Window) destination;
      return PinotDistMapping.identity(window.getInput().getRowType().getFieldCount());
    } else if (destination instanceof Aggregate) {
      Aggregate aggregate = (Aggregate) destination;
      PinotDistMapping mapping = new PinotDistMapping(source.getRowType().getFieldCount());
      List<Integer> groupSet = aggregate.getGroupSet().asList();
      for (int j = 0; j < groupSet.size(); j++) {
        mapping.set(groupSet.get(j), j);
      }
      return mapping;
    } else if (destination instanceof Join) {
      if (CollectionUtils.isEmpty(leadingSiblings)) {
        return PinotDistMapping.identity(source.getRowType().getFieldCount());
      }
      int leftFieldCount = 0;
      for (RelNode sibling : leadingSiblings) {
        leftFieldCount += sibling.getRowType().getFieldCount();
      }
      PinotDistMapping mapping = new PinotDistMapping(source.getRowType().getFieldCount());
      for (int i = 0; i < mapping.getSourceCount(); i++) {
        mapping.set(i, i + leftFieldCount);
      }
      return mapping;
    } else if (destination instanceof Filter) {
      return PinotDistMapping.identity(source.getRowType().getFieldCount());
    } else if (destination instanceof TableScan) {
      throw new IllegalStateException("Found destination as TableScan in MappingGenerator");
    } else if (destination instanceof Values) {
      throw new IllegalStateException("Found destination as Values in MappingGenerator");
    } else if (destination instanceof Sort) {
      return PinotDistMapping.identity(source.getRowType().getFieldCount());
    } else if (destination instanceof SetOp) {
      SetOp setOp = (SetOp) destination;
      if (setOp.isHomogeneous(true)) {
        return PinotDistMapping.identity(source.getRowType().getFieldCount());
      }
      // TODO(mse-physical): Handle heterogeneous set ops. Currently we drop al mapping refs.
      return new PinotDistMapping(source.getRowType().getFieldCount());
    }
    throw new IllegalStateException("Unknown node type: " + destination.getClass());
  }

  private static PinotDistMapping computeExactInputRefMapping(Project project, int sourceCount) {
    PinotDistMapping mapping = new PinotDistMapping(sourceCount);
    int indexInCurrentRelNode = 0;
    for (RexNode rexNode : project.getProjects()) {
      if (rexNode instanceof RexInputRef) {
        RexInputRef rexInputRef = (RexInputRef) rexNode;
        mapping.set(rexInputRef.getIndex(), indexInCurrentRelNode);
      }
      indexInCurrentRelNode++;
    }
    return mapping;
  }
}
