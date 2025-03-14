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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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


public class MappingGen {
  private MappingGen() {
  }

  public static List<Integer> apply(Map<Integer, List<Integer>> mapping, List<Integer> keys,
      Runnable multiMappedKeyHandler) {
    if (keys.isEmpty()) {
      return Collections.emptyList();
    }
    List<Integer> newKeys = new ArrayList<>();
    boolean anyMappingNotPresent = false;
    for (int key : keys) {
      List<Integer> mappedKeys = mapping.get(key);
      if (mappedKeys == null || mappedKeys.isEmpty()) {
        anyMappingNotPresent = true;
        break;
      }
      newKeys.add(mappedKeys.get(0));
      if (mappedKeys.size() > 1) {
        multiMappedKeyHandler.run();
      }
    }
    if (anyMappingNotPresent) {
      return Collections.emptyList();
    }
    return newKeys;
  }

  /**
   * Source to destination mapping.
   */
  public static Map<Integer, List<Integer>> compute(RelNode source, RelNode destination,
      @Nullable List<RelNode> leadingSiblings) {
    if (destination instanceof Project) {
      Project project = (Project) destination;
      return computeExactInputRefMapping(project, source.getRowType().getFieldCount());
    } else if (destination instanceof Window) {
      // Window preserves input fields, and appends a field for each window expr.
      Window window = (Window) destination;
      return createIdentityMap(window.getInput().getRowType().getFieldCount());
    } else if (destination instanceof Aggregate) {
      Aggregate aggregate = (Aggregate) destination;
      Map<Integer, List<Integer>> result = createEmptyMap(source.getRowType().getFieldCount());
      List<Integer> groupSet = aggregate.getGroupSet().asList();
      for (int j = 0; j < groupSet.size(); j++) {
        result.get(groupSet.get(j)).add(j);
      }
      return result;
    } else if (destination instanceof Join) {
      if (CollectionUtils.isEmpty(leadingSiblings)) {
        return createIdentityMap(source.getRowType().getFieldCount());
      }
      Preconditions.checkState(leadingSiblings.size() == 1, "At most two nodes allowed in join right now");
      int leftFieldCount = leadingSiblings.get(0).getRowType().getFieldCount();
      Map<Integer, List<Integer>> result = createEmptyMap(source.getRowType().getFieldCount());
      for (int i = 0; i < result.size(); i++) {
        result.get(i).add(i + leftFieldCount);
      }
      return result;
    } else if (destination instanceof Filter) {
      return createIdentityMap(source.getRowType().getFieldCount());
    } else if (destination instanceof TableScan) {
      throw new IllegalStateException("Found destination as TableScan");
    } else if (destination instanceof Values) {
      throw new IllegalStateException("");
    } else if (destination instanceof Sort) {
      return createIdentityMap(source.getRowType().getFieldCount());
    } else if (destination instanceof SetOp) {
      SetOp setOp = (SetOp) destination;
      Preconditions.checkState(setOp.isHomogeneous(true), "Only homogenous set-op inputs supported right now");
      // minus, union, intersect
      return createIdentityMap(source.getRowType().getFieldCount());
    }
    throw new IllegalStateException("Unknown node type: " + destination.getClass());
  }

  private static Map<Integer, List<Integer>> computeExactInputRefMapping(Project project, int sourceCount) {
    Map<Integer, List<Integer>> mp = createEmptyMap(sourceCount);
    int currentIndex = 0;
    for (RexNode rexNode : project.getProjects()) {
      if (rexNode instanceof RexInputRef) {
        RexInputRef rexInputRef = (RexInputRef) rexNode;
        mp.get(rexInputRef.getIndex()).add(currentIndex);
      }
      currentIndex++;
    }
    return mp;
  }

  private static Map<Integer, List<Integer>> createIdentityMap(int size) {
    Map<Integer, List<Integer>> result = new HashMap<>();
    for (int i = 0; i < size; i++) {
      List<Integer> ls = new ArrayList<>();
      ls.add(i);
      result.put(i, ls);
    }
    return result;
  }

  private static Map<Integer, List<Integer>> createEmptyMap(int size) {
    Map<Integer, List<Integer>> result = new HashMap<>();
    for (int i = 0; i < size; i++) {
      result.put(i, new ArrayList<>());
    }
    return result;
  }
}
