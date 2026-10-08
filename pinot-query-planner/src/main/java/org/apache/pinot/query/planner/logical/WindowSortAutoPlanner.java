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
package org.apache.pinot.query.planner.logical;

import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Exchange;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.pinot.calcite.rel.logical.PinotKWayMergeSortExchange;
import org.apache.pinot.calcite.rel.logical.PinotLogicalExchange;
import org.apache.pinot.calcite.rel.logical.PinotLogicalSortExchange;


/// Resolves AUTO on a finalized logical tree. This per-compilation pass is not shared between threads.
/// Deterministic tree paths separate exchanges before any stage allocation; the returned identity map binds only
/// this query's final logical exchanges to their observations during plain fragmentation.
public final class WindowSortAutoPlanner {
  private WindowSortAutoPlanner() {
  }

  public static Result resolve(RelNode root, @Nullable WindowSortAutoPlan selector) {
    Map<Exchange, Selection> selections = new IdentityHashMap<>();
    return new Result(rewrite(root, "root", selector, selections), selections);
  }

  private static RelNode rewrite(RelNode node, String path, @Nullable WindowSortAutoPlan selector,
      Map<Exchange, Selection> selections) {
    PinotLogicalSortExchange auto = node instanceof PinotLogicalSortExchange
        && ((PinotLogicalSortExchange) node).isAutoWindowSort() ? (PinotLogicalSortExchange) node : null;
    WindowSortAutoPlan.ExchangeKey key = auto != null
        ? new WindowSortAutoPlan.ExchangeKey(path, RelOptUtil.toString(auto.getInput()).hashCode(),
            auto.getCollation().hashCode()) : null;
    boolean senderSort = key != null && selector != null && selector.useSenderSort(key);
    boolean profile = key != null && selector != null && !senderSort && selector.shouldProfile(key);
    List<RelNode> inputs = new ArrayList<>();
    boolean changed = false;
    for (int i = 0; i < node.getInputs().size(); i++) {
      RelNode original = node.getInput(i);
      RelNode rewritten = rewrite(original, path + "/" + i, selector, selections);
      inputs.add(rewritten);
      changed |= original != rewritten;
    }
    if (auto == null) {
      return changed ? node.copy(node.getTraitSet(), inputs) : node;
    }
    RelNode input = inputs.get(0);
    Exchange exchange;
    RelNode result;
    if (senderSort) {
      RelNode sorted = unboundedSort(input, auto);
      exchange = PinotKWayMergeSortExchange.create(sorted, auto.getDistribution(), auto.getCollation(),
          auto.getPrePartitioned());
      result = exchange;
    } else {
      exchange = PinotLogicalExchange.create(input, auto.getDistribution(), auto.getExchangeType(),
          auto.getPrePartitioned());
      result = unboundedSort(exchange, auto);
    }
    if (selector != null) {
      selections.put(exchange, new Selection(key, profile, auto.getCollation().getFieldCollations()));
    }
    return result;
  }

  private static LogicalSort unboundedSort(RelNode input, PinotLogicalSortExchange auto) {
    return LogicalSort.create(input, auto.getCollation(), null, null);
  }

  public record Selection(WindowSortAutoPlan.ExchangeKey key, boolean profile,
                          List<RelFieldCollation> collations) {
  }

  public record Result(RelNode root, Map<Exchange, Selection> selections) {
  }
}
