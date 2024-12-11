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
package org.apache.pinot.tsdb.planner;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.pinot.tsdb.spi.AggInfo;
import org.apache.pinot.tsdb.spi.plan.BaseTimeSeriesPlanNode;
import org.apache.pinot.tsdb.spi.plan.LeafTimeSeriesPlanNode;


/**
 * Fragments the plan into executable units. Since we only support Broker-Reduce for Time Series Queries at present,
 * we will have 1 fragment for the broker, and 1 fragment for each {@link LeafTimeSeriesPlanNode}.
 * <p>
 * As an example, say we have the following plan:
 *   <pre>
 *                            +------------+
 *                            | Node-1     |
 *                            +------------+
 *                          /              \
 *                   +------------+     +------------+
 *                   | Node-2     |     | Leaf-2     |
 *                   +------------+     +------------+
 *                     /
 *            +------------+
 *            | Leaf-1     |
 *            +------------+
 *   </pre>
 *   The plan fragmenter will convert this to:
 *   <pre>
 *     This is fragment-1, aka the Broker's plan fragment:
 *
 *                            +------------+
 *                            | Node-1     |
 *                            +------------+
 *                          /              \
 *                   +------------+     +------------+
 *                   | Node-2     |     | Exchange   |
 *                   +------------+     +------------+
 *                     /
 *            +------------+
 *            | Exchange   |
 *            +------------+
 *   </pre>
 *   <pre>
 *     This is fragment-2:
 *            +------------+
 *            | Leaf-1     |
 *            +------------+
 *   </pre>
 *   <pre>
 *     This is fragment-3:
 *            +------------+
 *            | Leaf-2     |
 *            +------------+
 *   </pre>
 * </p>
 */
public class TimeSeriesPlanFragmenter {
  private TimeSeriesPlanFragmenter() {
  }

  /**
   * Fragments the plan as described in {@link TimeSeriesPlanFragmenter}. The first element of the list is the broker
   * fragment, and the other elements are the server fragments. For single-node queries, this pushes down the entire
   * plan to the servers.
   * <p>
   *   <b>Note:</b> This method may return cloned plan nodes, so you should use them as the plan subsequently.
   * </p>
   */
  public static List<BaseTimeSeriesPlanNode> getFragments(BaseTimeSeriesPlanNode rootNode,
      boolean isSingleServerQuery) {
    List<BaseTimeSeriesPlanNode> result = new ArrayList<>();
    Context context = new Context();
    if (isSingleServerQuery) {
      final String id = rootNode.getId();
      return ImmutableList.of(new TimeSeriesExchangeNode(id, Collections.emptyList(), null), rootNode);
    }
    result.add(fragmentRecursively(rootNode, context));
    result.addAll(context._fragments);
    return result;
  }

  private static BaseTimeSeriesPlanNode fragmentRecursively(BaseTimeSeriesPlanNode planNode, Context context) {
    if (planNode instanceof LeafTimeSeriesPlanNode) {
      LeafTimeSeriesPlanNode leafNode = (LeafTimeSeriesPlanNode) planNode;
      AggInfo currentAggInfo = leafNode.getAggInfo();
      Preconditions.checkState(!currentAggInfo.getIsPartial(),
          "Leaf node in the logical plan should not have partial agg");
      context._fragments.add(leafNode.withAggInfo(currentAggInfo.withPartialAggregation()));
      return new TimeSeriesExchangeNode(planNode.getId(), Collections.emptyList(), currentAggInfo);
    }
    List<BaseTimeSeriesPlanNode> newInputs = new ArrayList<>();
    for (BaseTimeSeriesPlanNode input : planNode.getInputs()) {
      newInputs.add(fragmentRecursively(input, context));
    }
    return planNode.withInputs(newInputs);
  }

  private static class Context {
    private final List<BaseTimeSeriesPlanNode> _fragments = new ArrayList<>();
  }
}
