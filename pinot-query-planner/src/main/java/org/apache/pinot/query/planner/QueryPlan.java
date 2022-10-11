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
package org.apache.pinot.query.planner;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.calcite.util.Pair;
import org.apache.pinot.query.planner.logical.LogicalPlanner;
import org.apache.pinot.query.planner.stage.MailboxReceiveNode;
import org.apache.pinot.query.planner.stage.StageNode;
import org.apache.pinot.query.planner.stage.TableScanNode;


/**
 * The {@code QueryPlan} is the dispatchable query execution plan from the result of {@link LogicalPlanner}.
 *
 * <p>QueryPlan should contain the necessary stage boundary information and the cross exchange information
 * for:
 * <ul>
 *   <li>dispatch individual stages to executor.</li>
 *   <li>instruct stage executor to establish connection channels to other stages.</li>
 *   <li>encode data blocks for transfer between stages based on partitioning scheme.</li>
 * </ul>
 */
public class QueryPlan {
  private final List<Pair<Integer, String>> _queryResultFields;
  private final Map<Integer, StageNode> _queryStageMap;
  private final Map<Integer, StageMetadata> _stageMetadataMap;

  public QueryPlan(List<Pair<Integer, String>> fields, Map<Integer, StageNode> queryStageMap,
      Map<Integer, StageMetadata> stageMetadataMap) {
    _queryResultFields = fields;
    _queryStageMap = queryStageMap;
    _stageMetadataMap = stageMetadataMap;
  }

  /**
   * Get the map between stageID and the stage plan root node.
   * @return stage plan map.
   */
  public Map<Integer, StageNode> getQueryStageMap() {
    return _queryStageMap;
  }

  /**
   * Get the stage metadata information.
   * @return stage metadata info.
   */
  public Map<Integer, StageMetadata> getStageMetadataMap() {
    return _stageMetadataMap;
  }

  /**
   * Get the query result field.
   * @return query result field.
   */
  public List<Pair<Integer, String>> getQueryResultFields() {
    return _queryResultFields;
  }

  public String explain() {
    if (_queryStageMap.isEmpty()) {
      return "EMPTY";
    }

    StringBuilder builder = new StringBuilder();
    explain(
        builder,
        _queryStageMap.get(0),
        "",
        "");
    return builder.toString();
  }

  private void explain(
      StringBuilder builder,
      StageNode root,
      String prefix,
      String childPrefix
  ) {
    int stage = root.getStageId();

    builder
        .append(prefix)
        .append("[").append(stage).append("] ")
        .append(root.explain());

    if (root instanceof TableScanNode) {
      builder.append(' ');
      builder.append(_stageMetadataMap.get(root.getStageId()).getServerInstanceToSegmentsMap());
    }

    builder.append('\n');

    if (root instanceof MailboxReceiveNode) {
      int senderStage = ((MailboxReceiveNode) root).getSenderStageId();
      StageNode sender = _queryStageMap.get(senderStage);
      explain(builder, sender, childPrefix + "└── ", childPrefix + "    ");
    } else {
      for (Iterator<StageNode> iterator = root.getInputs().iterator(); iterator.hasNext();) {
        StageNode input = iterator.next();
        if (iterator.hasNext()) {
          explain(builder, input, childPrefix + "├── ", childPrefix + "│   ");
        } else {
          explain(builder, input, childPrefix + "└── ", childPrefix + "    ");
        }
      }
    }
  }
}
