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
package org.apache.pinot.query.runtime;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.IntFunction;
import org.apache.pinot.common.datatable.StatMap;
import org.apache.pinot.query.planner.plannode.AggregateNode;
import org.apache.pinot.query.planner.plannode.BasePlanNode;
import org.apache.pinot.query.planner.plannode.EnrichedJoinNode;
import org.apache.pinot.query.planner.plannode.ExchangeNode;
import org.apache.pinot.query.planner.plannode.ExplainedNode;
import org.apache.pinot.query.planner.plannode.FilterNode;
import org.apache.pinot.query.planner.plannode.JoinNode;
import org.apache.pinot.query.planner.plannode.MailboxReceiveNode;
import org.apache.pinot.query.planner.plannode.MailboxSendNode;
import org.apache.pinot.query.planner.plannode.PlanNode;
import org.apache.pinot.query.planner.plannode.PlanNodeVisitor;
import org.apache.pinot.query.planner.plannode.ProjectNode;
import org.apache.pinot.query.planner.plannode.SetOpNode;
import org.apache.pinot.query.planner.plannode.SortNode;
import org.apache.pinot.query.planner.plannode.TableScanNode;
import org.apache.pinot.query.planner.plannode.ValueNode;
import org.apache.pinot.query.planner.plannode.WindowNode;
import org.apache.pinot.query.runtime.operator.MailboxSendOperator;
import org.apache.pinot.query.runtime.operator.MultiStageOperator;
import org.apache.pinot.query.runtime.plan.MultiStageQueryStats;
import org.apache.pinot.spi.utils.JsonUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class InStageStatsTreeBuilder implements PlanNodeVisitor<ObjectNode, InStageStatsTreeBuilder.Context> {
  private static final Logger LOGGER = LoggerFactory.getLogger(InStageStatsTreeBuilder.class);

  private final MultiStageQueryStats.StageStats _stageStats;
  private int _index;
  private static final String CHILDREN_KEY = "children";
  private final IntFunction<ObjectNode> _jsonStatsByStage;

  public InStageStatsTreeBuilder(MultiStageQueryStats.StageStats stageStats, IntFunction<ObjectNode> jsonStatsByStage) {
    _stageStats = stageStats;
    _index = stageStats.getLastOperatorIndex();
    _jsonStatsByStage = jsonStatsByStage;
  }

  private ObjectNode selfNode(MultiStageOperator.Type type, Context context) {
    return selfNode(type, context, _index, new JsonNode[0]);
  }

  private ObjectNode selfNode(MultiStageOperator.Type type, Context context, int index, JsonNode[] childrenArr) {
    ObjectNode json = JsonUtils.newObjectNode();
    json.put("type", type.toString());
    for (Map.Entry<String, JsonNode> entry : _stageStats.getOperatorStats(index).asJson().properties()) {
      json.set(entry.getKey(), entry.getValue());
    }

    if (json.get("parallelism") == null) {
      json.put("parallelism", context._parallelism);
    }

    addClockTimeMs(type, json, childrenArr, context);
    addSelfAllocatedBytes(type, json, childrenArr, context);
    addSelfGcTime(type, json, childrenArr, context);

    if (childrenArr.length > 0) {
      json.set(CHILDREN_KEY, JsonUtils.objectToJsonNode(childrenArr));
    }

    return json;
  }

  private void addClockTimeMs(MultiStageOperator.Type type, ObjectNode selfNode, JsonNode[] children, Context context) {
    JsonNode executionTimeMs = selfNode.get("executionTimeMs");
    long cpuTimeMs = executionTimeMs == null ? 0 : executionTimeMs.asLong(0);

    selfNode.put("clockTimeMs", cpuTimeMs / context._parallelism);

    long childrenStat = getChildrenStat(type, children, "executionTimeMs");
    long selfExecutionTimeMs = cpuTimeMs - childrenStat;
    if (selfExecutionTimeMs != 0) {
      selfNode.put("selfExecutionTimeMs", selfExecutionTimeMs);
      selfNode.put("selfClockTimeMs", selfExecutionTimeMs / context._parallelism);
    }
  }

  private void addSelfAllocatedBytes(
      MultiStageOperator.Type type, ObjectNode selfNode, JsonNode[] children, Context context) {
    JsonNode allocatedBytes = selfNode.get("allocatedMemoryBytes");
    long totalAllocatedBytes = allocatedBytes == null ? 0 : allocatedBytes.asLong(0);

    long selfAllocatedBytes = totalAllocatedBytes - getChildrenStat(type, children, "allocatedMemoryBytes");
    if (selfAllocatedBytes != 0) {
      selfNode.put("selfAllocatedMB", selfAllocatedBytes / (1024 * 1024));
    }
  }

  private void addSelfGcTime(MultiStageOperator.Type type, ObjectNode selfNode, JsonNode[] children, Context context) {
    JsonNode gcTimeMs = selfNode.get("gcTimeMs");
    long totalGcTimeMs = gcTimeMs == null ? 0 : gcTimeMs.asLong(0);

    long selfGcTimeMs = totalGcTimeMs - getChildrenStat(type, children, "gcTimeMs");
    if (selfGcTimeMs != 0) {
      selfNode.put("selfGcTimeMs", selfGcTimeMs);
    }
  }

  private long getChildrenStat(MultiStageOperator.Type type, JsonNode[] children, String key) {
    if (children == null) {
      return 0;
    }
    return Arrays.stream(children)
            .mapToLong(child -> {
              JsonNode statNode = child.get(key);
              return statNode == null ? 0 : statNode.asLong(0);
            })
            .sum();
  }

  private ObjectNode recursiveCase(BasePlanNode node, MultiStageOperator.Type expectedType, Context context) {
    MultiStageOperator.Type type = _stageStats.getOperatorType(_index);
    /*
     Sometimes the operator type is not what we expect, but we can still build the tree
     This always happen in stage 0, in which case we have two operators but we only have stats for the receive
     operator.
     This may also happen leaf stages, in which case the all the stage but the send operator will be compiled into
     a single leaf node.
    */
    if (type != expectedType) {
      if (type == MultiStageOperator.Type.LEAF) {
        return selfNode(MultiStageOperator.Type.LEAF, context);
      }
      List<PlanNode> inputs = node.getInputs();
      int childrenSize = inputs.size();
      switch (childrenSize) {
        case 0:
          return JsonUtils.newObjectNode();
        case 1:
          return inputs.get(0).visit(this, context);
        default:
          ObjectNode json = JsonUtils.newObjectNode();
          ArrayNode children = JsonUtils.newArrayNode();
          for (int i = 0; i < childrenSize; i++) {
            _index--;
            if (inputs.size() > i) {
              children.add(inputs.get(i).visit(this, context));
            }
          }
          json.set(CHILDREN_KEY, children);
          return json;
      }
    }
    int selfIndex = _index;
    List<PlanNode> inputs = node.getInputs();
    int size = inputs.size();
    JsonNode[] childrenArr = new JsonNode[size];
    if (size > _index) {
      LOGGER.warn("Operator {} has {} inputs but only {} stats are left", type, size, _index);
      return selfNode(type, context);
    }
    for (int i = size - 1; i >= 0; i--) {
      PlanNode planNode = inputs.get(i);
      _index--;
      JsonNode child = planNode.visit(this, context);

      childrenArr[i] = child;
    }

    ObjectNode json = selfNode(type, context, selfIndex, childrenArr);
    return json;
  }

  @Override
  public ObjectNode visitAggregate(AggregateNode node, Context context) {
    return recursiveCase(node, MultiStageOperator.Type.AGGREGATE, context);
  }

  @Override
  public ObjectNode visitFilter(FilterNode node, Context context) {
    return recursiveCase(node, MultiStageOperator.Type.FILTER, context);
  }

  @Override
  public ObjectNode visitJoin(JoinNode node, Context context) {
    if (node.getJoinStrategy() == JoinNode.JoinStrategy.HASH) {
      return recursiveCase(node, MultiStageOperator.Type.HASH_JOIN, context);
    } else {
      assert node.getJoinStrategy() == JoinNode.JoinStrategy.LOOKUP;
      return recursiveCase(node, MultiStageOperator.Type.LOOKUP_JOIN, context);
    }
  }

  @Override
  public ObjectNode visitEnrichedJoin(EnrichedJoinNode node, Context context) {
    return visitJoin(node, context);
  }

  @Override
  public ObjectNode visitMailboxReceive(MailboxReceiveNode node, Context context) {
    ObjectNode json = selfNode(MultiStageOperator.Type.MAILBOX_RECEIVE, context);

    ArrayNode children = JsonUtils.newArrayNode();
    int senderStageId = node.getSenderStageId();
    children.add(_jsonStatsByStage.apply(senderStageId));
    json.set(CHILDREN_KEY, children);
    return json;
  }

  @Override
  public ObjectNode visitMailboxSend(MailboxSendNode node, Context context) {
    @SuppressWarnings("unchecked")
    StatMap<MailboxSendOperator.StatKey> operatorStats =
        (StatMap<MailboxSendOperator.StatKey>) _stageStats.getOperatorStats(_index);
    long parallelism = operatorStats.getLong(MailboxSendOperator.StatKey.PARALLELISM);
    Context myContext = new Context((int) parallelism);
    return recursiveCase(node, MultiStageOperator.Type.MAILBOX_SEND, myContext);
  }

  @Override
  public ObjectNode visitProject(ProjectNode node, Context context) {
    return recursiveCase(node, MultiStageOperator.Type.TRANSFORM, context);
  }

  @Override
  public ObjectNode visitSort(SortNode node, Context context) {
    return recursiveCase(node, MultiStageOperator.Type.SORT_OR_LIMIT, context);
  }

  @Override
  public ObjectNode visitTableScan(TableScanNode node, Context context) {
    return recursiveCase(node, MultiStageOperator.Type.LEAF, context);
  }

  @Override
  public ObjectNode visitValue(ValueNode node, Context context) {
    return recursiveCase(node, MultiStageOperator.Type.LITERAL, context);
  }

  @Override
  public ObjectNode visitWindow(WindowNode node, Context context) {
    return recursiveCase(node, MultiStageOperator.Type.WINDOW, context);
  }

  @Override
  public ObjectNode visitSetOp(SetOpNode node, Context context) {
    MultiStageOperator.Type type;
    switch (node.getSetOpType()) {
      case UNION:
        type = MultiStageOperator.Type.UNION;
        break;
      case INTERSECT:
        type = MultiStageOperator.Type.INTERSECT;
        break;
      case MINUS:
        type = MultiStageOperator.Type.MINUS;
        break;
      default:
        throw new IllegalStateException("Unexpected set op type: " + node.getSetOpType());
    }
    return recursiveCase(node, type, context);
  }

  @Override
  public ObjectNode visitExchange(ExchangeNode node, Context context) {
    throw new UnsupportedOperationException("ExchangeNode should not be visited");
  }

  @Override
  public ObjectNode visitExplained(ExplainedNode node, Context context) {
    throw new UnsupportedOperationException("ExplainedNode should not be visited");
  }

  public static class Context {
    private final int _parallelism;

    public Context(int parallelism) {
      _parallelism = parallelism;
    }
  }
}
