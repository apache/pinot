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
package org.apache.pinot.query.runtime.plan;

import com.google.protobuf.ByteString;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.pinot.common.datatable.StatMap;
import org.apache.pinot.common.proto.Worker;
import org.apache.pinot.query.runtime.operator.OperatorTypeDescriptor;
import org.apache.pinot.query.runtime.operator.OperatorTypeRegistry;


/**
 * Broker-side decoder turning {@link Worker.MultiStageStatsTree} payloads into {@link StageStatsTreeNode} instances
 * the broker accumulates by stage id (and merges across worker reports for the same stage).
 *
 * <p>Pairs with {@link MultiStageStatsTreeEncoder} on the server side.
 */
public final class MultiStageStatsTreeDecoder {
  private MultiStageStatsTreeDecoder() {
  }

  /**
   * Thrown when a payload cannot be decoded — usually because the operator type id is unknown to this broker
   * (newer-server / older-broker case). The broker logs and marks the stage {@code mergeFailed}; the query continues.
   */
  public static class DecodeFailedException extends Exception {
    public DecodeFailedException(String message) {
      super(message);
    }

    public DecodeFailedException(String message, Throwable cause) {
      super(message, cause);
    }
  }

  /**
   * Decodes a single {@link Worker.StageStatsNode} (recursive). Used directly when the caller already has a node and
   * a known stage id.
   */
  public static StageStatsTreeNode decodeNode(Worker.StageStatsNode node)
      throws DecodeFailedException {
    OperatorTypeDescriptor type = OperatorTypeRegistry.fromId(node.getOperatorTypeId());
    if (type == null) {
      throw new DecodeFailedException("Unknown operator type id: " + node.getOperatorTypeId());
    }
    StatMap<?> statMap;
    try {
      statMap = deserializeStatMap(node.getStatMap(), type);
    } catch (IOException e) {
      throw new DecodeFailedException("Failed to deserialize StatMap for operator type " + type.name(), e);
    }
    List<StageStatsTreeNode> children = new ArrayList<>(node.getChildrenCount());
    for (Worker.StageStatsNode child : node.getChildrenList()) {
      children.add(decodeNode(child));
    }
    return new StageStatsTreeNode(type, node.getPlanNodeIdsList(), statMap, children);
  }

  /**
   * Result of decoding a {@link Worker.MultiStageStatsTree}: the current-stage tree plus any upstream-stage trees the
   * opchain attached to its report (e.g. from pipeline-breaker stats).
   */
  public static final class Decoded {
    private final int _currentStageId;
    private final StageStatsTreeNode _currentStage;
    private final Map<Integer, StageStatsTreeNode> _upstreamStages;

    public Decoded(int currentStageId, StageStatsTreeNode currentStage,
        Map<Integer, StageStatsTreeNode> upstreamStages) {
      _currentStageId = currentStageId;
      _currentStage = currentStage;
      _upstreamStages = upstreamStages;
    }

    public int getCurrentStageId() {
      return _currentStageId;
    }

    public StageStatsTreeNode getCurrentStage() {
      return _currentStage;
    }

    public Map<Integer, StageStatsTreeNode> getUpstreamStages() {
      return _upstreamStages;
    }
  }

  /**
   * Decodes a full {@link Worker.MultiStageStatsTree} into a {@link Decoded} containing the current-stage tree and a
   * map of upstream-stage trees keyed by stage id. Throws {@link DecodeFailedException} on any decode error; the
   * caller is responsible for logging and marking {@code mergeFailed}.
   */
  public static Decoded decode(Worker.MultiStageStatsTree proto)
      throws DecodeFailedException {
    StageStatsTreeNode currentStage = decodeNode(proto.getCurrentStage());
    Map<Integer, StageStatsTreeNode> upstreamStages = new HashMap<>(proto.getUpstreamStagesCount());
    for (Map.Entry<Integer, Worker.StageStatsNode> entry : proto.getUpstreamStagesMap().entrySet()) {
      upstreamStages.put(entry.getKey(), decodeNode(entry.getValue()));
    }
    return new Decoded(proto.getCurrentStageId(), currentStage, upstreamStages);
  }

  // The Type -> StatKey class relationship is preserved by MultiStageOperator.Type, but Java's type system can't
  // express the dependent type, so we suppress the resulting unchecked warning here.
  @SuppressWarnings({"unchecked", "rawtypes"})
  private static StatMap<?> deserializeStatMap(ByteString bytes, OperatorTypeDescriptor type)
      throws IOException {
    try (DataInputStream input = new DataInputStream(bytes.newInput())) {
      return StatMap.deserialize(input, (Class) type.getStatKeyClass());
    }
  }
}
