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
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import javax.annotation.Nullable;
import org.apache.commons.io.output.UnsynchronizedByteArrayOutputStream;
import org.apache.pinot.common.datatable.StatMap;
import org.apache.pinot.common.proto.Worker;
import org.apache.pinot.query.planner.plannode.PlanNode;
import org.apache.pinot.query.runtime.operator.MultiStageOperator;
import org.apache.pinot.query.runtime.operator.OperatorTypeDescriptor;


/**
 * Builds a {@link Worker.MultiStageStatsTree} proto from an opchain's live operator tree and its accumulated
 * {@link MultiStageQueryStats}. Used by the stream-mode stats reporting path (gRPC {@code SubmitWithStream} RPC).
 *
 * <p>The encoder walks the operator tree in inorder (leftmost-leaf-first) in lock-step with the flat
 * {@link MultiStageQueryStats.StageStats#_operatorTypes _operatorTypes} /
 * {@link MultiStageQueryStats.StageStats#_operatorStats _operatorStats} lists, which are already maintained in
 * inorder (each operator appends its entry via
 * {@link MultiStageQueryStats.StageStats.Open#addLastOperator addLastOperator} just before emitting EOS). For each
 * operator the encoder produces a {@link Worker.StageStatsNode} carrying the operator type id, the serialized
 * {@link StatMap} bytes, the recursive children, and the stage-scoped plan-node ids gathered from the
 * {@link OpChainExecutionContext} during opchain construction.
 *
 * <p>Only the current-stage tree is encoded. Upstream stages reach the broker via the upstream opchains' own
 * reports in stream mode, so per-opchain reporting of upstream-stage trees is not needed.
 *
 * <p><b>Pipeline breakers.</b> A pipeline breaker (dynamic-broadcast semi-join / lookup-join build side) runs as a
 * pre-stage sub-execution: its operators are folded into the leaf opchain's flat stats (an inorder prefix before
 * the {@code LEAF} entry — see {@code LeafOperator.calculateUpstreamStats}), but they are <b>not</b> children of the
 * {@code LeafOperator} in the live operator tree. To keep the tree walk aligned with the flat list, callers pass the
 * pipeline-breaker's root operator via {@code pipelineBreakerRoot}; the encoder grafts it as a child of the
 * {@code LEAF} node, so the encoded tree matches what the legacy mailbox path serializes
 * ({@code MAILBOX_SEND -> LEAF -> PIPELINE_BREAKER -> MAILBOX_RECEIVE}).
 */
public final class MultiStageStatsTreeEncoder {
  private MultiStageStatsTreeEncoder() {
  }

  /**
   * Convenience overload that resolves plan-node ids from the {@link OpChainExecutionContext}. This is the form
   * called from production code (the opchain completion callback in {@code QueryServer}).
   */
  public static Worker.MultiStageStatsTree encode(MultiStageOperator root, MultiStageQueryStats stats,
      OpChainExecutionContext context)
      throws IOException {
    return encode(root, stats, context, null);
  }

  /**
   * As {@link #encode(MultiStageOperator, MultiStageQueryStats, OpChainExecutionContext)} but grafts the given
   * pipeline-breaker root operator as a child of the {@code LEAF} node (see the class Javadoc). Pass {@code null}
   * when the stage has no pipeline breaker.
   */
  public static Worker.MultiStageStatsTree encode(MultiStageOperator root, MultiStageQueryStats stats,
      OpChainExecutionContext context, @Nullable MultiStageOperator pipelineBreakerRoot)
      throws IOException {
    return encode(root, stats, op -> resolvePlanNodeIds(op, context), pipelineBreakerRoot);
  }

  /**
   * Encodes the current-stage operator tree + stats into a {@link Worker.MultiStageStatsTree}. Tests use this entry
   * point with a custom {@code planNodeIdResolver} so they don't need to construct a full
   * {@link OpChainExecutionContext}.
   *
   * <p><b>All-or-nothing contract:</b> this method either returns a fully-built {@link Worker.MultiStageStatsTree}
   * or throws without returning any partial result. Callers cannot recover partial stats on failure. Specifically:
   * <ul>
   *   <li>The upfront {@code treeSize != flatSize} check throws {@link IllegalStateException} before any proto
   *       node is allocated.</li>
   *   <li>An {@link java.io.IOException} from {@link #serializeStatMap} during node traversal leaves
   *       {@link Worker.StageStatsNode} builders only on the Java call stack; they are discarded as the exception
   *       unwinds. No partially-built tree is reachable by the caller.</li>
   * </ul>
   *
   * @throws IllegalStateException if the operator tree shape does not align with the flat stats list (missing
   *     entries — typically caused by an operator that failed before emitting EOS).
   * @throws java.io.IOException if stat-map serialization fails for any node.
   */
  public static Worker.MultiStageStatsTree encode(MultiStageOperator root, MultiStageQueryStats stats,
      Function<MultiStageOperator, List<Integer>> planNodeIdResolver)
      throws IOException {
    return encode(root, stats, planNodeIdResolver, null);
  }

  /**
   * As {@link #encode(MultiStageOperator, MultiStageQueryStats, Function)} but grafts {@code pipelineBreakerRoot}
   * (if non-null) as a child of the {@code LEAF} node so the walked operator tree aligns with the leaf opchain's
   * folded flat stats. See the class Javadoc.
   */
  public static Worker.MultiStageStatsTree encode(MultiStageOperator root, MultiStageQueryStats stats,
      Function<MultiStageOperator, List<Integer>> planNodeIdResolver,
      @Nullable MultiStageOperator pipelineBreakerRoot)
      throws IOException {
    MultiStageQueryStats.StageStats.Open openStats = stats.getCurrentStats();
    int treeSize = countOperators(root, pipelineBreakerRoot);
    int flatSize = openStats.getLastOperatorIndex() + 1;
    if (treeSize != flatSize) {
      throw new IllegalStateException("Operator tree size (" + treeSize + ") does not match flat stats list size ("
          + flatSize + ") for stage " + stats.getCurrentStageId()
          + ". This usually means an operator failed before emitting EOS.");
    }
    int[] cursor = new int[]{0};
    Worker.StageStatsNode rootNode = encodeNode(root, openStats, cursor, planNodeIdResolver, pipelineBreakerRoot);
    return Worker.MultiStageStatsTree.newBuilder()
        .setCurrentStageId(stats.getCurrentStageId())
        .setCurrentStage(rootNode)
        .build();
  }

  private static Worker.StageStatsNode encodeNode(MultiStageOperator op,
      MultiStageQueryStats.StageStats openStats, int[] cursor,
      Function<MultiStageOperator, List<Integer>> planNodeIdResolver,
      @Nullable MultiStageOperator pipelineBreakerRoot)
      throws IOException {
    Worker.StageStatsNode.Builder builder = Worker.StageStatsNode.newBuilder();
    // Inorder: encode children first.
    for (MultiStageOperator child : effectiveChildren(op, pipelineBreakerRoot)) {
      builder.addChildren(encodeNode(child, openStats, cursor, planNodeIdResolver, pipelineBreakerRoot));
    }
    int idx = cursor[0]++;
    OperatorTypeDescriptor type = openStats.getOperatorType(idx);
    StatMap<?> statMap = openStats.getOperatorStats(idx);
    builder.setOperatorTypeId(type.getId());
    builder.setStatMap(serializeStatMap(statMap));
    for (Integer id : planNodeIdResolver.apply(op)) {
      if (id != null && id >= 0) {
        builder.addPlanNodeIds(id);
      }
    }
    return builder.build();
  }

  private static int countOperators(MultiStageOperator op, @Nullable MultiStageOperator pipelineBreakerRoot) {
    int count = 1;
    for (MultiStageOperator child : effectiveChildren(op, pipelineBreakerRoot)) {
      count += countOperators(child, pipelineBreakerRoot);
    }
    return count;
  }

  /**
   * The operators to recurse into for {@code op}. This is {@code op.getChildOperators()}, except that a folded
   * pipeline breaker is grafted as a child of the {@code LEAF} node: the pipeline breaker ran pre-stage and is not a
   * live child of the {@code LeafOperator}, but its operators occupy the inorder prefix of the leaf opchain's flat
   * stats, so grafting it here realigns the tree walk with the flat list. The graft only applies at the {@code LEAF}
   * (the sole place a pipeline breaker folds into); the pipeline-breaker subtree itself contains no {@code LEAF}, so
   * there is no double-graft.
   */
  private static List<MultiStageOperator> effectiveChildren(MultiStageOperator op,
      @Nullable MultiStageOperator pipelineBreakerRoot) {
    List<MultiStageOperator> children = op.getChildOperators();
    if (pipelineBreakerRoot == null || op.getOperatorType() != MultiStageOperator.Type.LEAF) {
      return children;
    }
    List<MultiStageOperator> withGraft = new ArrayList<>(children);
    withGraft.add(pipelineBreakerRoot);
    return withGraft;
  }

  private static ByteString serializeStatMap(StatMap<?> statMap)
      throws IOException {
    try (UnsynchronizedByteArrayOutputStream baos = new UnsynchronizedByteArrayOutputStream.Builder().get();
        DataOutputStream output = new DataOutputStream(baos)) {
      statMap.serialize(output);
      output.flush();
      return ByteString.copyFrom(baos.toByteArray());
    }
  }

  private static List<Integer> resolvePlanNodeIds(MultiStageOperator op, OpChainExecutionContext context) {
    return context.getPlanNodesForOperator(op).stream()
        .map((PlanNode pn) -> context.getPlanNodeId(pn))
        .toList();
  }
}
