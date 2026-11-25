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


/// Builds a [Worker.MultiStageStatsTree] proto from an opchain's live operator tree and its accumulated
/// [MultiStageQueryStats]. Used by the stream-mode stats reporting path (gRPC `SubmitWithStream` RPC).
///
/// The encoder walks the operator tree in inorder (leftmost-leaf-first) in lock-step with the flat
/// [`_operatorTypes`]\[MultiStageQueryStats.StageStats#_operatorTypes\] /
/// [`_operatorStats`]\[MultiStageQueryStats.StageStats#_operatorStats\] lists, which are already maintained in
/// inorder (each operator appends its entry via
/// [`addLastOperator`]\[MultiStageQueryStats.StageStats.Open#addLastOperator\] just before emitting EOS). For each
/// operator the encoder produces a [Worker.StageStatsNode] carrying the operator type id, the serialized
/// [StatMap] bytes, the recursive children, and the stage-scoped plan-node ids gathered from the
/// [OpChainExecutionContext] during opchain construction.
///
/// Only the current-stage tree is encoded. Upstream stages reach the broker via the upstream opchains' own
/// reports in stream mode, so per-opchain reporting of upstream-stage trees is not needed.
///
/// **Pipeline breakers.** A pipeline breaker (dynamic-broadcast semi-join / lookup-join build side) runs as a
/// pre-stage sub-execution: its operators are folded into the leaf opchain's flat stats (an inorder prefix before
/// the `LEAF` entry — see `LeafOperator.calculateUpstreamStats`), but they are **not** children of the
/// `LeafOperator` in the live operator tree. To keep the tree walk aligned with the flat list, callers pass the
/// pipeline-breaker's root operator via `pipelineBreakerRoot`; the encoder grafts it as a child of the
/// `LEAF` node, so the encoded tree matches what the legacy mailbox path serializes
/// (`MAILBOX_SEND -> LEAF -> PIPELINE_BREAKER -> MAILBOX_RECEIVE`).
///
/// Two cases make the graft narrower than the live pipeline-breaker tree, and the encoder accounts for both so it
/// stays aligned with the fold rather than overshooting it:
///
/// - **Stage did not fold the breaker.** When `skip.pipeline.breaker.stats=true` the leaf opchain does
///      not fold the breaker at all (its flat stats are just `LEAF`, `MAILBOX_SEND`). Callers must then
///      pass `pipelineBreakerRoot == null` so nothing is grafted — even though a pipeline-breaker opchain ran.
/// - **Multiple build sides.** A breaker with several children folds only the first one, because
///      `PipelineBreakerOperator.calculateUpstreamStats` combines them via `MultiStageQueryStats.mergeUpstream`,
///      which drops the current-stage entries of every child but the first. The encoder grafts only the child
///      the fold kept (see [#keptPipelineBreakerChildren]), matching what the legacy path also emits.
///
/// A pipeline-breaker child is usually a single `MAILBOX_RECEIVE` operator, but a plugin opchain converter may build
/// it as a subtree (for example a root operator over the receive). The graft walks the kept child's whole subtree,
/// so it does not depend on the child's shape or operator types.
public final class MultiStageStatsTreeEncoder {
  private MultiStageStatsTreeEncoder() {
  }

  /// Convenience overload that resolves plan-node ids from the [OpChainExecutionContext]. This is the form
  /// called from production code (the opchain completion callback in `QueryServer`).
  public static Worker.MultiStageStatsTree encode(MultiStageOperator root, MultiStageQueryStats stats,
      OpChainExecutionContext context)
      throws IOException {
    return encode(root, stats, context, null);
  }

  /// As [#encode(MultiStageOperator, MultiStageQueryStats, OpChainExecutionContext)] but grafts the given
  /// pipeline-breaker root operator as a child of the `LEAF` node (see the class Javadoc). Pass `null`
  /// when the stage has no pipeline breaker.
  public static Worker.MultiStageStatsTree encode(MultiStageOperator root, MultiStageQueryStats stats,
      OpChainExecutionContext context, @Nullable MultiStageOperator pipelineBreakerRoot)
      throws IOException {
    return encode(root, stats, op -> resolvePlanNodeIds(op, context), pipelineBreakerRoot);
  }

  /// Encodes the current-stage operator tree + stats into a [Worker.MultiStageStatsTree]. Tests use this entry
  /// point with a custom `planNodeIdResolver` so they don't need to construct a full
  /// [OpChainExecutionContext].
  ///
  /// **All-or-nothing contract:** this method either returns a fully-built [Worker.MultiStageStatsTree]
  /// or throws without returning any partial result. Callers cannot recover partial stats on failure. Specifically:
  ///
  /// - The upfront `treeSize != flatSize` check throws [IllegalStateException] before any proto
  ///      node is allocated.
  /// - An [java.io.IOException] from [#serializeStatMap] during node traversal leaves
  ///      [Worker.StageStatsNode] builders only on the Java call stack; they are discarded as the exception
  ///      unwinds. No partially-built tree is reachable by the caller.
  ///
  /// @throws IllegalStateException if the operator tree shape does not align with the flat stats list (missing
  ///     entries — typically caused by an operator that failed before emitting EOS).
  /// @throws java.io.IOException if stat-map serialization fails for any node.
  public static Worker.MultiStageStatsTree encode(MultiStageOperator root, MultiStageQueryStats stats,
      Function<MultiStageOperator, List<Integer>> planNodeIdResolver)
      throws IOException {
    return encode(root, stats, planNodeIdResolver, null);
  }

  /// As [#encode(MultiStageOperator, MultiStageQueryStats, Function)] but grafts `pipelineBreakerRoot`
  /// (if non-null) as a child of the `LEAF` node so the walked operator tree aligns with the leaf opchain's
  /// folded flat stats. See the class Javadoc.
  public static Worker.MultiStageStatsTree encode(MultiStageOperator root, MultiStageQueryStats stats,
      Function<MultiStageOperator, List<Integer>> planNodeIdResolver,
      @Nullable MultiStageOperator pipelineBreakerRoot)
      throws IOException {
    MultiStageQueryStats.StageStats.Open openStats = stats.getCurrentStats();
    int flatSize = openStats.getLastOperatorIndex() + 1;
    int keptPipelineBreakerChildren =
        keptPipelineBreakerChildren(openStats, flatSize, pipelineBreakerRoot, stats.getCurrentStageId());
    int treeSize = countOperators(root, pipelineBreakerRoot, keptPipelineBreakerChildren);
    if (treeSize != flatSize) {
      throw new IllegalStateException("Operator tree size (" + treeSize + ") does not match flat stats list size ("
          + flatSize + ") for stage " + stats.getCurrentStageId()
          + ". This usually means an operator failed before emitting EOS.");
    }
    int[] cursor = new int[]{0};
    Worker.StageStatsNode rootNode =
        encodeNode(root, openStats, cursor, planNodeIdResolver, pipelineBreakerRoot, keptPipelineBreakerChildren);
    return Worker.MultiStageStatsTree.newBuilder()
        .setCurrentStageId(stats.getCurrentStageId())
        .setCurrentStage(rootNode)
        .build();
  }

  private static Worker.StageStatsNode encodeNode(MultiStageOperator op,
      MultiStageQueryStats.StageStats openStats, int[] cursor,
      Function<MultiStageOperator, List<Integer>> planNodeIdResolver,
      @Nullable MultiStageOperator pipelineBreakerRoot, int keptPipelineBreakerChildren)
      throws IOException {
    Worker.StageStatsNode.Builder builder = Worker.StageStatsNode.newBuilder();
    // Inorder: encode children first.
    for (MultiStageOperator child : effectiveChildren(op, pipelineBreakerRoot, keptPipelineBreakerChildren)) {
      builder.addChildren(
          encodeNode(child, openStats, cursor, planNodeIdResolver, pipelineBreakerRoot, keptPipelineBreakerChildren));
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

  private static int countOperators(MultiStageOperator op, @Nullable MultiStageOperator pipelineBreakerRoot,
      int keptPipelineBreakerChildren) {
    int count = 1;
    for (MultiStageOperator child : effectiveChildren(op, pipelineBreakerRoot, keptPipelineBreakerChildren)) {
      count += countOperators(child, pipelineBreakerRoot, keptPipelineBreakerChildren);
    }
    return count;
  }

  /// How many of the pipeline breaker's children the leaf opchain's folded flat stats actually kept. The fold
  /// (`LeafOperator.calculateUpstreamStats`) prepends the pipeline breaker as an inorder prefix: the entries of its
  /// first child's subtree followed by a single `PIPELINE_BREAKER`. Only the first child survives because
  /// `PipelineBreakerOperator.calculateUpstreamStats` combines its children via `MultiStageQueryStats.mergeUpstream`,
  /// which drops the current-stage entries of the merged-in children. Grafting every live child would make the tree
  /// larger than the flat list (`treeSize > flatSize`).
  ///
  /// Returns 0 when there is no pipeline breaker to graft. Callers must pass `pipelineBreakerRoot == null`
  /// when the stage did not fold a pipeline breaker (e.g. `skip.pipeline.breaker.stats=true`); otherwise the
  /// leading entries are not the breaker's and this throws.
  ///
  /// @throws IllegalStateException if a pipeline breaker is expected but the flat list does not hold the first
  ///     child's entries followed by `PIPELINE_BREAKER` — a genuine shape mismatch we must not silently encode.
  private static int keptPipelineBreakerChildren(MultiStageQueryStats.StageStats openStats, int flatSize,
      @Nullable MultiStageOperator pipelineBreakerRoot, int stageId) {
    if (pipelineBreakerRoot == null) {
      return 0;
    }
    List<MultiStageOperator> children = pipelineBreakerRoot.getChildOperators();
    int kept = children.isEmpty() ? 0 : 1;
    int prefixSize = kept == 0 ? 0 : countOperators(children.get(0), null, 0);
    if (prefixSize >= flatSize || openStats.getOperatorType(prefixSize) != MultiStageOperator.Type.PIPELINE_BREAKER) {
      throw new IllegalStateException("Expected a PIPELINE_BREAKER entry after the " + prefixSize + " entries of the "
          + "pipeline breaker's first child in the folded leaf stats for stage " + stageId + ", but found "
          + (prefixSize >= flatSize ? "end of list" : openStats.getOperatorType(prefixSize).name()));
    }
    return kept;
  }

  /// The operators to recurse into for `op`. This is `op.getChildOperators()`, except for a folded
  /// pipeline breaker, which ran pre-stage and is not a live child of the `LeafOperator` even though its
  /// operators occupy the inorder prefix of the leaf opchain's flat stats:
  ///
  /// - at the `LEAF` node the pipeline-breaker root is grafted as an extra child, and
  /// - at the grafted pipeline-breaker root itself only the first `keptPipelineBreakerChildren` children are
  ///      walked — the children the fold actually kept (see [#keptPipelineBreakerChildren]).
  ///
  /// The graft only applies at the `LEAF` (the sole place a pipeline breaker folds into); the pipeline-breaker
  /// subtree itself contains no `LEAF`, so there is no double-graft.
  private static List<MultiStageOperator> effectiveChildren(MultiStageOperator op,
      @Nullable MultiStageOperator pipelineBreakerRoot, int keptPipelineBreakerChildren) {
    if (pipelineBreakerRoot != null) {
      if (op == pipelineBreakerRoot) {
        List<MultiStageOperator> children = op.getChildOperators();
        int kept = Math.min(keptPipelineBreakerChildren, children.size());
        return kept == children.size() ? children : children.subList(0, kept);
      }
      if (op.getOperatorType() == MultiStageOperator.Type.LEAF) {
        List<MultiStageOperator> withGraft = new ArrayList<>(op.getChildOperators());
        withGraft.add(pipelineBreakerRoot);
        return withGraft;
      }
    }
    return op.getChildOperators();
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
