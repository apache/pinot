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
/// - **Multiple build sides.** A breaker with several `MAILBOX_RECEIVE` children folds to _fewer_
///      receive entries than it has children, because `PipelineBreakerOperator.calculateUpstreamStats`
///      collapses same-stage receives via `MultiStageQueryStats.mergeUpstream`. The encoder grafts only the
///      receives the fold kept (see [#keptPipelineBreakerReceives]), matching what the legacy path also emits.
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
    int keptPipelineBreakerReceives =
        keptPipelineBreakerReceives(openStats, flatSize, pipelineBreakerRoot, stats.getCurrentStageId());
    int treeSize = countOperators(root, pipelineBreakerRoot, keptPipelineBreakerReceives);
    if (treeSize != flatSize) {
      throw new IllegalStateException("Operator tree size (" + treeSize + ") does not match flat stats list size ("
          + flatSize + ") for stage " + stats.getCurrentStageId()
          + ". This usually means an operator failed before emitting EOS.");
    }
    int[] cursor = new int[]{0};
    Worker.StageStatsNode rootNode =
        encodeNode(root, openStats, cursor, planNodeIdResolver, pipelineBreakerRoot, keptPipelineBreakerReceives);
    return Worker.MultiStageStatsTree.newBuilder()
        .setCurrentStageId(stats.getCurrentStageId())
        .setCurrentStage(rootNode)
        .build();
  }

  private static Worker.StageStatsNode encodeNode(MultiStageOperator op,
      MultiStageQueryStats.StageStats openStats, int[] cursor,
      Function<MultiStageOperator, List<Integer>> planNodeIdResolver,
      @Nullable MultiStageOperator pipelineBreakerRoot, int keptPipelineBreakerReceives)
      throws IOException {
    Worker.StageStatsNode.Builder builder = Worker.StageStatsNode.newBuilder();
    // Inorder: encode children first.
    for (MultiStageOperator child : effectiveChildren(op, pipelineBreakerRoot, keptPipelineBreakerReceives)) {
      builder.addChildren(
          encodeNode(child, openStats, cursor, planNodeIdResolver, pipelineBreakerRoot, keptPipelineBreakerReceives));
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
      int keptPipelineBreakerReceives) {
    int count = 1;
    for (MultiStageOperator child : effectiveChildren(op, pipelineBreakerRoot, keptPipelineBreakerReceives)) {
      count += countOperators(child, pipelineBreakerRoot, keptPipelineBreakerReceives);
    }
    return count;
  }

  /// How many of the pipeline breaker's `MAILBOX_RECEIVE` children the leaf opchain's folded flat stats actually
  /// kept. The fold (`LeafOperator.calculateUpstreamStats`) prepends the pipeline breaker as an inorder prefix
  /// of `MAILBOX_RECEIVE` entries followed by a single `PIPELINE_BREAKER`. That prefix can hold
  /// _fewer_ receives than the live [MultiStageOperator.Type#PIPELINE_BREAKER] operator has children,
  /// because `PipelineBreakerOperator.calculateUpstreamStats` combines its same-stage receives via
  /// `MultiStageQueryStats.mergeUpstream`, which collapses their current-stage entries (only the first receive's
  /// entry survives). We therefore graft only the receives the fold kept; grafting the full live receive list would
  /// make the tree larger than the flat list (`treeSize > flatSize`).
  ///
  /// Returns 0 when there is no pipeline breaker to graft. Callers must pass `pipelineBreakerRoot == null`
  /// when the stage did not fold a pipeline breaker (e.g. `skip.pipeline.breaker.stats=true`); otherwise the
  /// leading entries are not the breaker's and this throws.
  ///
  /// @throws IllegalStateException if a pipeline breaker is expected but the flat list does not start with
  ///     `MAILBOX_RECEIVE`* then `PIPELINE_BREAKER` — a genuine shape mismatch we must not silently encode.
  private static int keptPipelineBreakerReceives(MultiStageQueryStats.StageStats openStats, int flatSize,
      @Nullable MultiStageOperator pipelineBreakerRoot, int stageId) {
    if (pipelineBreakerRoot == null) {
      return 0;
    }
    int receives = 0;
    while (receives < flatSize && openStats.getOperatorType(receives) == MultiStageOperator.Type.MAILBOX_RECEIVE) {
      receives++;
    }
    if (receives >= flatSize || openStats.getOperatorType(receives) != MultiStageOperator.Type.PIPELINE_BREAKER) {
      throw new IllegalStateException("Expected a PIPELINE_BREAKER entry after " + receives + " leading "
          + "MAILBOX_RECEIVE entries in the folded leaf stats for stage " + stageId + ", but found "
          + (receives >= flatSize ? "end of list" : openStats.getOperatorType(receives).name()));
    }
    return receives;
  }

  /// The operators to recurse into for `op`. This is `op.getChildOperators()`, except for a folded
  /// pipeline breaker, which ran pre-stage and is not a live child of the `LeafOperator` even though its
  /// operators occupy the inorder prefix of the leaf opchain's flat stats:
  ///
  /// - at the `LEAF` node the pipeline-breaker root is grafted as an extra child, and
  /// - at the grafted pipeline-breaker root itself only the first `keptPipelineBreakerReceives` children are
  ///      walked — the receives the fold actually kept (see [#keptPipelineBreakerReceives]).
  ///
  /// The graft only applies at the `LEAF` (the sole place a pipeline breaker folds into); the pipeline-breaker
  /// subtree itself contains no `LEAF`, so there is no double-graft.
  private static List<MultiStageOperator> effectiveChildren(MultiStageOperator op,
      @Nullable MultiStageOperator pipelineBreakerRoot, int keptPipelineBreakerReceives) {
    if (pipelineBreakerRoot != null) {
      if (op == pipelineBreakerRoot) {
        List<MultiStageOperator> receives = op.getChildOperators();
        int kept = Math.min(keptPipelineBreakerReceives, receives.size());
        return kept == receives.size() ? receives : receives.subList(0, kept);
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
