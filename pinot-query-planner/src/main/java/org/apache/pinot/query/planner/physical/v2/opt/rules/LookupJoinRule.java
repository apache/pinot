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
package org.apache.pinot.query.planner.physical.v2.opt.rules;

import java.util.ArrayList;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.core.Join;
import org.apache.pinot.calcite.rel.hint.PinotHintOptions;
import org.apache.pinot.calcite.rel.traits.PinotExecStrategyTrait;
import org.apache.pinot.query.context.PhysicalPlannerContext;
import org.apache.pinot.query.planner.physical.v2.ExchangeStrategy;
import org.apache.pinot.query.planner.physical.v2.PRelNode;
import org.apache.pinot.query.planner.physical.v2.PinotDataDistribution;
import org.apache.pinot.query.planner.physical.v2.nodes.PhysicalExchange;
import org.apache.pinot.query.planner.physical.v2.opt.PRelNodeTransformer;


/**
 * Post-pass rule that isolates every lookup join in its own plan fragment.
 *
 * <p>Runs after {@link WorkerExchangeAssignmentRule}, which assigns workers and exchanges generically (with zero
 * lookup join awareness). This rule then post-processes lookup joins to ensure:</p>
 * <ol>
 *   <li><b>Right side</b>: Exchange converted to {@link ExchangeStrategy#LOOKUP_LOCAL_EXCHANGE} — a pseudo-exchange
 *       that does not split fragments. Later, during plan fragment assignment,
 *       {@code PlanFragmentAndMailboxAssignment.processLookupLocalExchange} registers the dim table with fake
 *       segments so the fragment is classified as a leaf stage for {@code DimensionTableDataManager} access.</li>
 *   <li><b>Left side</b>: Exchange exists (inserts {@link ExchangeStrategy#IDENTITY_EXCHANGE} if missing, e.g. when
 *       the left input is an intermediate node like a hash join with the same workers).</li>
 *   <li><b>Above</b>: Lookup join wrapped in {@link ExchangeStrategy#IDENTITY_EXCHANGE} so downstream nodes
 *       (other joins, sorts) are not absorbed into the lookup join's leaf fragment.</li>
 * </ol>
 *
 * <p>This achieves the same outcome as V1, where {@code WorkerManager.assignWorkersToNonRootFragment} detects
 * lookup joins and assigns the same workers as the fact table while registering fake empty segments for the
 * dim table.</p>
 */
public class LookupJoinRule implements PRelNodeTransformer {
  private final PhysicalPlannerContext _context;

  public LookupJoinRule(PhysicalPlannerContext context) {
    _context = context;
  }

  @Override
  public PRelNode execute(PRelNode rootNode) {
    // TODO: Support lookup joins in Lite mode. Currently fails with "Right input must be leaf
    //  operator" because the broker lacks DimensionTableDataManager.
    if (_context.isUseLiteMode()) {
      return rootNode;
    }
    return transform(rootNode, null);
  }

  private PRelNode transform(PRelNode node, @Nullable PRelNode parent) {
    // Post-order: transform children first
    List<PRelNode> newInputs = new ArrayList<>();
    boolean changed = false;
    for (PRelNode input : node.getPRelInputs()) {
      PRelNode transformed = transform(input, node);
      newInputs.add(transformed);
      if (transformed != input) {
        changed = true;
      }
    }
    if (changed) {
      node = node.with(newInputs);
    }

    if (!isLookupJoin(node)) {
      return node;
    }

    // Right side: convert existing exchange to LOOKUP_LOCAL_EXCHANGE
    node = convertRightExchange(node);

    // Left side: ensure exchange exists (insert IDENTITY if missing)
    node = ensureLeftExchange(node);

    // Above: wrap this node in IDENTITY_EXCHANGE so downstream nodes don't enter the leaf fragment
    if (parent != null) {
      return wrapWithIdentityExchange(node);
    }
    return node;
  }

  /**
   * Converts the right-side exchange to LOOKUP_LOCAL_EXCHANGE. The right side should already have an exchange
   * from {@link WorkerExchangeAssignmentRule} (IDENTITY at leaf boundary, or RANDOM/BROADCAST if workers differ).
   * If no exchange exists (shouldn't happen), inserts LOOKUP_LOCAL_EXCHANGE directly.
   */
  private PRelNode convertRightExchange(PRelNode joinNode) {
    if (joinNode.getPRelInputs().size() < 2) {
      return joinNode;
    }
    PRelNode rightInput = joinNode.getPRelInput(1);

    if (!(rightInput instanceof PhysicalExchange)) {
      // No exchange on right side — insert LOOKUP_LOCAL_EXCHANGE wrapping the right input.
      return joinNode.with(replaceInput(joinNode, 1, createLookupLocalExchange(rightInput, joinNode)));
    }

    PhysicalExchange existing = (PhysicalExchange) rightInput;
    if (existing.getExchangeStrategy() == ExchangeStrategy.LOOKUP_LOCAL_EXCHANGE) {
      return joinNode; // already converted (idempotent)
    }

    // Replace with LOOKUP_LOCAL_EXCHANGE using the JOIN's distribution (not the existing exchange's).
    // The existing exchange may carry the dim table's leaf-stage distribution (different workers/hash),
    // but LOOKUP_LOCAL needs the join's workers since the dim table is accessed locally on each join worker
    // via DimensionTableDataManager. Any collation from the existing exchange is intentionally dropped —
    // lookup joins don't require sorted dim table input.
    return joinNode.with(replaceInput(joinNode, 1,
        createLookupLocalExchange(existing.getPRelInput(0), joinNode)));
  }

  /**
   * Ensures the left input has an exchange. If the left input is already a PhysicalExchange (e.g. IDENTITY from
   * leaf boundary), do nothing. Otherwise insert IDENTITY_EXCHANGE to create a fragment boundary.
   */
  private PRelNode ensureLeftExchange(PRelNode joinNode) {
    if (joinNode.getPRelInputs().isEmpty()) {
      return joinNode;
    }
    PRelNode leftInput = joinNode.getPRelInput(0);
    if (leftInput instanceof PhysicalExchange) {
      return joinNode; // already has exchange
    }
    PhysicalExchange identity = new PhysicalExchange(nodeId(), leftInput,
        leftInput.getPinotDataDistributionOrThrow(), List.of(), ExchangeStrategy.IDENTITY_EXCHANGE, null,
        PinotExecStrategyTrait.getDefaultExecStrategy(), _context.getDefaultHashFunction());
    return joinNode.with(replaceInput(joinNode, 0, identity));
  }

  /**
   * Wraps the lookup join node in an IDENTITY_EXCHANGE, creating a fragment boundary above it.
   */
  private PhysicalExchange wrapWithIdentityExchange(PRelNode joinNode) {
    return new PhysicalExchange(nodeId(), joinNode, joinNode.getPinotDataDistributionOrThrow(), List.of(),
        ExchangeStrategy.IDENTITY_EXCHANGE, null, PinotExecStrategyTrait.getDefaultExecStrategy(),
        _context.getDefaultHashFunction());
  }

  /**
   * Creates a LOOKUP_LOCAL_EXCHANGE wrapping the given child node, using the join's worker distribution.
   */
  private PhysicalExchange createLookupLocalExchange(PRelNode child, PRelNode joinNode) {
    PinotDataDistribution joinDist = joinNode.getPinotDataDistributionOrThrow();
    RelDistribution.Type distType = joinDist.getWorkers().size() == 1
        ? RelDistribution.Type.SINGLETON : RelDistribution.Type.RANDOM_DISTRIBUTED;
    PinotDataDistribution newDist = new PinotDataDistribution(distType,
        joinDist.getWorkers(), joinDist.getWorkerHash(), null, null);
    return new PhysicalExchange(nodeId(), child, newDist, List.of(), ExchangeStrategy.LOOKUP_LOCAL_EXCHANGE, null,
        PinotExecStrategyTrait.getDefaultExecStrategy(), _context.getDefaultHashFunction());
  }

  private static List<PRelNode> replaceInput(PRelNode node, int index, PRelNode newInput) {
    List<PRelNode> inputs = new ArrayList<>(node.getPRelInputs());
    inputs.set(index, newInput);
    return inputs;
  }

  private static boolean isLookupJoin(PRelNode node) {
    return node.unwrap() instanceof Join
        && PinotHintOptions.JoinHintOptions.useLookupJoinStrategy((Join) node.unwrap());
  }

  private int nodeId() {
    return _context.getNodeIdGenerator().get();
  }
}
