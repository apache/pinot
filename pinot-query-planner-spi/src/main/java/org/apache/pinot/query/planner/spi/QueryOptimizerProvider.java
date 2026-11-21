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
package org.apache.pinot.query.planner.spi;

import java.util.List;
import org.apache.calcite.plan.Context;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitDef;
import org.apache.calcite.rel.metadata.RelMetadataProvider;


/// Plugin SPI for replacing the multi-stage planner's logical optimizer with a
/// cost-based optimizer (for example, Calcite's `VolcanoPlanner`).
///
/// Implementations are discovered via [java.util.ServiceLoader] the same way as
/// [RuleSetCustomizer]. Declare the provider in
/// `META-INF/services/org.apache.pinot.query.planner.spi.QueryOptimizerProvider`
/// inside the plugin JAR. Implementations must have a public no-arg constructor.
///
/// A provider is selected per query by the `queryOptimizer` query option, whose
/// value is [getName()]. When the option is unset, the built-in HEP planner is
/// used.
///
/// ### Planner contract
///
/// The planner returned by [createPlanner] is driven through the generic
/// [RelOptPlanner] interface: Pinot calls
/// [org.apache.calcite.plan.RelOptPlanner#setRoot] with a tree of Calcite /
/// Pinot logical `RelNode`s rooted in [org.apache.calcite.plan.Convention#NONE],
/// then [org.apache.calcite.plan.RelOptPlanner#findBestExp] and expects back a
/// tree of Pinot logical nodes, again in `Convention.NONE`, ready for Pinot's
/// trait program and plan-node conversion.
///
/// A VolcanoPlanner cannot produce a `Convention.NONE` root directly, so an
/// implementation that searches in its own convention must:
///
/// - register the trait definitions it needs — in particular
///   [org.apache.calcite.plan.ConventionTraitDef#INSTANCE] for its own
///   convention. The `traitDefs` passed to [createPlanner] come from the
///   framework config and are currently empty, so they are informational only;
/// - convert the `Convention.NONE` input into its convention (typically via
///   `ConverterRule`s and `Converter` nodes, or by overriding `setRoot` /
///   `changeTraits`);
/// - convert the result back to `Convention.NONE` before `findBestExp` returns.
///   The result is handed to the Pinot trait program and
///   `RelToPlanNodeConverter`, which only understand Pinot logical nodes.
///
/// The planner is created per query and discarded afterwards.
///
/// ### Cancellation
///
/// The broker cancels planning by interrupting the planning thread (for
/// example, on broker timeout or query termination). Calcite's planners ignore
/// thread interrupts: `RelOptPlanner#checkCancel()` only consults the
/// [org.apache.calcite.util.CancelFlag] of the planner [Context]. An
/// implementation must therefore override `checkCancel()` to throw when
/// `Thread.currentThread().isInterrupted()` (without clearing the interrupt
/// status), then delegate to `super.checkCancel()`. Pinot's built-in
/// `LogicalPlanner` does this by throwing
/// `org.apache.pinot.spi.exception.EarlyTerminationException`; plugins should
/// throw the same exception so the broker reports the query as cancelled.
/// `VolcanoPlanner` calls `checkCancel()` before each rule match, so without
/// this override an interrupted Volcano search runs to completion, which can
/// take minutes for large join graphs.
///
/// The built-in HEP optimizer derives its rule list from `PinotRuleSet`
/// (`RuleSetCustomizer` contributions) and honors the `usePlannerRules` /
/// `skipPlannerRules` query options and the broker's default-disabled rules. A
/// plugin planner replaces that HEP program entirely and is responsible for its
/// own rule set — those rule controls have no effect on it.
public interface QueryOptimizerProvider {

  /// Name used as the value of the `queryOptimizer` query option.
  String getName();

  /// Metadata providers that supply statistics (row counts, cardinalities,
  /// etc.) to the optimizer. The broker installs these on each query's
  /// [org.apache.calcite.plan.RelOptCluster].
  ///
  /// Calcite's [org.apache.calcite.rel.metadata.JaninoRelMetadataProvider]
  /// caches generated handlers keyed by provider identity, so implementations
  /// must return the same provider and handler instances on every call.
  /// Handlers must not bake per-query statistics into themselves; instead they
  /// should look the statistics up at invocation time from the query's
  /// [CostModelProvider], reachable via
  /// `rel.getCluster().getPlanner().getContext().unwrap(CostModelProvider.class)`.
  List<RelMetadataProvider> getMetadataProviders();

  /// Creates a per-query cost model. The broker calls
  /// [CostModelProvider#configure] on it with the query options before passing
  /// it to [createPlanner]. Returning a fresh instance per query keeps
  /// per-query statistics isolated from other concurrent queries.
  CostModelProvider createCostModelProvider();

  /// Builds a new planner for a single query. The returned planner replaces the
  /// built-in HEP planner for the logical optimization phase and is discarded
  /// once the query finishes planning.
  ///
  /// @param costModelProvider the provider returned by [#createCostModelProvider],
  ///                          already configured with the query options
  /// @param context the Calcite planner context for this query. It unwraps to
  ///                [org.apache.pinot.query.context.PlannerContext] and
  ///                [org.apache.pinot.query.QueryEnvironment.Config] as needed.
  /// @param traitDefs the trait definitions from the planner framework config
  ///                   (currently empty; the planner is expected to register its
  ///                   own trait definitions, see [#createPlanner] contract)
  RelOptPlanner createPlanner(CostModelProvider costModelProvider, Context context, List<RelTraitDef> traitDefs);
}
