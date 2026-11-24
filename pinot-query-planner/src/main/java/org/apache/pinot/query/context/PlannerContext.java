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
package org.apache.pinot.query.context;

import com.google.common.annotations.VisibleForTesting;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.calcite.plan.Context;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.hep.HepProgram;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.prepare.PlannerImpl;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.rel.RelDistributionTraitDef;
import org.apache.calcite.rel.metadata.JaninoRelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataQueryBase;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.SqlExplainFormat;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.pinot.calcite.rex.SearchSealer;
import org.apache.pinot.common.utils.config.QueryOptionsUtils;
import org.apache.pinot.query.QueryEnvironment;
import org.apache.pinot.query.planner.logical.LogicalPlanner;
import org.apache.pinot.query.planner.spi.CostModelProvider;
import org.apache.pinot.query.planner.spi.QueryOptimizerProvider;
import org.apache.pinot.query.validate.Validator;


/// Holds all per-query contextual information used during the planning phase.
///
/// This class implements [Context] so that Calcite rules can retrieve it directly from the
/// planner: `call.getPlanner().getContext().unwrap(PlannerContext.class)`. Both the opt planner
/// and the trait planner expose this instance as their context.
///
/// Callers may also unwrap [QueryEnvironment.Config] to access broker-wide defaults:
/// `call.getPlanner().getContext().unwrap(QueryEnvironment.Config.class)`.
public class PlannerContext implements AutoCloseable, Context {
  private final PlannerImpl _planner;

  private final Validator _validator;

  private final RelOptPlanner _relOptPlanner;
  private final LogicalPlanner _relTraitPlanner;

  private final Map<String, String> _options;
  private final QueryEnvironment.Config _envConfig;
  private final Map<String, String> _plannerOutput;
  private final SqlExplainFormat _sqlExplainFormat;
  @Nullable
  private final PhysicalPlannerContext _physicalPlannerContext;
  @Nullable
  private final CostModelProvider _costModelProvider;
  private final List<RelMetadataProvider> _metadataProviders;
  @Nullable
  private JaninoRelMetadataProvider _previousThreadMetadataProvider;
  private boolean _threadMetadataProviderSaved;
  private final SearchSealer _searchSealer;

  public PlannerContext(FrameworkConfig config, Prepare.CatalogReader catalogReader, RelDataTypeFactory typeFactory,
      HepProgram optProgram, HepProgram traitProgram, Map<String, String> options, QueryEnvironment.Config envConfig,
      SqlExplainFormat sqlExplainFormat, @Nullable PhysicalPlannerContext physicalPlannerContext,
      @Nullable QueryOptimizerProvider optimizerProvider) {
    _planner = new PlannerImpl(config);
    _validator = new Validator(config.getOperatorTable(), catalogReader, typeFactory,
        QueryOptionsUtils.isPreferSelectAliasInGroupBy(options, envConfig.defaultPreferSelectAliasInGroupBy()),
        envConfig.meterGroupByAliasShadowedByColumn());
    _options = options;
    _envConfig = envConfig;
    _costModelProvider = optimizerProvider == null ? null : optimizerProvider.createCostModelProvider();
    if (_costModelProvider != null) {
      _costModelProvider.configure(options);
    }
    _metadataProviders = optimizerProvider == null
        ? List.of()
        : List.copyOf(optimizerProvider.getMetadataProviders());
    _relOptPlanner = optimizerProvider == null
        ? new LogicalPlanner(optProgram, this, config.getTraitDefs())
        : optimizerProvider.createPlanner(_costModelProvider, this, config.getTraitDefs());
    _relTraitPlanner = new LogicalPlanner(traitProgram, this,
        List.of(RelDistributionTraitDef.INSTANCE));
    _plannerOutput = new HashMap<>();
    _sqlExplainFormat = sqlExplainFormat;
    _physicalPlannerContext = physicalPlannerContext;
    _searchSealer = createSearchSealer(options, envConfig);
  }

  /// Test factory: creates a minimal [PlannerContext] without going through
  /// [org.apache.pinot.query.QueryEnvironment], suitable for unit tests.
  @VisibleForTesting
  public static PlannerContext forTesting(Map<String, String> options, QueryEnvironment.Config envConfig) {
    return new PlannerContext(options, envConfig);
  }

  /// Minimal constructor for use in unit tests. Creates no-op planners backed by an empty HEP program.
  @VisibleForTesting
  PlannerContext(Map<String, String> options, QueryEnvironment.Config envConfig) {
    this(options, envConfig, null);
  }

  /// Minimal constructor for use in unit tests, optionally using a cost-based optimizer provider.
  @VisibleForTesting
  PlannerContext(Map<String, String> options, QueryEnvironment.Config envConfig,
      @Nullable QueryOptimizerProvider optimizerProvider) {
    _planner = null;
    _validator = null;
    _options = options;
    _envConfig = envConfig;
    HepProgram emptyProgram = new HepProgramBuilder().build();
    _costModelProvider = optimizerProvider == null ? null : optimizerProvider.createCostModelProvider();
    if (_costModelProvider != null) {
      _costModelProvider.configure(options);
    }
    _metadataProviders = optimizerProvider == null
        ? List.of()
        : List.copyOf(optimizerProvider.getMetadataProviders());
    _relOptPlanner = optimizerProvider == null
        ? new LogicalPlanner(emptyProgram, this)
        : optimizerProvider.createPlanner(_costModelProvider, this, List.of());
    _relTraitPlanner = new LogicalPlanner(emptyProgram, this);
    _plannerOutput = new HashMap<>();
    _sqlExplainFormat = null;
    _physicalPlannerContext = null;
    _searchSealer = createSearchSealer(options, envConfig);
  }

  private static SearchSealer createSearchSealer(Map<String, String> options, QueryEnvironment.Config envConfig) {
    return new SearchSealer(
        QueryOptionsUtils.getSealedInListThreshold(options, envConfig.defaultSealedInListThreshold()));
  }

  public PlannerImpl getPlanner() {
    return _planner;
  }

  public Validator getValidator() {
    return _validator;
  }

  public RelOptPlanner getRelOptPlanner() {
    return _relOptPlanner;
  }

  /// Returns the metadata providers supplied by the active cost-based optimizer,
  /// or an empty list when the built-in HEP planner is in use. Used by
  /// [QueryEnvironment] to install statistics providers on the query's [RelOptCluster].
  public List<RelMetadataProvider> getMetadataProviders() {
    return _metadataProviders;
  }

  /// Remembers the value of Calcite's thread-local metadata provider before the query's custom
  /// metadata providers are installed, so [close] can restore it. Must be called before
  /// [org.apache.calcite.plan.RelOptCluster#setMetadataProvider] is invoked with the query's providers.
  public void rememberPreviousThreadMetadataProvider(@Nullable JaninoRelMetadataProvider previous) {
    _previousThreadMetadataProvider = previous;
    _threadMetadataProviderSaved = true;
  }

  public LogicalPlanner getRelTraitPlanner() {
    return _relTraitPlanner;
  }

  public Map<String, String> getOptions() {
    return _options;
  }

  public QueryEnvironment.Config getEnvConfig() {
    return _envConfig;
  }

  /// Unwraps this context. Returns `this` when asked for [PlannerContext] or
  /// [Context], [#_envConfig] when asked for [QueryEnvironment.Config], and
  /// [#_costModelProvider] when asked for [CostModelProvider] so optimizer
  /// metadata handlers can read per-query statistics.
  @Override
  @Nullable
  public <C> C unwrap(Class<C> clazz) {
    if (clazz.isInstance(this)) {
      return clazz.cast(this);
    }
    if (clazz.isInstance(_envConfig)) {
      return clazz.cast(_envConfig);
    }
    if (_costModelProvider != null && clazz.isInstance(_costModelProvider)) {
      return clazz.cast(_costModelProvider);
    }
    return null;
  }

  @Override
  public void close() {
    if (_planner != null) {
      _planner.close();
    }
    if (_threadMetadataProviderSaved) {
      if (_previousThreadMetadataProvider == null) {
        RelMetadataQueryBase.THREAD_PROVIDERS.remove();
      } else {
        RelMetadataQueryBase.THREAD_PROVIDERS.set(_previousThreadMetadataProvider);
      }
      _threadMetadataProviderSaved = false;
      _previousThreadMetadataProvider = null;
    }
  }

  public Map<String, String> getPlannerOutput() {
    return _plannerOutput;
  }

  public SqlExplainFormat getSqlExplainFormat() {
    return _sqlExplainFormat;
  }

  @Nullable
  public PhysicalPlannerContext getPhysicalPlannerContext() {
    return _physicalPlannerContext;
  }

  /// Returns the query's [SearchSealer], which hides large IN lists from Calcite during optimization.
  public SearchSealer getSearchSealer() {
    return _searchSealer;
  }

  public boolean isUsePhysicalOptimizer() {
    return _physicalPlannerContext != null;
  }
}
