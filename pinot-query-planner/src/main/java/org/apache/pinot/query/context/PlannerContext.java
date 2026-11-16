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
import com.google.common.base.Preconditions;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.calcite.plan.Context;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitDef;
import org.apache.calcite.plan.hep.HepProgram;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.prepare.PlannerImpl;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.rel.RelDistributionTraitDef;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.SqlExplainFormat;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.pinot.calcite.rex.SearchSealer;
import org.apache.pinot.calcite.sql.fun.PinotOperatorTable;
import org.apache.pinot.common.utils.config.QueryOptionsUtils;
import org.apache.pinot.query.QueryEnvironment;
import org.apache.pinot.query.planner.logical.LogicalPlanner;
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

  private SqlValidator _validator;
  // Kept to create a new validator in #enableMvAggregateFallback()
  @Nullable
  private final Prepare.CatalogReader _catalogReader;
  @Nullable
  private final RelDataTypeFactory _typeFactory;

  // Kept to create new planners in #enableMvAggregateFallback()
  private final HepProgram _optProgram;
  private final HepProgram _traitProgram;
  @Nullable
  private final List<RelTraitDef> _optTraitDefs;
  private RelOptPlanner _relOptPlanner;
  private LogicalPlanner _relTraitPlanner;
  // Set when the query is optimized again with the MV aggregate, see #enableMvAggregateFallback()
  private boolean _mvAggregateFallback;

  private final Map<String, String> _options;
  private final QueryEnvironment.Config _envConfig;
  private final Map<String, String> _plannerOutput;
  private final SqlExplainFormat _sqlExplainFormat;
  @Nullable
  private final PhysicalPlannerContext _physicalPlannerContext;
  private final SearchSealer _searchSealer;

  public PlannerContext(FrameworkConfig config, Prepare.CatalogReader catalogReader, RelDataTypeFactory typeFactory,
      HepProgram optProgram, HepProgram traitProgram, Map<String, String> options, QueryEnvironment.Config envConfig,
      SqlExplainFormat sqlExplainFormat, @Nullable PhysicalPlannerContext physicalPlannerContext) {
    _planner = new PlannerImpl(config);
    _validator = new Validator(config.getOperatorTable(), catalogReader, typeFactory);
    _catalogReader = catalogReader;
    _typeFactory = typeFactory;
    _options = options;
    _envConfig = envConfig;
    _optProgram = optProgram;
    _traitProgram = traitProgram;
    _optTraitDefs = config.getTraitDefs();
    _relOptPlanner = new LogicalPlanner(optProgram, this, _optTraitDefs);
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
    _planner = null;
    _validator = null;
    _catalogReader = null;
    _typeFactory = null;
    _options = options;
    _envConfig = envConfig;
    HepProgram emptyProgram = new HepProgramBuilder().build();
    _optProgram = emptyProgram;
    _traitProgram = emptyProgram;
    _optTraitDefs = null;
    _relOptPlanner = new LogicalPlanner(emptyProgram, this);
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

  public SqlValidator getValidator() {
    return _validator;
  }

  public RelOptPlanner getRelOptPlanner() {
    return _relOptPlanner;
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
  /// [Context], and delegates to [#_envConfig] when asked for
  /// [QueryEnvironment.Config] so that existing rules remain compatible.
  @Override
  @Nullable
  public <C> C unwrap(Class<C> clazz) {
    if (clazz.isInstance(this)) {
      return clazz.cast(this);
    }
    if (clazz.isInstance(_envConfig)) {
      return clazz.cast(_envConfig);
    }
    return null;
  }

  @Override
  public void close() {
    if (_planner != null) {
      _planner.close();
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

  /// Whether the planner plans `GROUP BY ARRAY_TO_MV(expr)` as an MV aggregate in this planner run, that is after
  /// [#enableMvAggregateFallback()].
  public boolean isUseMvAggregate() {
    return _mvAggregateFallback;
  }

  /// Prepares this context to plan the query again with the MV aggregate, from its SQL. The planner calls it when the
  /// plan without the MV aggregate fails the `ARRAY_TO_MV` validation.
  ///
  /// - Turns the MV aggregate on.
  /// - Replaces the validator with one where `ARRAY_TO_MV` has a nullable type, see
  ///   [PinotOperatorTable#NULLABLE_ARRAY_TO_MV]. A validator keeps the types of the query that it validated, so
  ///   the query must be validated again with the new one.
  /// - Replaces the planners with new ones. A new planner starts without the vertices of the plan that the old one
  ///   optimized.
  public void enableMvAggregateFallback() {
    Preconditions.checkState(_catalogReader != null && _typeFactory != null,
        "The MV aggregate fallback needs a context with a validator");
    _mvAggregateFallback = true;
    _validator = new Validator(PinotOperatorTable.withNullableArrayToMv(_envConfig.isNullHandlingEnabled()),
        _catalogReader, _typeFactory);
    _relOptPlanner = _optTraitDefs != null ? new LogicalPlanner(_optProgram, this, _optTraitDefs)
        : new LogicalPlanner(_optProgram, this);
    _relTraitPlanner = new LogicalPlanner(_traitProgram, this, List.of(RelDistributionTraitDef.INSTANCE));
  }
}
