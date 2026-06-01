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
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.calcite.plan.Context;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.hep.HepProgram;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.prepare.PlannerImpl;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.rel.RelDistributionTraitDef;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.SqlExplainFormat;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.pinot.query.QueryEnvironment;
import org.apache.pinot.query.planner.logical.LogicalPlanner;
import org.apache.pinot.query.validate.Validator;


/**
 * Holds all per-query contextual information used during the planning phase.
 *
 * <p>This class implements {@link Context} so that Calcite rules can retrieve it directly from the
 * planner: {@code call.getPlanner().getContext().unwrap(PlannerContext.class)}. Both the opt planner
 * and the trait planner expose this instance as their context.
 *
 * <p>Callers may also unwrap {@link QueryEnvironment.Config} to access broker-wide defaults:
 * {@code call.getPlanner().getContext().unwrap(QueryEnvironment.Config.class)}.
 */
public class PlannerContext implements AutoCloseable, Context {
  private final PlannerImpl _planner;

  private final SqlValidator _validator;

  private final RelOptPlanner _relOptPlanner;
  private final LogicalPlanner _relTraitPlanner;

  private final Map<String, String> _options;
  private final QueryEnvironment.Config _envConfig;
  private final Map<String, String> _plannerOutput;
  private final SqlExplainFormat _sqlExplainFormat;
  @Nullable
  private final PhysicalPlannerContext _physicalPlannerContext;

  public PlannerContext(FrameworkConfig config, Prepare.CatalogReader catalogReader, RelDataTypeFactory typeFactory,
      HepProgram optProgram, HepProgram traitProgram, Map<String, String> options, QueryEnvironment.Config envConfig,
      SqlExplainFormat sqlExplainFormat, @Nullable PhysicalPlannerContext physicalPlannerContext) {
    _planner = new PlannerImpl(config);
    _validator = new Validator(config.getOperatorTable(), catalogReader, typeFactory);
    _options = options;
    _envConfig = envConfig;
    _relOptPlanner = new LogicalPlanner(optProgram, this, config.getTraitDefs());
    _relTraitPlanner = new LogicalPlanner(traitProgram, this,
        Collections.singletonList(RelDistributionTraitDef.INSTANCE));
    _plannerOutput = new HashMap<>();
    _sqlExplainFormat = sqlExplainFormat;
    _physicalPlannerContext = physicalPlannerContext;
  }

  /**
   * Test factory: creates a minimal {@link PlannerContext} without going through
   * {@link org.apache.pinot.query.QueryEnvironment}, suitable for unit tests.
   */
  @VisibleForTesting
  public static PlannerContext forTesting(Map<String, String> options, QueryEnvironment.Config envConfig) {
    return new PlannerContext(options, envConfig);
  }

  /**
   * Minimal constructor for use in unit tests. Creates no-op planners backed by an empty HEP program.
   */
  @VisibleForTesting
  PlannerContext(Map<String, String> options, QueryEnvironment.Config envConfig) {
    _planner = null;
    _validator = null;
    _options = options;
    _envConfig = envConfig;
    HepProgram emptyProgram = new HepProgramBuilder().build();
    _relOptPlanner = new LogicalPlanner(emptyProgram, this);
    _relTraitPlanner = new LogicalPlanner(emptyProgram, this);
    _plannerOutput = new HashMap<>();
    _sqlExplainFormat = null;
    _physicalPlannerContext = null;
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

  /**
   * Unwraps this context. Returns {@code this} when asked for {@link PlannerContext} or
   * {@link Context}, and delegates to {@link #_envConfig} when asked for
   * {@link QueryEnvironment.Config} so that existing rules remain compatible.
   */
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
    _planner.close();
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

  public boolean isUsePhysicalOptimizer() {
    return _physicalPlannerContext != null;
  }
}
