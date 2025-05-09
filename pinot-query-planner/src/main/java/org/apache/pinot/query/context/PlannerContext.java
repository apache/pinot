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

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.calcite.plan.Contexts;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.hep.HepProgram;
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
 * PlannerContext is an object that holds all contextual information during planning phase.
 *
 * TODO: currently we don't support option or query rewrite.
 * It is used to hold per query context for query planning, which cannot be shared across queries.
 */
public class PlannerContext implements AutoCloseable {
  private final PlannerImpl _planner;

  private final SqlValidator _validator;

  private final RelOptPlanner _relOptPlanner;
  private final LogicalPlanner _relTraitPlanner;

  private final Map<String, String> _options;
  private final Map<String, String> _plannerOutput;
  private final SqlExplainFormat _sqlExplainFormat;
  @Nullable
  private final PhysicalPlannerContext _physicalPlannerContext;

  public PlannerContext(FrameworkConfig config, Prepare.CatalogReader catalogReader, RelDataTypeFactory typeFactory,
      HepProgram optProgram, HepProgram traitProgram, Map<String, String> options, QueryEnvironment.Config envConfig,
      SqlExplainFormat sqlExplainFormat, @Nullable PhysicalPlannerContext physicalPlannerContext) {
    _planner = new PlannerImpl(config);
    _validator = new Validator(config.getOperatorTable(), catalogReader, typeFactory);
    _relOptPlanner = new LogicalPlanner(optProgram, Contexts.EMPTY_CONTEXT, config.getTraitDefs());
    _relTraitPlanner = new LogicalPlanner(traitProgram, Contexts.of(envConfig),
        Collections.singletonList(RelDistributionTraitDef.INSTANCE));
    _options = options;
    _plannerOutput = new HashMap<>();
    _sqlExplainFormat = sqlExplainFormat;
    _physicalPlannerContext = physicalPlannerContext;
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
