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
package org.apache.pinot.query;

import java.util.Collection;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.plan.Contexts;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.prepare.PlannerImpl;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.sql2rel.StandardConvertletTable;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.pinot.query.context.PlannerContext;
import org.apache.pinot.query.parser.CalciteSqlParser;
import org.apache.pinot.query.planner.LogicalPlanner;
import org.apache.pinot.query.planner.QueryPlan;
import org.apache.pinot.query.planner.StagePlanner;
import org.apache.pinot.query.routing.WorkerManager;
import org.apache.pinot.query.rules.PinotQueryRuleSets;
import org.apache.pinot.query.type.TypeFactory;
import org.apache.pinot.query.validate.Validator;


/**
 * The {@code QueryEnvironment} contains the main entrypoint for query planning.
 *
 * <p>It provide the higher level entry interface to convert a SQL string into a {@link QueryPlan}.
 */
public class QueryEnvironment {
  // Calcite configurations
  private final FrameworkConfig _config;

  // Calcite extension/plugins
  private final CalciteSchema _rootSchema;
  private final PlannerImpl _planner;
  private final Prepare.CatalogReader _catalogReader;
  private final RelDataTypeFactory _typeFactory;
  private final RelOptPlanner _relOptPlanner;
  private final SqlValidator _validator;

  // Pinot extensions
  private final Collection<RelOptRule> _logicalRuleSet;
  private final WorkerManager _workerManager;

  public QueryEnvironment(TypeFactory typeFactory, CalciteSchema rootSchema, WorkerManager workerManager) {
    _typeFactory = typeFactory;
    _rootSchema = rootSchema;
    _workerManager = workerManager;
    _config = Frameworks.newConfigBuilder().traitDefs().build();

    // Planner is not thread-safe. must be reset() after each use.
    _planner = new PlannerImpl(_config);

    // catalog
    _catalogReader = new CalciteCatalogReader(_rootSchema, _rootSchema.path(null), _typeFactory, null);
    _validator = new Validator(SqlStdOperatorTable.instance(), _catalogReader, _typeFactory);

    // optimizer rules
    _logicalRuleSet = PinotQueryRuleSets.LOGICAL_OPT_RULES;

    // optimizer
    HepProgramBuilder hepProgramBuilder = new HepProgramBuilder();
    for (RelOptRule relOptRule : _logicalRuleSet) {
      hepProgramBuilder.addRuleInstance(relOptRule);
    }
    _relOptPlanner = new LogicalPlanner(hepProgramBuilder.build(), Contexts.EMPTY_CONTEXT);
  }

  /**
   * Plan a SQL query.
   *
   * @param sqlQuery SQL query string.
   * @return a dispatchable query plan
   */
  public QueryPlan planQuery(String sqlQuery) {
    PlannerContext plannerContext = new PlannerContext();
    try {
      SqlNode parsed = parse(sqlQuery, plannerContext);
      SqlNode validated = validate(parsed);
      RelRoot relation = toRelation(validated, plannerContext);
      RelNode optimized = optimize(relation, plannerContext);
      return toDispatchablePlan(optimized, plannerContext);
    } catch (Exception e) {
      throw new RuntimeException("Error composing query plan for: " + sqlQuery, e);
    } finally {
      // TODO: No 2 query should be planned at the same time, because PlannerImpl is stateful.
      // make calls to planQuery sequentially
      _planner.close();
      _planner.reset();
    }
  }

  // --------------------------------------------------------------------------
  // steps
  // --------------------------------------------------------------------------

  protected SqlNode parse(String query, PlannerContext plannerContext)
      throws Exception {
    // 1. invoke CalciteSqlParser to parse out SqlNode;
    return CalciteSqlParser.compile(query, plannerContext);
  }

  protected SqlNode validate(SqlNode parsed)
      throws Exception {
    // 2. validator to validate.
    SqlNode validated = _validator.validate(parsed);
    if (null == validated || !validated.getKind().belongsTo(SqlKind.QUERY)) {
      throw new IllegalArgumentException(
          String.format("unsupported SQL query, cannot validate out a valid sql from:\n%s", parsed));
    }
    return validated;
  }

  protected RelRoot toRelation(SqlNode parsed, PlannerContext plannerContext) {
    // 3. convert sqlNode to relNode.
    RexBuilder rexBuilder = new RexBuilder(_typeFactory);
    RelOptCluster cluster = RelOptCluster.create(_relOptPlanner, rexBuilder);
    SqlToRelConverter sqlToRelConverter =
        new SqlToRelConverter(_planner, _validator, _catalogReader, cluster, StandardConvertletTable.INSTANCE,
            SqlToRelConverter.config());
    return sqlToRelConverter.convertQuery(parsed, false, true);
  }

  protected RelNode optimize(RelRoot relRoot, PlannerContext plannerContext) {
    // 4. optimize relNode
    // TODO: add support for traits, cost factory.
    try {
      _relOptPlanner.setRoot(relRoot.rel);
      return _relOptPlanner.findBestExp();
    } catch (Exception e) {
      throw new UnsupportedOperationException(
          "Cannot generate a valid execution plan for the given query: " + RelOptUtil.toString(relRoot.rel), e);
    }
  }

  protected QueryPlan toDispatchablePlan(RelNode relRoot, PlannerContext plannerContext) {
    // 5. construct a dispatchable query plan.
    StagePlanner queryStagePlanner = new StagePlanner(plannerContext, _workerManager);
    return queryStagePlanner.makePlan(relRoot);
  }
}
