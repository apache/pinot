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

import com.google.common.annotations.VisibleForTesting;
import java.util.Arrays;
import java.util.Collection;
import java.util.Properties;
import org.apache.calcite.config.CalciteConnectionConfigImpl;
import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.hep.HepProgram;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.hint.HintStrategyTable;
import org.apache.calcite.rel.rules.PinotQueryRuleSets;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.sql.SqlExplain;
import org.apache.calcite.sql.SqlExplainFormat;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.util.ChainedSqlOperatorTable;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.sql2rel.StandardConvertletTable;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.pinot.query.context.PlannerContext;
import org.apache.pinot.query.planner.PlannerUtils;
import org.apache.pinot.query.planner.QueryPlan;
import org.apache.pinot.query.planner.logical.StagePlanner;
import org.apache.pinot.query.routing.WorkerManager;
import org.apache.pinot.query.type.TypeFactory;
import org.apache.pinot.sql.parsers.CalciteSqlParser;
import org.apache.pinot.sql.parsers.SqlNodeAndOptions;


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
  private final Prepare.CatalogReader _catalogReader;
  private final RelDataTypeFactory _typeFactory;

  private final HepProgram _hepProgram;

  // Pinot extensions
  private final Collection<RelOptRule> _logicalRuleSet;
  private final WorkerManager _workerManager;

  public QueryEnvironment(TypeFactory typeFactory, CalciteSchema rootSchema, WorkerManager workerManager) {
    _typeFactory = typeFactory;
    _rootSchema = rootSchema;
    _workerManager = workerManager;

    // catalog
    Properties catalogReaderConfigProperties = new Properties();
    catalogReaderConfigProperties.setProperty(CalciteConnectionProperty.CASE_SENSITIVE.camelName(), "true");
    _catalogReader = new CalciteCatalogReader(_rootSchema, _rootSchema.path(null), _typeFactory,
        new CalciteConnectionConfigImpl(catalogReaderConfigProperties));

    _config = Frameworks.newConfigBuilder().traitDefs()
        .operatorTable(new ChainedSqlOperatorTable(Arrays.asList(SqlStdOperatorTable.instance(), _catalogReader)))
        .defaultSchema(_rootSchema.plus())
        .sqlToRelConverterConfig(SqlToRelConverter.config()
            .withHintStrategyTable(getHintStrategyTable())
            .addRelBuilderConfigTransform(c -> c.withPushJoinCondition(true))
            .addRelBuilderConfigTransform(c -> c.withAggregateUnique(true)))
        .build();

    // optimizer rules
    _logicalRuleSet = PinotQueryRuleSets.LOGICAL_OPT_RULES;

    // optimizer
    HepProgramBuilder hepProgramBuilder = new HepProgramBuilder();
    for (RelOptRule relOptRule : _logicalRuleSet) {
      hepProgramBuilder.addRuleInstance(relOptRule);
    }
    _hepProgram = hepProgramBuilder.build();
  }

  /**
   * Plan a SQL query.
   *
   * This function is thread safe since we construct a new PlannerContext every time.
   *
   * TODO: follow benchmark and profile to measure whether it make sense for the latency-concurrency trade-off
   * between reusing plannerImpl vs. create a new planner for each query.
   *
   * @param sqlQuery SQL query string.
   * @param sqlNodeAndOptions parsed SQL query.
   * @return a dispatchable query plan
   */
  public QueryPlan planQuery(String sqlQuery, SqlNodeAndOptions sqlNodeAndOptions) {
    try (PlannerContext plannerContext = new PlannerContext(_config, _catalogReader, _typeFactory, _hepProgram)) {
      plannerContext.setOptions(sqlNodeAndOptions.getOptions());
      RelNode relRoot = compileQuery(sqlNodeAndOptions.getSqlNode(), plannerContext);
      return toDispatchablePlan(relRoot, plannerContext);
    } catch (Exception e) {
      throw new RuntimeException("Error composing query plan for: " + sqlQuery, e);
    }
  }

  /**
   * Explain a SQL query.
   *
   * Similar to {@link QueryEnvironment#planQuery(String, SqlNodeAndOptions)}, this API runs the query compilation.
   * But it doesn't run the distributed {@link QueryPlan} generation, instead it only returns the explained logical
   * plan.
   *
   * @param sqlQuery SQL query string.
   * @param sqlNodeAndOptions parsed SQL query.
   * @return the explained query plan.
   */
  public String explainQuery(String sqlQuery, SqlNodeAndOptions sqlNodeAndOptions) {
    try (PlannerContext plannerContext = new PlannerContext(_config, _catalogReader, _typeFactory, _hepProgram)) {
      SqlExplain explain = (SqlExplain) sqlNodeAndOptions.getSqlNode();
      plannerContext.setOptions(sqlNodeAndOptions.getOptions());
      RelNode relRoot = compileQuery(explain.getExplicandum(), plannerContext);
      SqlExplainFormat format = explain.getFormat() == null ? SqlExplainFormat.DOT : explain.getFormat();
      SqlExplainLevel level =
          explain.getDetailLevel() == null ? SqlExplainLevel.DIGEST_ATTRIBUTES : explain.getDetailLevel();
      return PlannerUtils.explainPlan(relRoot, format, level);
    } catch (Exception e) {
      throw new RuntimeException("Error explain query plan for: " + sqlQuery, e);
    }
  }

  @VisibleForTesting
  public QueryPlan planQuery(String sqlQuery) {
    return planQuery(sqlQuery, CalciteSqlParser.compileToSqlNodeAndOptions(sqlQuery));
  }

  @VisibleForTesting
  public String explainQuery(String sqlQuery) {
    return explainQuery(sqlQuery, CalciteSqlParser.compileToSqlNodeAndOptions(sqlQuery));
  }

  // --------------------------------------------------------------------------
  // steps
  // --------------------------------------------------------------------------

  @VisibleForTesting
  protected RelNode compileQuery(SqlNode sqlNode, PlannerContext plannerContext)
      throws Exception {
    SqlNode validated = validate(sqlNode, plannerContext);
    RelRoot relation = toRelation(validated, plannerContext);
    return optimize(relation, plannerContext);
  }

  private SqlNode validate(SqlNode parsed, PlannerContext plannerContext)
      throws Exception {
    // 2. validator to validate.
    SqlNode validated = plannerContext.getValidator().validate(parsed);
    if (null == validated || !validated.getKind().belongsTo(SqlKind.QUERY)) {
      throw new IllegalArgumentException(
          String.format("unsupported SQL query, cannot validate out a valid sql from:\n%s", parsed));
    }
    return validated;
  }

  private RelRoot toRelation(SqlNode parsed, PlannerContext plannerContext) {
    // 3. convert sqlNode to relNode.
    RexBuilder rexBuilder = new RexBuilder(_typeFactory);
    RelOptCluster cluster = RelOptCluster.create(plannerContext.getRelOptPlanner(), rexBuilder);
    SqlToRelConverter sqlToRelConverter =
        new SqlToRelConverter(plannerContext.getPlanner(), plannerContext.getValidator(), _catalogReader, cluster,
            StandardConvertletTable.INSTANCE, _config.getSqlToRelConverterConfig());
    return sqlToRelConverter.convertQuery(parsed, false, true);
  }

  private RelNode optimize(RelRoot relRoot, PlannerContext plannerContext) {
    // 4. optimize relNode
    // TODO: add support for traits, cost factory.
    try {
      plannerContext.getRelOptPlanner().setRoot(relRoot.rel);
      return plannerContext.getRelOptPlanner().findBestExp();
    } catch (Exception e) {
      throw new UnsupportedOperationException(
          "Cannot generate a valid execution plan for the given query: " + RelOptUtil.toString(relRoot.rel), e);
    }
  }

  private QueryPlan toDispatchablePlan(RelNode relRoot, PlannerContext plannerContext) {
    // 5. construct a dispatchable query plan.
    StagePlanner queryStagePlanner = new StagePlanner(plannerContext, _workerManager);
    return queryStagePlanner.makePlan(relRoot);
  }

  // --------------------------------------------------------------------------
  // utils
  // --------------------------------------------------------------------------

  private HintStrategyTable getHintStrategyTable() {
    return HintStrategyTable.builder().build();
  }
}
