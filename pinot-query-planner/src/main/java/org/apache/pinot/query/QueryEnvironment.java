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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.calcite.config.CalciteConnectionConfigImpl;
import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.hep.HepMatchOrder;
import org.apache.calcite.plan.hep.HepProgram;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.prepare.PinotCalciteCatalogReader;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.hint.HintStrategyTable;
import org.apache.calcite.rel.hint.PinotHintStrategyTable;
import org.apache.calcite.rel.logical.LogicalCorrelate;
import org.apache.calcite.rel.rules.PinotQueryRuleSets;
import org.apache.calcite.rel.rules.PinotRelDistributionTraitRule;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.runtime.CalciteContextException;
import org.apache.calcite.sql.SqlExplain;
import org.apache.calcite.sql.SqlExplainFormat;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.fun.PinotOperatorTable;
import org.apache.calcite.sql.util.PinotChainedSqlOperatorTable;
import org.apache.calcite.sql2rel.PinotConvertletTable;
import org.apache.calcite.sql2rel.RelDecorrelator;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.RelBuilder;
import org.apache.pinot.common.config.provider.TableCache;
import org.apache.pinot.query.context.PlannerContext;
import org.apache.pinot.query.planner.PlannerUtils;
import org.apache.pinot.query.planner.QueryPlan;
import org.apache.pinot.query.planner.SubPlan;
import org.apache.pinot.query.planner.explain.PhysicalExplainPlanVisitor;
import org.apache.pinot.query.planner.logical.PinotLogicalQueryPlanner;
import org.apache.pinot.query.planner.logical.RelToPlanNodeConverter;
import org.apache.pinot.query.planner.physical.DispatchableSubPlan;
import org.apache.pinot.query.planner.physical.PinotDispatchPlanner;
import org.apache.pinot.query.routing.WorkerManager;
import org.apache.pinot.query.type.TypeFactory;
import org.apache.pinot.sql.parsers.CalciteSqlParser;
import org.apache.pinot.sql.parsers.SqlNodeAndOptions;
import org.apache.pinot.sql.parsers.parser.SqlPhysicalExplain;


/**
 * The {@code QueryEnvironment} contains the main entrypoint for query planning.
 *
 * <p>It provide the higher level entry interface to convert a SQL string into a {@link DispatchableSubPlan}.
 */
public class QueryEnvironment {
  private static final String DATABASE_KEY = "database";
  // Calcite configurations
  private final FrameworkConfig _config;

  // Calcite extension/plugins
  private final CalciteSchema _rootSchema;
  private final Prepare.CatalogReader _catalogReader;
  private final RelDataTypeFactory _typeFactory;

  private final HepProgram _optProgram;
  private final HepProgram _traitProgram;

  // Pinot extensions
  private final WorkerManager _workerManager;
  private final TableCache _tableCache;

  public QueryEnvironment(TypeFactory typeFactory, CalciteSchema rootSchema, WorkerManager workerManager,
      TableCache tableCache) {
    _typeFactory = typeFactory;
    _rootSchema = rootSchema;
    _workerManager = workerManager;
    _tableCache = tableCache;

    // catalog & config
    _catalogReader = getCatalog(null);
    _config = getConfig(_catalogReader);
    // opt programs
    _optProgram = getOptProgram();
    _traitProgram = getTraitProgram();
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
   * @return QueryPlannerResult containing the dispatchable query plan and the relRoot.
   */
  public QueryPlannerResult planQuery(String sqlQuery, SqlNodeAndOptions sqlNodeAndOptions, long requestId) {
    try (PlannerContext plannerContext = getPlannerContext(sqlNodeAndOptions.getOptions())) {
      plannerContext.setOptions(sqlNodeAndOptions.getOptions());
      RelRoot relRoot = compileQuery(sqlNodeAndOptions.getSqlNode(), plannerContext);
      // TODO: current code only assume one SubPlan per query, but we should support multiple SubPlans per query.
      // Each SubPlan should be able to run independently from Broker then set the results into the dependent
      // SubPlan for further processing.
      DispatchableSubPlan dispatchableSubPlan = toDispatchableSubPlan(relRoot, plannerContext, requestId);
      return new QueryPlannerResult(dispatchableSubPlan, null, dispatchableSubPlan.getTableNames());
    } catch (CalciteContextException e) {
      throw new RuntimeException("Error composing query plan for '" + sqlQuery + "': " + e.getMessage() + "'", e);
    } catch (Throwable t) {
      throw new RuntimeException("Error composing query plan for: " + sqlQuery, t);
    }
  }

  /**
   * Explain a SQL query.
   *
   * Similar to {@link QueryEnvironment#planQuery(String, SqlNodeAndOptions, long)}, this API runs the query
   * compilation. But it doesn't run the distributed {@link DispatchableSubPlan} generation, instead it only
   * returns the
   * explained logical plan.
   *
   * @param sqlQuery SQL query string.
   * @param sqlNodeAndOptions parsed SQL query.
   * @return QueryPlannerResult containing the explained query plan and the relRoot.
   */
  public QueryPlannerResult explainQuery(String sqlQuery, SqlNodeAndOptions sqlNodeAndOptions, long requestId) {
    try (PlannerContext plannerContext = getPlannerContext(sqlNodeAndOptions.getOptions())) {
      SqlExplain explain = (SqlExplain) sqlNodeAndOptions.getSqlNode();
      plannerContext.setOptions(sqlNodeAndOptions.getOptions());
      RelRoot relRoot = compileQuery(explain.getExplicandum(), plannerContext);
      if (explain instanceof SqlPhysicalExplain) {
        // get the physical plan for query.
        DispatchableSubPlan dispatchableSubPlan = toDispatchableSubPlan(relRoot, plannerContext, requestId);
        return new QueryPlannerResult(null, PhysicalExplainPlanVisitor.explain(dispatchableSubPlan),
            dispatchableSubPlan.getTableNames());
      } else {
        // get the logical plan for query.
        SqlExplainFormat format = explain.getFormat() == null ? SqlExplainFormat.DOT : explain.getFormat();
        SqlExplainLevel level =
            explain.getDetailLevel() == null ? SqlExplainLevel.DIGEST_ATTRIBUTES : explain.getDetailLevel();
        Set<String> tableNames = RelToPlanNodeConverter.getTableNamesFromRelRoot(relRoot.rel);
        return new QueryPlannerResult(null, PlannerUtils.explainPlan(relRoot.rel, format, level), tableNames);
      }
    } catch (Exception e) {
      throw new RuntimeException("Error explain query plan for: " + sqlQuery, e);
    }
  }

  @VisibleForTesting
  public DispatchableSubPlan planQuery(String sqlQuery) {
    return planQuery(sqlQuery, CalciteSqlParser.compileToSqlNodeAndOptions(sqlQuery), 0).getQueryPlan();
  }

  @VisibleForTesting
  public String explainQuery(String sqlQuery, long requestId) {
    return explainQuery(sqlQuery, CalciteSqlParser.compileToSqlNodeAndOptions(sqlQuery), requestId).getExplainPlan();
  }

  public List<String> getTableNamesForQuery(String sqlQuery) {
    return getTableNamesForQuery(sqlQuery, CalciteSqlParser.compileToSqlNodeAndOptions(sqlQuery).getOptions());
  }

  public List<String> getTableNamesForQuery(String sqlQuery, Map<String, String> options) {
    try (PlannerContext plannerContext = getPlannerContext(options)) {
      SqlNode sqlNode = CalciteSqlParser.compileToSqlNodeAndOptions(sqlQuery).getSqlNode();
      if (sqlNode.getKind().equals(SqlKind.EXPLAIN)) {
        sqlNode = ((SqlExplain) sqlNode).getExplicandum();
      }
      RelRoot relRoot = compileQuery(sqlNode, plannerContext);
      Set<String> tableNames = RelToPlanNodeConverter.getTableNamesFromRelRoot(relRoot.rel);
      return new ArrayList<>(tableNames);
    } catch (Throwable t) {
      throw new RuntimeException("Error composing query plan for: " + sqlQuery, t);
    }
  }

  /**
   * Results of planning a query
   */
  public static class QueryPlannerResult {
    private DispatchableSubPlan _dispatchableSubPlan;
    private String _explainPlan;
    Set<String> _tableNames;

    QueryPlannerResult(@Nullable DispatchableSubPlan dispatchableSubPlan, @Nullable String explainPlan,
        Set<String> tableNames) {
      _dispatchableSubPlan = dispatchableSubPlan;
      _explainPlan = explainPlan;
      _tableNames = tableNames;
    }

    public String getExplainPlan() {
      return _explainPlan;
    }

    public DispatchableSubPlan getQueryPlan() {
      return _dispatchableSubPlan;
    }

    // Returns all the table names in the query.
    public Set<String> getTableNames() {
      return _tableNames;
    }
  }

  // --------------------------------------------------------------------------
  // steps
  // --------------------------------------------------------------------------

  @VisibleForTesting
  protected RelRoot compileQuery(SqlNode sqlNode, PlannerContext plannerContext)
      throws Exception {
    SqlNode validated = validate(sqlNode, plannerContext);
    RelRoot relation = toRelation(validated, plannerContext);
    RelRoot decorrelated = decorrelateIfNeeded(relation);
    RelNode optimized = optimize(decorrelated, plannerContext);
    return relation.withRel(optimized);
  }

  private RelRoot decorrelateIfNeeded(RelRoot relRoot) {
    if (hasCorrelateNode(relRoot.rel)) {
      try {
        relRoot = relRoot.withRel(RelDecorrelator.decorrelateQuery(relRoot.rel, RelBuilder.create(_config)));
      } catch (Throwable e) {
        throw new UnsupportedOperationException(
            "Failed to de-correlate the given query to a valid execution plan: " + RelOptUtil.toString(relRoot.rel), e);
      }
    }
    return relRoot;
  }

  private static boolean hasCorrelateNode(RelNode relNode) {
    if (relNode instanceof LogicalCorrelate) {
      return true;
    }
    for (RelNode input : relNode.getInputs()) {
      if (hasCorrelateNode(input)) {
        return true;
      }
    }
    return false;
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
            PinotConvertletTable.INSTANCE, _config.getSqlToRelConverterConfig());
    RelRoot relRoot = sqlToRelConverter.convertQuery(parsed, false, true);
    return relRoot.withRel(sqlToRelConverter.trimUnusedFields(false, relRoot.rel));
  }

  private RelNode optimize(RelRoot relRoot, PlannerContext plannerContext) {
    // 4. optimize relNode
    // TODO: add support for traits, cost factory.
    try {
      RelOptPlanner optPlanner = plannerContext.getRelOptPlanner();
      optPlanner.setRoot(relRoot.rel);
      RelNode optimized = optPlanner.findBestExp();
      RelOptPlanner traitPlanner = plannerContext.getRelTraitPlanner();
      traitPlanner.setRoot(optimized);
      return traitPlanner.findBestExp();
    } catch (Exception e) {
      throw new UnsupportedOperationException(
          "Cannot generate a valid execution plan for the given query: " + RelOptUtil.toString(relRoot.rel), e);
    }
  }

  private SubPlan toSubPlan(RelRoot relRoot) {
    // 5. construct a logical query plan.
    PinotLogicalQueryPlanner pinotLogicalQueryPlanner = new PinotLogicalQueryPlanner();
    QueryPlan queryPlan = pinotLogicalQueryPlanner.planQuery(relRoot);
    return pinotLogicalQueryPlanner.makePlan(queryPlan);
  }

  private DispatchableSubPlan toDispatchableSubPlan(RelRoot relRoot, PlannerContext plannerContext, long requestId) {
    SubPlan subPlanRoot = toSubPlan(relRoot);
    PinotDispatchPlanner pinotDispatchPlanner =
        new PinotDispatchPlanner(plannerContext, _workerManager, requestId, _tableCache);
    return pinotDispatchPlanner.createDispatchableSubPlan(subPlanRoot);
  }

  // --------------------------------------------------------------------------
  // utils
  // --------------------------------------------------------------------------

  private Prepare.CatalogReader getCatalog(@Nullable String schemaPath) {
    Properties catalogReaderConfigProperties = new Properties();
    catalogReaderConfigProperties.setProperty(CalciteConnectionProperty.CASE_SENSITIVE.camelName(), "true");
    CalciteSchema subSchema = schemaPath == null ? _rootSchema : _rootSchema.getSubSchema(schemaPath, false);
    if (subSchema != null) {
      return new PinotCalciteCatalogReader(subSchema, subSchema.path(null), _typeFactory,
          new CalciteConnectionConfigImpl(catalogReaderConfigProperties));
    } else {
      throw new IllegalArgumentException("Cannot find schema with path: " + schemaPath);
    }
  }

  private FrameworkConfig getConfig(Prepare.CatalogReader catalogReader) {
    return Frameworks.newConfigBuilder().traitDefs()
        .operatorTable(new PinotChainedSqlOperatorTable(Arrays.asList(
            PinotOperatorTable.instance(),
            catalogReader)))
        .defaultSchema(catalogReader.getRootSchema().plus())
        .sqlToRelConverterConfig(SqlToRelConverter.config()
            .withHintStrategyTable(getHintStrategyTable())
            .withTrimUnusedFields(true)
            // SUB-QUERY Threshold is useless as we are encoding all IN clause in-line anyway
            .withInSubQueryThreshold(Integer.MAX_VALUE)
            .addRelBuilderConfigTransform(c -> c.withPushJoinCondition(true))
            .addRelBuilderConfigTransform(c -> c.withAggregateUnique(true))
            .addRelBuilderConfigTransform(c -> c.withPruneInputOfAggregate(false)))
        .build();
  }

  private PlannerContext getPlannerContext(Map<String, String> options) {
    String database = options.getOrDefault(DATABASE_KEY, "default");
    if (database.equalsIgnoreCase("default")) {
      return new PlannerContext(_config, _catalogReader, _typeFactory, _optProgram, _traitProgram);
    } else {
      Prepare.CatalogReader catalogReader = getCatalog(database);
      FrameworkConfig config = getConfig(catalogReader);
      return new PlannerContext(config, catalogReader, _typeFactory, _optProgram, _traitProgram);
    }
  }

  private HintStrategyTable getHintStrategyTable() {
    return PinotHintStrategyTable.PINOT_HINT_STRATEGY_TABLE;
  }

  private static HepProgram getOptProgram() {
    HepProgramBuilder hepProgramBuilder = new HepProgramBuilder();
    // Set the match order as DEPTH_FIRST. The default is arbitrary which works the same as DEPTH_FIRST, but it's
    // best to be explicit.
    hepProgramBuilder.addMatchOrder(HepMatchOrder.DEPTH_FIRST);

    // ----
    // Run the Calcite CORE rules using 1 HepInstruction per rule. We use 1 HepInstruction per rule for simplicity:
    // the rules used here can rest assured that they are the only ones evaluated in a dedicated graph-traversal.
    for (RelOptRule relOptRule : PinotQueryRuleSets.BASIC_RULES) {
      hepProgramBuilder.addRuleInstance(relOptRule);
    }

    // ----
    // Run Pinot rule to attach aggregation auxiliary info
    hepProgramBuilder.addRuleCollection(PinotQueryRuleSets.PINOT_AGG_PROCESS_RULES);

    // ----
    // Pushdown filters using a single HepInstruction.
    hepProgramBuilder.addRuleCollection(PinotQueryRuleSets.FILTER_PUSHDOWN_RULES);

    // ----
    // Prune duplicate/unnecessary nodes using a single HepInstruction.
    // TODO: We can consider using HepMatchOrder.TOP_DOWN if we find cases where it would help.
    hepProgramBuilder.addRuleCollection(PinotQueryRuleSets.PRUNE_RULES);
    return hepProgramBuilder.build();
  }

  private static HepProgram getTraitProgram() {
    HepProgramBuilder hepProgramBuilder = new HepProgramBuilder();

    // Set the match order as BOTTOM_UP.
    hepProgramBuilder.addMatchOrder(HepMatchOrder.BOTTOM_UP);

    // ----
    // Run pinot specific rules that should run after all other rules, using 1 HepInstruction per rule.
    for (RelOptRule relOptRule : PinotQueryRuleSets.PINOT_POST_RULES) {
      hepProgramBuilder.addRuleInstance(relOptRule);
    }

    // apply RelDistribution trait to all nodes
    hepProgramBuilder.addRuleInstance(PinotRelDistributionTraitRule.INSTANCE);

    return hepProgramBuilder.build();
  }
}
