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
import java.io.Closeable;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.calcite.config.CalciteConnectionConfig;
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
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.runtime.CalciteContextException;
import org.apache.calcite.sql.SqlExplain;
import org.apache.calcite.sql.SqlExplainFormat;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.validate.SqlNameMatcher;
import org.apache.calcite.sql2rel.RelDecorrelator;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.RelBuilder;
import org.apache.pinot.calcite.rel.rules.PinotImplicitTableHintRule;
import org.apache.pinot.calcite.rel.rules.PinotJoinToDynamicBroadcastRule;
import org.apache.pinot.calcite.rel.rules.PinotQueryRuleSets;
import org.apache.pinot.calcite.rel.rules.PinotRelDistributionTraitRule;
import org.apache.pinot.calcite.rel.rules.PinotRuleUtils;
import org.apache.pinot.calcite.sql.fun.PinotOperatorTable;
import org.apache.pinot.calcite.sql2rel.PinotConvertletTable;
import org.apache.pinot.common.config.provider.TableCache;
import org.apache.pinot.common.utils.config.QueryOptionsUtils;
import org.apache.pinot.query.catalog.PinotCatalog;
import org.apache.pinot.query.context.PlannerContext;
import org.apache.pinot.query.context.RuleTimingPlannerListener;
import org.apache.pinot.query.planner.PlannerUtils;
import org.apache.pinot.query.planner.SubPlan;
import org.apache.pinot.query.planner.explain.AskingServerStageExplainer;
import org.apache.pinot.query.planner.explain.MultiStageExplainAskingServersUtils;
import org.apache.pinot.query.planner.explain.PhysicalExplainPlanVisitor;
import org.apache.pinot.query.planner.logical.PinotLogicalQueryPlanner;
import org.apache.pinot.query.planner.logical.RelToPlanNodeConverter;
import org.apache.pinot.query.planner.logical.TransformationTracker;
import org.apache.pinot.query.planner.physical.DispatchableSubPlan;
import org.apache.pinot.query.planner.physical.PinotDispatchPlanner;
import org.apache.pinot.query.planner.plannode.PlanNode;
import org.apache.pinot.query.routing.WorkerManager;
import org.apache.pinot.query.type.TypeFactory;
import org.apache.pinot.query.validate.BytesCastVisitor;
import org.apache.pinot.spi.exception.QueryErrorCode;
import org.apache.pinot.spi.exception.QueryException;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.sql.parsers.CalciteSqlParser;
import org.apache.pinot.sql.parsers.SqlNodeAndOptions;
import org.apache.pinot.sql.parsers.parser.SqlPhysicalExplain;
import org.immutables.value.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * The {@code QueryEnvironment} contains the main entrypoint for query planning.
 *
 * <p>It provide the higher level entry interface to convert a SQL string into a {@link DispatchableSubPlan}.
 * It is also used to execute some static analysis on the query like to determine if it can be compiled or get the
 * tables involved in the query.
 *
 * Queries are first compiled with Calcite into {@link CompiledQuery} objects, which can then be used to plan, explain
 * or get the tables involved in the query. These later processes are Pinot specific. They include for example how to
 * distribute the query to the workers, which is not a Calcite native concept.
 *
 * To learn more about Calcite compilation process, read
 * <a href="https://www.querifylabs.com/blog/relational-operators-in-apache-calcite">this Querify Labs post</a>.
 */

//TODO: We should consider splitting this class in two: One that is used for parsing and one that is used for
// executing queries. This would allow us to remove the worker manager from the parsing environment and therefore
// make sure there is a worker manager when executing queries.
@Value.Enclosing
public class QueryEnvironment {
  private static final Logger LOGGER = LoggerFactory.getLogger(QueryEnvironment.class);

  private final TypeFactory _typeFactory = new TypeFactory();
  private final FrameworkConfig _config;
  private final CalciteCatalogReader _catalogReader;
  private final HepProgram _optProgram;
  private final Config _envConfig;
  private final PinotCatalog _catalog;

  public QueryEnvironment(Config config) {
    _envConfig = config;
    String database = config.getDatabase();
    _catalog = new PinotCatalog(config.getTableCache(), database);
    CalciteSchema rootSchema = CalciteSchema.createRootSchema(false, false, database, _catalog);
    Properties connectionConfigProperties = new Properties();
    /*connectionConfigProperties.setProperty(CalciteConnectionProperty.CASE_SENSITIVE.camelName(), Boolean.toString(
        config.getTableCache() == null
            ? !CommonConstants.Helix.DEFAULT_ENABLE_CASE_INSENSITIVE
            : !config.getTableCache().isIgnoreCase()));*/
    connectionConfigProperties.setProperty(CalciteConnectionProperty.CASE_SENSITIVE.camelName(), "true");
    CalciteConnectionConfig connectionConfig = new CalciteConnectionConfigImpl(connectionConfigProperties);
    _config = Frameworks.newConfigBuilder().traitDefs().operatorTable(PinotOperatorTable.instance())
        .defaultSchema(rootSchema.plus()).sqlToRelConverterConfig(PinotRuleUtils.PINOT_SQL_TO_REL_CONFIG).build();
    _catalogReader = new CalciteCatalogReader(rootSchema, List.of(database), _typeFactory, connectionConfig);
    boolean makeCaseInsensitive = config.getTableCache() == null
        ? CommonConstants.Helix.DEFAULT_ENABLE_CASE_INSENSITIVE
        : config.getTableCache().isIgnoreCase();
    if (makeCaseInsensitive) {
      SqlNameMatcher nameMatcher = _catalogReader.nameMatcher();
      try {
        // very controversial hack: we need to set the name matcher to be case insensitive without making the whole
        // Calcite configuration case insensitive, cause that leads to transform everything internally to lower-case
        Field field = nameMatcher.getClass().getDeclaredField("caseSensitive");
        field.setAccessible(true);
        field.set(nameMatcher, false);
      } catch (Exception e) {
        LOGGER.warn("Unable to force case-insensitiveness internally", e);
      }
    }
    _optProgram = getOptProgram();
  }

  public QueryEnvironment(String database, TableCache tableCache, @Nullable WorkerManager workerManager) {
    this(configBuilder()
        .database(database)
        .tableCache(tableCache)
        .workerManager(workerManager)
        .build());
  }

  /**
   * Returns a planner context that can be used to either parse, explain or execute a query.
   */
  private PlannerContext getPlannerContext(SqlNodeAndOptions sqlNodeAndOptions) {
    WorkerManager workerManager = getWorkerManager(sqlNodeAndOptions);
    HepProgram traitProgram = getTraitProgram(workerManager, _envConfig);
    SqlExplainFormat format = SqlExplainFormat.DOT;
    if (sqlNodeAndOptions.getSqlNode().getKind().equals(SqlKind.EXPLAIN)) {
      SqlExplain explain = (SqlExplain) sqlNodeAndOptions.getSqlNode();
      if (explain.getFormat() != null) {
        format = explain.getFormat();
      }
    }
    return new PlannerContext(_config, _catalogReader, _typeFactory, _optProgram, traitProgram,
        sqlNodeAndOptions.getOptions(), _envConfig, format);
  }

  /// @deprecated Use [#compile] and then [plan][CompiledQuery#planQuery(long)] the returned query instead
  @VisibleForTesting
  @Deprecated
  public DispatchableSubPlan planQuery(String sqlQuery) {
    try (CompiledQuery compiledQuery = compile(sqlQuery)) {
      return compiledQuery.planQuery(0).getQueryPlan();
    }
  }

  @Nullable
  private WorkerManager getWorkerManager(SqlNodeAndOptions sqlNodeAndOptions) {
    String inferPartitionHint = sqlNodeAndOptions.getOptions()
        .get(CommonConstants.Broker.Request.QueryOptionKey.INFER_PARTITION_HINT);
    WorkerManager workerManager = _envConfig.getWorkerManager();

    if (inferPartitionHint == null) {
      return _envConfig.defaultInferPartitionHint() ? workerManager : null;
    }
    switch (inferPartitionHint.toLowerCase()) {
      case "true":
        return workerManager;
      case "false":
        return null;
      default:
        throw new RuntimeException("Invalid value for query option '"
            + CommonConstants.Broker.Request.QueryOptionKey.INFER_PARTITION_HINT + "': "
            + inferPartitionHint);
    }
  }

  private QueryEnvironment.QueryPlannerResult getQueryPlannerResult(PlannerContext plannerContext,
      DispatchableSubPlan dispatchableSubPlan, String explainStr, Set<String> tableNames) {
    Map<String, String> extraFields = new HashMap<>();
    if (plannerContext.getPlannerOutput().containsKey(RuleTimingPlannerListener.RULE_TIMINGS)) {
      extraFields.put(RuleTimingPlannerListener.RULE_TIMINGS,
          plannerContext.getPlannerOutput().get(RuleTimingPlannerListener.RULE_TIMINGS));
    }
    return new QueryPlannerResult(dispatchableSubPlan, explainStr, tableNames, extraFields);
  }

  /// @deprecated Use [#compile] and then [explain][CompiledQuery#explain(long) ] the returned query instead
  @VisibleForTesting
  @Deprecated
  public String explainQuery(String sqlQuery, long requestId) {
    SqlNodeAndOptions sqlNodeAndOptions = CalciteSqlParser.compileToSqlNodeAndOptions(sqlQuery);
    try (CompiledQuery compiledQuery = compile(sqlQuery, sqlNodeAndOptions)) {
      QueryPlannerResult queryPlannerResult = compiledQuery.explain(requestId, null);
      return queryPlannerResult.getExplainPlan();
    }
  }

  public CompiledQuery compile(String sqlQuery) {
    return compile(sqlQuery, CalciteSqlParser.compileToSqlNodeAndOptions(sqlQuery));
  }

  /// Given a query, parses, validates and optimizes the query into a [CompiledQuery].
  ///
  /// The returned query can then be planned, explained or used to get the tables involved in the query.
  ///
  /// @throws QueryException if the query cannot be compiled. Usual error types are QueryErrorCode.SQL_PARSING and
  /// QueryErrorCode.QUERY_VALIDATION. QueryErrorCode.QUERY_EXECUTION is also possible if there is an error when
  /// a function call is reduced into a constant.
  public CompiledQuery compile(String sqlQuery, SqlNodeAndOptions sqlNodeAndOptions) {
    PlannerContext plannerContext = null;
    try {
      plannerContext = getPlannerContext(sqlNodeAndOptions);

      SqlNode sqlNode = sqlNodeAndOptions.getSqlNode();
      SqlNode queryNode;
      if (sqlNode.getKind().equals(SqlKind.EXPLAIN)) {
        queryNode = ((SqlExplain) sqlNode).getExplicandum();
      } else {
        queryNode = sqlNode;
      }
      RelRoot relRoot = compileQuery(queryNode, plannerContext);
      return new CompiledQuery(_envConfig.getDatabase(), sqlQuery, relRoot, plannerContext, sqlNodeAndOptions);
    } catch (QueryException e) {
      throw e;
    } catch (Throwable t) {
      if (plannerContext != null) {
        plannerContext.close();
      }
      throw QueryErrorCode.SQL_PARSING.asException("Error composing query plan: " + t.getMessage(), t);
    }
  }

  /// @deprecated Use [#compile] and then [getTableNames][CompiledQuery#getTableNames()] the returned query instead
  @VisibleForTesting
  @Deprecated
  public List<String> getTableNamesForQuery(String sqlQuery) {
    try (CompiledQuery compiledQuery = compile(sqlQuery, CalciteSqlParser.compileToSqlNodeAndOptions(sqlQuery))) {
      return new ArrayList<>(compiledQuery.getTableNames());
    }
  }

  /**
   * Returns whether the query can be successfully compiled in this query environment
   */
  public boolean canCompileQuery(String query) {
    try (CompiledQuery compiledQuery = compile(query)) {
      return true;
    } catch (QueryException e) {
      return false;
    }
  }

  /**
   * Results of planning a query
   */
  public static class QueryPlannerResult {
    private final DispatchableSubPlan _dispatchableSubPlan;
    private final String _explainPlan;
    private final Set<String> _tableNames;
    private final Map<String, String> _extraFields;

    QueryPlannerResult(@Nullable DispatchableSubPlan dispatchableSubPlan, @Nullable String explainPlan,
        Set<String> tableNames, Map<String, String> extraFields) {
      _dispatchableSubPlan = dispatchableSubPlan;
      _explainPlan = explainPlan;
      _tableNames = tableNames;
      _extraFields = extraFields;
    }

    public String getExplainPlan() {
      return _explainPlan;
    }

    public DispatchableSubPlan getQueryPlan() {
      return _dispatchableSubPlan;
    }

    public Set<String> getTableNames() {
      return _tableNames;
    }

    public Map<String, String> getExtraFields() {
      return _extraFields;
    }
  }

  // --------------------------------------------------------------------------
  // steps
  // --------------------------------------------------------------------------

  private RelRoot compileQuery(SqlNode sqlNode, PlannerContext plannerContext) {
    SqlNode validated = validate(sqlNode, plannerContext);
    RelRoot relation = toRelation(validated, plannerContext);
    RelNode optimized = optimize(relation, plannerContext);
    return relation.withRel(optimized);
  }

  /// Query validation is a transformation from SqlNode to SqlNode where each node is validated.
  ///
  /// In Calcite a SqlNode is a tree of nodes where each node is a part of the SQL query. These nodes are the output of
  /// the parsing process and are actually bound to the SQL text being written (ie they include the line and column
  /// where each node starts and ends in the SQL text).
  ///
  /// The parser is responsible for creating these nodes, but doesn't validate their semantic. For example, they may
  /// be using tables that doesn't exist or calling functions with arguments of an incorrect type. The validation
  /// process is where these errors are caught.
  ///
  /// In case there is no error, the returned tree is semantically the same as the input tree, but it is now typed and
  /// may be slightly different. For example, a function call may have its arguments casted to the correct type (only
  /// in case it was legal to apply an automatic cast for these types!).
  private SqlNode validate(SqlNode sqlNode, PlannerContext plannerContext) {
    try {
      SqlNode validated = plannerContext.getValidator().validate(sqlNode);
      if (!validated.getKind().belongsTo(SqlKind.QUERY)) {
        throw new IllegalArgumentException("Unsupported SQL query, failed to validate query:\n" + sqlNode);
      }
      validated.accept(new BytesCastVisitor(plannerContext.getValidator()));
      return validated;
    } catch (QueryException e) {
      throw e;
    } catch (CalciteContextException e) {
      throw CalciteContextExceptionClassifier.classifyValidationException(e);
    } catch (Throwable e) {
      throw QueryErrorCode.QUERY_VALIDATION.asException(e.getMessage(), e);
    }
  }

  /// Converts a validated SqlNode into a relational expression formed by a [RelRoot] which contains a [RelNode].
  ///
  /// In Calcite a RelNode is a tree of nodes where each node is a part of the relational algebra. Contrary to
  /// SqlNode, RelNode is not bound to the SQL text being written and they are always typed.
  ///
  /// The tree returned here may have been slightly modifies. For example, function calls whose arguments are constants
  /// may have been reduced to a constant. Some other modifications may have also been applied.
  /// See [SqlToRelConverter.Config#isExpand()].
  ///
  /// It is important to notice that the returned tree is not yet [optimized][#optimize(RelRoot, PlannerContext)].
  private RelRoot toRelation(SqlNode sqlNode, PlannerContext plannerContext) {
    try {
      RexBuilder rexBuilder = new RexBuilder(_typeFactory);
      RelOptCluster cluster = RelOptCluster.create(plannerContext.getRelOptPlanner(), rexBuilder);
      SqlToRelConverter converter =
          new SqlToRelConverter(plannerContext.getPlanner(), plannerContext.getValidator(), _catalogReader, cluster,
              PinotConvertletTable.INSTANCE, _config.getSqlToRelConverterConfig());
      RelRoot relRoot;
      try {
        relRoot = converter.convertQuery(sqlNode, false, true);
      } catch (Throwable e) {
        throw new RuntimeException("Failed to convert query to relational expression:\n" + sqlNode, e);
      }
      RelNode rootNode = relRoot.rel;
      try {
        // NOTE: DO NOT use converter.decorrelate(sqlNode, rootNode) because the converted type check can fail. This is
        //       probably a bug in Calcite.
        RelBuilder relBuilder = PinotRuleUtils.PINOT_REL_FACTORY.create(cluster, null);
        rootNode = RelDecorrelator.decorrelateQuery(rootNode, relBuilder);
      } catch (Throwable e) {
        throw new RuntimeException("Failed to decorrelate query:\n" + RelOptUtil.toString(rootNode), e);
      }
      try {
        rootNode = converter.trimUnusedFields(false, rootNode);
      } catch (Throwable e) {
        throw new RuntimeException("Failed to trim unused fields from query:\n" + RelOptUtil.toString(rootNode), e);
      }
      return relRoot.withRel(rootNode);
    } catch (QueryException e) {
      throw e;
    } catch (Throwable e) {
      throw QueryErrorCode.QUERY_PLANNING.asException(
          "Error converting query to relational expression: " + e.getMessage(), e);
    }
  }

  /// Optimizes a relational expression formed by a [RelRoot], returning an equivalent optimized [RelNode].
  ///
  /// In order to optimize the query, we use different [programs][HepProgram] that are composed of a sequence of
  /// [rules][RelOptRule]. These rules are applied to the tree of nodes that form the relational expression and
  /// optionally generate new nodes.
  ///
  /// The result of the method is an optimized tree of nodes that is semantically equivalent to the input tree, but
  /// may be more efficient to execute. This doesn't mean that the query is ready to use. In fact, in fact it can be
  /// further optimized by applying Pinot specific. But this is the further we can go with Calcite.
  private RelNode optimize(RelRoot relRoot, PlannerContext plannerContext) {
    // TODO: add support for cost factory
    try {
      RelOptPlanner optPlanner = plannerContext.getRelOptPlanner();
      optPlanner.setRoot(relRoot.rel);
      RuleTimingPlannerListener listener = new RuleTimingPlannerListener(plannerContext);
      optPlanner.addListener(listener);
      RelNode optimized = optPlanner.findBestExp();
      listener.printRuleTimings();
      listener.populateRuleTimings();
      RelOptPlanner traitPlanner = plannerContext.getRelTraitPlanner();
      traitPlanner.setRoot(optimized);
      return traitPlanner.findBestExp();
    } catch (Throwable e) {
      throw QueryErrorCode.QUERY_PLANNING.asException("Error optimizing query: " + e.getMessage(), e);
    }
  }

  private DispatchableSubPlan toDispatchableSubPlan(RelRoot relRoot, PlannerContext plannerContext, long requestId) {
    return toDispatchableSubPlan(relRoot, plannerContext, requestId, null);
  }

  private DispatchableSubPlan toDispatchableSubPlan(RelRoot relRoot, PlannerContext plannerContext, long requestId,
      @Nullable TransformationTracker.Builder<PlanNode, RelNode> tracker) {
    SubPlan plan = PinotLogicalQueryPlanner.makePlan(relRoot, tracker,
        _envConfig.getTableCache(), useSpools(plannerContext.getOptions()));
    PinotDispatchPlanner pinotDispatchPlanner =
        new PinotDispatchPlanner(plannerContext, _envConfig.getWorkerManager(), requestId, _envConfig.getTableCache());
    return pinotDispatchPlanner.createDispatchableSubPlan(plan);
  }

  // --------------------------------------------------------------------------
  // utils
  // --------------------------------------------------------------------------

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
    // Pushdown filters using a single HepInstruction.
    hepProgramBuilder.addRuleCollection(PinotQueryRuleSets.FILTER_PUSHDOWN_RULES);

    // Pushdown projects after first filter pushdown to minimize projected columns.
    hepProgramBuilder.addRuleCollection(PinotQueryRuleSets.PROJECT_PUSHDOWN_RULES);

    // Pushdown filters again since filter should be pushed down at the lowest level, after project pushdown.
    hepProgramBuilder.addRuleCollection(PinotQueryRuleSets.FILTER_PUSHDOWN_RULES);

    // ----
    // Prune duplicate/unnecessary nodes using a single HepInstruction.
    // TODO: We can consider using HepMatchOrder.TOP_DOWN if we find cases where it would help.
    hepProgramBuilder.addRuleCollection(PinotQueryRuleSets.PRUNE_RULES);
    return hepProgramBuilder.build();
  }

  private static HepProgram getTraitProgram(@Nullable WorkerManager workerManager, Config config) {
    HepProgramBuilder hepProgramBuilder = new HepProgramBuilder();

    // Set the match order as BOTTOM_UP.
    hepProgramBuilder.addMatchOrder(HepMatchOrder.BOTTOM_UP);

    // ----
    // Run pinot specific rules that should run after all other rules, using 1 HepInstruction per rule.
    for (RelOptRule relOptRule : PinotQueryRuleSets.PINOT_POST_RULES) {
      if (isEligibleQueryPostRule(relOptRule, config)) {
        hepProgramBuilder.addRuleInstance(relOptRule);
      }
    }

    // apply RelDistribution trait to all nodes
    if (workerManager != null) {
      hepProgramBuilder.addRuleInstance(PinotImplicitTableHintRule.withWorkerManager(workerManager));
    }
    hepProgramBuilder.addRuleInstance(PinotRelDistributionTraitRule.INSTANCE);

    return hepProgramBuilder.build();
  }

  // This method is used to filter out post rules that are not eligible to run based on the config.
  private static boolean isEligibleQueryPostRule(RelOptRule relOptRule, Config config) {
    if (relOptRule instanceof PinotJoinToDynamicBroadcastRule && !config.defaultEnableDynamicFilteringSemiJoin()) {
      return false;
    }
    return true;
  }

  public static ImmutableQueryEnvironment.Config.Builder configBuilder() {
    return ImmutableQueryEnvironment.Config.builder();
  }

  public boolean useSpools(Map<String, String> options) {
    String optionValue = options.get(CommonConstants.Broker.Request.QueryOptionKey.USE_SPOOLS);
    if (optionValue == null) {
      return _envConfig.defaultUseSpools();
    }
    return Boolean.parseBoolean(optionValue);
  }

  @Value.Immutable
  public interface Config {
    String getDatabase();

    /**
     * In theory nullable only in tests. We should fix LiteralOnlyBrokerRequestTest to not need this.
     */
    @Nullable
    TableCache getTableCache();

    /**
     * Whether to apply partition hint by default or not.
     *
     * This is treated as the default value for the broker and it is expected to be obtained from a Pinot configuration.
     * This default value can be always overridden at query level by the query option
     * {@link CommonConstants.Broker.Request.QueryOptionKey#INFER_PARTITION_HINT}.
     */
    @Value.Default
    default boolean defaultInferPartitionHint() {
      return CommonConstants.Broker.DEFAULT_INFER_PARTITION_HINT;
    }

    /**
     * Whether to use spools or not.
     *
     * This is treated as the default value for the broker and it is expected to be obtained from a Pinot configuration.
     * This default value can be always overridden at query level by the query option
     * {@link CommonConstants.Broker.Request.QueryOptionKey#USE_SPOOLS}.
     */
    @Value.Default
    default boolean defaultUseSpools() {
      return CommonConstants.Broker.DEFAULT_OF_SPOOLS;
    }


    @Value.Default
    default boolean defaultEnableGroupTrim() {
      return CommonConstants.Broker.DEFAULT_MSE_ENABLE_GROUP_TRIM;
    }

    @Value.Default
    default boolean defaultEnableDynamicFilteringSemiJoin() {
      return CommonConstants.Broker.DEFAULT_ENABLE_DYNAMIC_FILTERING_SEMI_JOIN;
    }

    /**
     * Returns the worker manager.
     *
     * This is used whenever the query needs to be executed, but can be null when the QueryEnvironment will be used
     * just to execute some static analysis on the query like parsing it or getting the tables involved in the query.
     */
    @Nullable
    WorkerManager getWorkerManager();
  }

  /// A query that have been parsed, validates, transformed into a [RelNode] and optimized with Calcite.
  ///
  /// This represents the last point where Calcite is being used. This object can then be:
  /// - Used to get the tables involved in the query (see [#getTableNames])
  /// - Used to explain the query plan (see [#explain])
  /// - Used to plan how to evaluate the query using Pinot Engine (see [#planQuery])
  ///
  /// Compiled queries are created by calling [QueryEnvironment#compile] and should be closed to release resources,
  /// including the [PlannerContext].
  /// They are also not static classes. Instead they are bound to the [QueryEnvironment] that created them.
  public class CompiledQuery implements Closeable {
    private final String _database;
    private final String _textQuery;
    private final RelRoot _relRoot;
    private final PlannerContext _plannerContext;
    private final SqlNodeAndOptions _sqlNodeAndOptions;
    private final Set<String> _tableNames;

    private CompiledQuery(String database, String textQuery, RelRoot relRoot, PlannerContext plannerContext,
        SqlNodeAndOptions sqlNodeAndOptions) {
      _database = database;
      _textQuery = textQuery;
      _relRoot = relRoot;
      _plannerContext = plannerContext;
      _sqlNodeAndOptions = sqlNodeAndOptions;
      // Important & tricky: RelToPlanNodeConverter uses thread local. Therefore we need to get the table names here
      // instead of lazily in getTableNames() method.
      _tableNames = RelToPlanNodeConverter.getTableNamesFromRelRoot(relRoot.rel);
    }

    public Set<String> getTableNames() {
      return _tableNames;
    }

    public boolean isExplain() {
      return _sqlNodeAndOptions.getSqlNode().getKind().equals(SqlKind.EXPLAIN);
    }

    /// Explain the query plan.
    /// The original query must be an EXPLAIN query and way it will be explained depends on the options of the EXPLAIN
    /// query and the [QueryEnvironment.Config] used to create the [QueryEnvironment] that compiled this query.
    public QueryEnvironment.QueryPlannerResult explain(long requestId,
        @Nullable AskingServerStageExplainer.OnServerExplainer onServerExplainer) {
      try {
        SqlExplain explain = (SqlExplain) _sqlNodeAndOptions.getSqlNode();

        SqlExplainFormat format = _plannerContext.getSqlExplainFormat();
        if (explain instanceof SqlPhysicalExplain) {
          // get the physical plan for query.
          DispatchableSubPlan dispatchableSubPlan = toDispatchableSubPlan(_relRoot, _plannerContext, requestId);
          return getQueryPlannerResult(_plannerContext, dispatchableSubPlan,
              PhysicalExplainPlanVisitor.explain(dispatchableSubPlan), dispatchableSubPlan.getTableNames());
        } else {
          // get the logical plan for query.
          SqlExplainLevel level =
              explain.getDetailLevel() == null ? SqlExplainLevel.DIGEST_ATTRIBUTES : explain.getDetailLevel();
          Set<String> tableNames = RelToPlanNodeConverter.getTableNamesFromRelRoot(_relRoot.rel);
          if (!explain.withImplementation() || onServerExplainer == null) {
            return getQueryPlannerResult(_plannerContext, null, PlannerUtils.explainPlan(_relRoot.rel, format, level),
                tableNames);
          } else {
            Map<String, String> options = _sqlNodeAndOptions.getOptions();
            boolean explainPlanVerbose = QueryOptionsUtils.isExplainPlanVerbose(options);

            // A map from the actual PlanNodes to the original RelNode in the logical rel tree
            TransformationTracker.ByIdentity.Builder<PlanNode, RelNode> nodeTracker =
                new TransformationTracker.ByIdentity.Builder<>();
            // Transform RelNodes into DispatchableSubPlan
            DispatchableSubPlan dispatchableSubPlan =
                toDispatchableSubPlan(_relRoot, _plannerContext, requestId, nodeTracker);

            AskingServerStageExplainer serversExplainer = new AskingServerStageExplainer(
                onServerExplainer, explainPlanVerbose, RelBuilder.create(_config));

            RelNode explainedNode = MultiStageExplainAskingServersUtils.modifyRel(_relRoot.rel,
                dispatchableSubPlan.getQueryStages(), nodeTracker, serversExplainer);

            return getQueryPlannerResult(_plannerContext, dispatchableSubPlan,
                PlannerUtils.explainPlan(explainedNode, format, level), dispatchableSubPlan.getTableNames());
          }
        }
      } catch (Exception e) {
        throw new RuntimeException("Error explain query plan for: " + _textQuery, e);
      }
    }

    /// Plan the query, returning a [QueryPlannerResult] that can be then sent to the workers to execute the query.
    public QueryPlannerResult planQuery(long requestId) {
      try {
        // TODO: current code only assume one SubPlan per query, but we should support multiple SubPlans per query.
        // Each SubPlan should be able to run independently from Broker then set the results into the dependent
        // SubPlan for further processing.
        DispatchableSubPlan dispatchableSubPlan = toDispatchableSubPlan(_relRoot, _plannerContext, requestId);
        return getQueryPlannerResult(_plannerContext, dispatchableSubPlan, null, dispatchableSubPlan.getTableNames());
      } catch (QueryException e) {
        throw e;
      } catch (Throwable t) {
        throw new RuntimeException("Error composing query plan for '" + _textQuery + "': " + t.getMessage() + "'", t);
      }
    }

    @Override
    public void close() {
      _plannerContext.close();
    }

    public String getTextQuery() {
      return _textQuery;
    }

    public SqlNodeAndOptions getSqlNodeAndOptions() {
      return _sqlNodeAndOptions;
    }

    public String getDatabase() {
      return _database;
    }

    public Map<String, String> getOptions() {
      return _sqlNodeAndOptions.getOptions();
    }

    public RelRoot getRelRoot() {
      return _relRoot;
    }

    public RelNode getRelNode() {
      return _relRoot.rel;
    }
  }
}
