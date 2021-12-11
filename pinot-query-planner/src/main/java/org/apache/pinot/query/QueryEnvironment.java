package org.apache.pinot.query;

import com.google.common.collect.ImmutableList;
import java.util.Collection;
import java.util.Collections;
import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.plan.Contexts;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTraitSet;
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
import org.apache.pinot.query.logical.QueryPlanner;
import org.apache.pinot.query.parser.CalciteSqlParser;
import org.apache.pinot.query.physical.StagePlanner;
import org.apache.pinot.query.planner.QueryContext;
import org.apache.pinot.query.planner.QueryPlan;
import org.apache.pinot.query.rules.PinotQueryRuleSets;
import org.apache.pinot.query.type.TypeFactory;
import org.apache.pinot.query.validate.Validator;


public class QueryEnvironment {
  private final RelDataTypeFactory _typeFactory;
  private final CalciteSchema _rootSchema;
  private final FrameworkConfig _config;

  private final PlannerImpl _planner;
  private final SqlValidator _validator;
  private final Prepare.CatalogReader _catalogReader;
  private final Collection<RelOptRule> _logicalRuleSet;
  private final RelOptPlanner _relOptPlanner;

  public QueryEnvironment(
      TypeFactory typeFactory,
      CalciteSchema rootSchema) {
    _typeFactory = typeFactory;
    _rootSchema = rootSchema;
    _config = Frameworks.newConfigBuilder().traitDefs().build();

    // this is only here as a placeholder for SqlToRelConverter expandView implementation.
    _planner = new PlannerImpl(_config);

    // catalog
    _catalogReader = new CalciteCatalogReader(_rootSchema, Collections.<String>emptyList(), _typeFactory, null);
    _validator = new Validator(SqlStdOperatorTable.instance(), _catalogReader, _typeFactory);

    // optimizer rules
    _logicalRuleSet = PinotQueryRuleSets.LOGICAL_OPT_RULES;

    // optimizer
    HepProgramBuilder hepProgramBuilder = new HepProgramBuilder();
    for (RelOptRule relOptRule : _logicalRuleSet) {
      hepProgramBuilder.addRuleInstance(relOptRule);
    }
    _relOptPlanner = new QueryPlanner(hepProgramBuilder.build(), Contexts.EMPTY_CONTEXT);
  }

  public QueryPlan sqlQuery(String sqlQuery) {
    QueryContext queryContext = new PlannerContext();
    try {
      SqlNode parsed = this.parse(sqlQuery, queryContext);
      SqlNode validated = this.validate(parsed);
      RelRoot relation = this.toRelation(validated, queryContext);
      RelNode optimized = this.optimize(relation, queryContext);
      return this.toQuery(optimized, queryContext);
    } catch (Exception e) {
      throw new RuntimeException("Error composing query plan", e);
    }
  }

  // --------------------------------------------------------------------------
  // steps
  // --------------------------------------------------------------------------

  protected SqlNode parse(String query, QueryContext queryContext) throws Exception {
    // 1. invoke CalciteSqlParser to parse out SqlNode;
    SqlNode compiled = CalciteSqlParser.compile(query, queryContext);
    // 2. TODO: add query rewrite logic
    return compiled;
  }

  protected SqlNode validate(SqlNode parsed) throws Exception {
    // 3. validator to validate.
    SqlNode validated = _validator.validate(parsed);
    if (null == validated || !validated.getKind().belongsTo(SqlKind.QUERY)) {
      throw new IllegalArgumentException(String.format(
          "unsupported SQL query, cannot validate out a valid sql from:\n%s", parsed));
    }
    return validated;
  }

  protected RelRoot toRelation(SqlNode parsed, QueryContext queryContext) {
    RexBuilder rexBuilder = createRexBuilder();
    RelOptCluster cluster = createRelOptCluster(_relOptPlanner, rexBuilder);
    SqlToRelConverter sqlToRelConverter =
        new SqlToRelConverter(_planner, _validator, _catalogReader, cluster,
            StandardConvertletTable.INSTANCE, SqlToRelConverter.Config.DEFAULT);
    return sqlToRelConverter.convertQuery(parsed, false, true);
  }

  protected RelNode optimize(RelRoot relRoot, QueryContext queryContext) {
//    RelTraitSet traitSet = relRoot.rel.getCluster().traitSet().replace(EnumerableConvention.INSTANCE).simplify();
    try {
      _relOptPlanner.setRoot(relRoot.rel);
      return _relOptPlanner.findBestExp();
    } catch (Exception e) {
      throw new UnsupportedOperationException("Cannot generate a valid execution plan for the given query: " + RelOptUtil.toString(relRoot.rel), e);
    }
  }

  protected QueryPlan toQuery(RelNode relRoot, QueryContext queryContext) {
    StagePlanner queryStagePlanner = new StagePlanner(queryContext);
    return queryStagePlanner.makePlan(relRoot);
  }

  // --------------------------------------------------------------------------
  // utils
  // --------------------------------------------------------------------------
  private RexBuilder createRexBuilder() {
    return new RexBuilder(_typeFactory);
  }

  private RelOptCluster createRelOptCluster(RelOptPlanner relOptPlanner, RexBuilder rexBuilder) {
    return RelOptCluster.create(relOptPlanner, rexBuilder);
  }
}
