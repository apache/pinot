package org.apache.pinot.query;

import java.util.Collections;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.prepare.PlannerImpl;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.sql2rel.StandardConvertletTable;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.pinot.query.parser.CalciteSqlParser;
import org.apache.pinot.query.context.PlannerContext;
import org.apache.pinot.query.type.TypeFactory;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.sql.SqlNode;
import org.apache.pinot.query.validate.Validator;


public class QueryEnvironment {
  private final RelDataTypeFactory _typeFactory;
  private final CalciteSchema _rootSchema;
  private final FrameworkConfig _config;

  private final PlannerImpl _planner;
  private final RelOptPlanner _relOptPlanner;
  private final SqlValidator _validator;
  private final Prepare.CatalogReader _catalogReader;
  private final RexBuilder _rexBuilder;
  private final RelOptCluster _cluster;

  public QueryEnvironment(
      TypeFactory typeFactory,
      CalciteSchema rootSchema) {
    _typeFactory = typeFactory;
    _rootSchema = rootSchema;
    _config = Frameworks.newConfigBuilder().build();

    // this is only here as a placeholder for SqlToRelConverter expandView implementation.
    _planner = new PlannerImpl(_config);
    // default cost factory and strategy is used.
    _relOptPlanner = new VolcanoPlanner();
    _rexBuilder = createRexBuilder();
    _cluster = createRelOptCluster();

    // catalog
    _catalogReader = new CalciteCatalogReader(_rootSchema, Collections.<String>emptyList(), _typeFactory, null);
    _validator = new Validator(new SqlStdOperatorTable(), _catalogReader, _typeFactory);
  }

  public QueryPlan sqlQuery(String sqlQuery) {
    QueryContext queryContext = new PlannerContext();
    try {
      SqlNode parsed = this.parse(sqlQuery, queryContext);
      SqlNode validated = this.validate(parsed);
      RelRoot relation = this.toRelation(validated, queryContext);
      RelRoot relRoot = this.optimize(relation, queryContext);
      return this.toQuery(relRoot, queryContext);
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
    if (null != validated && validated.getKind().belongsTo(SqlKind.QUERY)) {
      throw new IllegalArgumentException(String.format(
          "unsupported SQL query, cannot validate out a valid sql from:\n%s", parsed));
    }
    return validated;
  }

  protected RelRoot toRelation(SqlNode parsed, QueryContext queryContext) {
    SqlToRelConverter sqlToRelConverter =
        new SqlToRelConverter(_planner, _validator, _catalogReader, _cluster,
            StandardConvertletTable.INSTANCE, null);
    return sqlToRelConverter.convertQuery(parsed, false, true);
  }

  protected RelRoot optimize(RelRoot relRoot, QueryContext queryContext) {
    // no-op for now.
    return relRoot;
  }

  protected QueryPlan toQuery(RelRoot relRoot, QueryContext queryContext) {
    // no working for now
    return null;
  }

  // --------------------------------------------------------------------------
  // utils
  // --------------------------------------------------------------------------
  private RexBuilder createRexBuilder() {
    return new RexBuilder(_typeFactory);
  }

  private RelOptCluster createRelOptCluster() {
    return RelOptCluster.create(_relOptPlanner, _rexBuilder);
  }
}
