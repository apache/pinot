package org.apache.pinot.query.planner;

import org.apache.pinot.query.PinotPlanner;
import org.apache.pinot.query.PinotPlannerContext;
import org.apache.pinot.query.PinotQueryPlan;
import org.apache.pinot.query.catalog.CatalogReader;
import org.apache.pinot.query.parser.CalciteSqlParser;
import org.apache.pinot.query.validate.Validator;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.prepare.PlannerImpl;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.sql2rel.StandardConvertletTable;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;


public class QueryPlanner implements PinotPlanner {
  private final RelDataTypeFactory _typeFactory;
  private final PinotPlannerContext _plannerContext;
  private final FrameworkConfig _config;
  private PlannerImpl _planner;
  private RelOptPlanner _relOptPlanner;
  private SqlValidator _validator;
  private Prepare.CatalogReader _catalogReader;
  private RexBuilder _rexBuilder;
  private RelOptCluster _cluster;

  public QueryPlanner(
      RelDataTypeFactory typeFactory,
      PinotPlannerContext plannerContext) {
    _typeFactory = typeFactory;
    _plannerContext = plannerContext;

    _config = Frameworks.newConfigBuilder().build();
    // this is only here as a placeholder for SqlToRelConverter expandView implementation.
    _planner = new PlannerImpl(_config);
    // default cost factory and strategy is used.
    _relOptPlanner = new VolcanoPlanner();
    _catalogReader = new CatalogReader(, null, _typeFactory, null);
    _validator = new Validator(new SqlStdOperatorTable(), _catalogReader, _typeFactory);
  }

  @Override
  public SqlNode parse(String query, PinotPlannerContext plannerContext) {
    SqlNode sqlNode = CalciteSqlParser.compile(query, plannerContext);
    return _validator.validate(sqlNode);
  }

  @Override
  public RelRoot toRelation(SqlNode parsed, PinotPlannerContext plannerContext) {
    _rexBuilder = createRexBuilder();
    _cluster = createRelOptCluster();
    SqlToRelConverter sqlToRelConverter =
        new SqlToRelConverter(_planner, _validator, _catalogReader, _cluster,
            StandardConvertletTable.INSTANCE, null);
    return sqlToRelConverter.convertQuery(parsed, false, true);
  }

  @Override
  public RelRoot optimize(RelRoot relRoot, PinotPlannerContext plannerContext) {
    // no-op for now.
    return relRoot;
  }

  @Override
  public PinotQueryPlan toQuery(RelRoot relRoot, PinotPlannerContext plannerContext) {
    return null;
  }

  private RexBuilder createRexBuilder() {
    return new RexBuilder(_typeFactory);
  }

  private RelOptCluster createRelOptCluster() {
    return RelOptCluster.create(_relOptPlanner, _rexBuilder);
  }
}
