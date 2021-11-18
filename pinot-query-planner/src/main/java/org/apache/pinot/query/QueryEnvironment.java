package org.apache.pinot.query;

import org.apache.pinot.query.exception.QueryException;
import org.apache.pinot.query.planner.QueryPlanner;
import org.apache.pinot.query.planner.QueryPlannerContext;
import org.apache.pinot.query.type.TypeFactory;
import org.apache.pinot.query.type.TypeSystem;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;


public class QueryEnvironment {

  public PinotQueryPlan query(String query) throws QueryException {
    PinotPlannerContext plannerContext = new QueryPlannerContext();
    TypeFactory typeFactory = new TypeFactory(new TypeSystem());
    PinotPlanner planner = getQueryPlanner(typeFactory, plannerContext);
    try {
      SqlNode parsed = planner.parse(query, plannerContext);
      if (null != parsed && parsed.getKind().belongsTo(SqlKind.QUERY)) {
        RelRoot relation = planner.toRelation(parsed, plannerContext);
        RelRoot optimized = planner.optimize(relation, plannerContext);
        return planner.toQuery(optimized, plannerContext);
      } else {
        throw new IllegalArgumentException(String.format("unsupported SQL query, cannot parse out a valid sql from:\n%s", query));
      }
    } catch (Exception e) {
      throw new QueryException("Error processing query", e);
    }
  }

  private static PinotPlanner getQueryPlanner(TypeFactory typeFactory, PinotPlannerContext plannerContext) {
    return new QueryPlanner(typeFactory, plannerContext);
  }
}
