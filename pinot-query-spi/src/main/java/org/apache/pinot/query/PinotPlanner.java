package org.apache.pinot.query;

import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.sql.SqlNode;

public interface PinotPlanner {
  SqlNode parse(String query, PinotPlannerContext plannerContext);

  RelRoot toRelation(SqlNode parsed, PinotPlannerContext plannerContext);

  RelRoot optimize(RelRoot relation, PinotPlannerContext plannerContext);

  PinotQueryPlan toQuery(RelRoot optimized, PinotPlannerContext plannerContext);
}
