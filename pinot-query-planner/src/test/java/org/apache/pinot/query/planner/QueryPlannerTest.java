package org.apache.pinot.query.planner;

import org.apache.pinot.query.PinotPlanner;
import org.apache.pinot.query.PinotPlannerContext;
import org.apache.pinot.query.type.TypeFactory;
import org.apache.pinot.query.type.TypeSystem;
import org.testng.annotations.Test;


public class QueryPlannerTest {

  @Test
  public void testStartUp() {
    PinotPlannerContext plannerContext = new QueryPlannerContext();
    TypeFactory typeFactory = new TypeFactory(new TypeSystem());
    PinotPlanner planner = new QueryPlanner(typeFactory, plannerContext);
    planner.parse("SELECT * FROM t", plannerContext);
  }
}
