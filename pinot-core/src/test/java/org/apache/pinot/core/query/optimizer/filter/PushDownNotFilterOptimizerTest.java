package org.apache.pinot.core.query.optimizer.filter;

import org.apache.pinot.common.request.Expression;
import org.apache.pinot.sql.parsers.CalciteSqlParser;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;


public class PushDownNotFilterOptimizerTest {
  private static final PushDownNotFilterOptimizer OPTIMIZER = new PushDownNotFilterOptimizer();

  @Test
  public void testPushDownNotFilterOptimizer() {
    testPushDownNot("NOT(AirTime = 141 AND ArrDelay > 1)", "NOT(AirTime = 141) OR NOT(ArrDelay > 1)");
    testPushDownNot("NOT(AirTime = 141 AND ArrDelay > 1 OR ArrTime > 808)", "(NOT(AirTime = 141) OR NOT(ArrDelay > 1)) "
        + "AND NOT(ArrTime > 808)");
    testPushDownNot("NOT(AirTime = 141 OR ArrDelay > 1)", "NOT(AirTime = 141) AND NOT(ArrDelay > 1)");
    testPushDownNot("NOT(AirTime = 141 OR ArrDelay > 1 OR ArrTime > 808)", "NOT(AirTime = 141) AND NOT(ArrDelay > 1) "
        + "AND NOT(ArrTime > 808)");
  }

  @Test
  public void testNoOptimizerChange() {
    testPushDownNot("NOT(AirTime = 141)", "NOT(AirTime = 141)");
    testPushDownNot("AirTime = 141", "AirTime = 141");
    testPushDownNot("true", "true");
  }

  private void testPushDownNot(String filterString, String expectedOptimizedFilterString) {
    Expression expectedExpression = CalciteSqlParser.compileToExpression(expectedOptimizedFilterString);
    Expression optimizedFilterExpression = OPTIMIZER.optimize(CalciteSqlParser.compileToExpression(filterString));
    assertEquals(expectedExpression, optimizedFilterExpression);
  }
}
