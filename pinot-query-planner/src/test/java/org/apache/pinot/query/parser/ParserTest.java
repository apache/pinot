package org.apache.pinot.query.parser;

import org.apache.pinot.query.PinotPlannerContext;
import org.apache.pinot.query.planner.QueryPlannerContext;
import org.apache.calcite.sql.SqlNode;
import org.testng.Assert;
import org.testng.annotations.Test;


public class ParserTest {

  @Test
  public void testSqlStrings() {
    testQueryParsing("SELECT * FROM a JOIN b ON a.c = b.d WHERE a.e >= 0",
        "SELECT *\n" + "FROM `a`\n" + "INNER JOIN `b` ON `a`.`c` = `b`.`d`\n" + "WHERE `a`.`e` >= 0");
  }

  private void testQueryParsing(String query, String digest) {
    PinotPlannerContext plannerContext = new QueryPlannerContext();
    SqlNode sqlNode = CalciteSqlParser.compile(query, plannerContext);
    Assert.assertEquals(sqlNode.toString(), digest);
  }
}
