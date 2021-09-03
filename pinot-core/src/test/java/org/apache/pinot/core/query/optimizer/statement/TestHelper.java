package org.apache.pinot.core.query.optimizer.statement;

import org.apache.pinot.common.request.BrokerRequest;
import org.apache.pinot.common.request.PinotQuery;
import org.apache.pinot.core.query.optimizer.QueryOptimizer;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.sql.parsers.CalciteSqlCompiler;
import org.testng.Assert;


class TestHelper {
  private static final QueryOptimizer OPTIMIZER = new QueryOptimizer();
  private static final CalciteSqlCompiler SQL_COMPILER = new CalciteSqlCompiler();

  /**
   * Given two queries, this function will validate that the query obtained after rewriting the first query is the
   * same as the second query.
   */
  static void assertEqualsQuery(String queryOriginal, String queryAfterRewrite, TableConfig config,
      Schema schema) {
    BrokerRequest userBrokerRequest = SQL_COMPILER.compileToBrokerRequest(queryOriginal);
    PinotQuery userQuery = userBrokerRequest.getPinotQuery();
    OPTIMIZER.optimize(userQuery, config, schema);

    BrokerRequest rewrittenBrokerRequest = SQL_COMPILER.compileToBrokerRequest(queryAfterRewrite);
    PinotQuery rewrittenQuery = rewrittenBrokerRequest.getPinotQuery();
    OPTIMIZER.optimize(rewrittenQuery, config, schema);

    // Currently there is no way to specify Double.NEGATIVE_INFINITY in SQL, so in the test cases we specify string
    // '-Infinity' as
    // default null value, but change "stringValue:-Infinity" to "doubleValue:-Infinity" to adjust for internal rewrite.
    Assert.assertEquals(userQuery.toString(),
        rewrittenQuery.toString().replace("stringValue:-Infinity", "doubleValue:-Infinity"));
  }

}
