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
package org.apache.pinot.query.validate;

import java.util.concurrent.atomic.AtomicLong;
import org.apache.pinot.common.metrics.BrokerMeter;
import org.apache.pinot.common.metrics.BrokerMetrics;
import org.apache.pinot.query.QueryEnvironmentTestBase;
import org.apache.pinot.spi.metrics.NoopPinotMetricsRegistry;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;


/// Checks when [BrokerMeter#QUERIES_WITH_GROUP_BY_ALIAS_SHADOWED_BY_COLUMN] is marked.
public class GroupByAliasShadowedByColumnMeterTest extends QueryEnvironmentTestBase {
  private static final String FLAG = "SET preferSelectAliasInGroupBy=true; ";
  private static final String CASE_ALIAS =
      "SELECT CASE WHEN col1 = 'foo' THEN 'bar' ELSE col1 END AS col1, COUNT(*) FROM a GROUP BY col1";

  private static final CountingBrokerMetrics BROKER_METRICS = new CountingBrokerMetrics();

  @BeforeClass
  public void registerBrokerMetrics() {
    assertTrue(BrokerMetrics.register(BROKER_METRICS) || BrokerMetrics.get() == BROKER_METRICS,
        "Another BrokerMetrics instance is already registered in this JVM");
  }

  @DataProvider
  public Object[][] queries() {
    return new Object[][]{
        {"alias of a different expression shadowed by a column", CASE_ALIAS, 1},
        {"same, with preferSelectAliasInGroupBy", FLAG + CASE_ALIAS, 1},
        {"same, with ORDER BY", CASE_ALIAS + " ORDER BY 1", 1},
        {"alias of another column, query fails", "SELECT col2 AS col1, COUNT(*) FROM a GROUP BY col1", 1},
        {"two shadowed aliases count once",
            "SELECT col2 AS col1, col1 AS col2, COUNT(*) FROM a GROUP BY col1, col2", 1},
        {"shadowed alias in a subquery",
            "SELECT * FROM (SELECT UPPER(col1) AS col1, COUNT(*) AS cnt FROM a GROUP BY col1) WHERE cnt > 1", 1},
        {"column aliased to its own name", "SELECT col1 AS col1, COUNT(*) FROM a GROUP BY col1", 0},
        {"qualified column aliased to its own name", "SELECT a.col1 AS col1, COUNT(*) FROM a GROUP BY col1", 0},
        {"alias that is not a column", "SELECT UPPER(col1) AS upper_col1, COUNT(*) FROM a GROUP BY upper_col1", 0},
        {"column without an alias", "SELECT col1, COUNT(*) FROM a GROUP BY col1", 0},
        {"no group by", "SELECT UPPER(col1) AS col1 FROM a", 0},
    };
  }

  @Test(dataProvider = "queries")
  public void testMeter(String description, String sql, int expectedIncrease) {
    String explain = sql.startsWith(FLAG)
        ? FLAG + "EXPLAIN PLAN FOR " + sql.substring(FLAG.length())
        : "EXPLAIN PLAN FOR " + sql;
    long before = meterCount();
    try {
      _queryEnvironment.explainQuery(explain, 1L);
    } catch (RuntimeException e) {
      // Some queries fail validation. The meter is marked before the failure.
    }
    assertEquals(meterCount() - before, expectedIncrease, description);
  }

  private static long meterCount() {
    return BROKER_METRICS._groupByAliasShadowedByColumnCount.get();
  }

  /// Counts the marks of [BrokerMeter#QUERIES_WITH_GROUP_BY_ALIAS_SHADOWED_BY_COLUMN]. The planner test classpath has
  /// no metrics implementation, so a real registry cannot be used.
  private static class CountingBrokerMetrics extends BrokerMetrics {
    private final AtomicLong _groupByAliasShadowedByColumnCount = new AtomicLong();

    CountingBrokerMetrics() {
      super(new NoopPinotMetricsRegistry());
    }

    @Override
    public void addMeteredGlobalValue(BrokerMeter meter, long unitCount) {
      if (meter == BrokerMeter.QUERIES_WITH_GROUP_BY_ALIAS_SHADOWED_BY_COLUMN) {
        _groupByAliasShadowedByColumnCount.addAndGet(unitCount);
      }
      super.addMeteredGlobalValue(meter, unitCount);
    }
  }
}
