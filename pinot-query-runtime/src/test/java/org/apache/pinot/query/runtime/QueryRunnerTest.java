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
package org.apache.pinot.query.runtime;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.pinot.common.utils.NamedThreadFactory;
import org.apache.pinot.core.query.scheduler.resources.ResourceManager;
import org.apache.pinot.core.transport.ServerInstance;
import org.apache.pinot.query.planner.QueryPlan;
import org.apache.pinot.query.planner.stage.MailboxReceiveNode;
import org.apache.pinot.query.runtime.operator.MailboxReceiveOperator;
import org.apache.pinot.query.runtime.plan.DistributedStagePlan;
import org.apache.pinot.query.service.QueryDispatcher;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


public class QueryRunnerTest extends QueryRunnerTestBase {
  private static final ExecutorService EXECUTOR_SERVICE = Executors.newFixedThreadPool(
      ResourceManager.DEFAULT_QUERY_WORKER_THREADS, new NamedThreadFactory("query_server_enclosure"));

  @Test(dataProvider = "testDataWithSqlToFinalRowCount")
  public void testSqlWithFinalRowCountChecker(String sql, int expectedRows)
      throws Exception {
    List<Object[]> resultRows = queryRunner(sql);
    Assert.assertEquals(resultRows.size(), expectedRows);
  }

  @Test(dataProvider = "testSql")
  public void testSqlWithH2Checker(String sql)
      throws Exception {
    List<Object[]> resultRows = queryRunner(sql);
    // query H2 for data
    List<Object[]> expectedRows = queryH2(sql);
    compareRowEquals(resultRows, expectedRows);
  }

  private List<Object[]> queryRunner(String sql) {
    QueryPlan queryPlan = _queryEnvironment.planQuery(sql);
    Map<String, String> requestMetadataMap =
        ImmutableMap.of("REQUEST_ID", String.valueOf(RANDOM_REQUEST_ID_GEN.nextLong()));
    MailboxReceiveOperator mailboxReceiveOperator = null;
    for (int stageId : queryPlan.getStageMetadataMap().keySet()) {
      if (queryPlan.getQueryStageMap().get(stageId) instanceof MailboxReceiveNode) {
        MailboxReceiveNode reduceNode = (MailboxReceiveNode) queryPlan.getQueryStageMap().get(stageId);
        mailboxReceiveOperator = QueryDispatcher.createReduceStageOperator(_mailboxService,
            queryPlan.getStageMetadataMap().get(reduceNode.getSenderStageId()).getServerInstances(),
            Long.parseLong(requestMetadataMap.get("REQUEST_ID")), reduceNode.getSenderStageId(),
            reduceNode.getDataSchema(), "localhost", _reducerGrpcPort);
      } else {
        for (ServerInstance serverInstance : queryPlan.getStageMetadataMap().get(stageId).getServerInstances()) {
          DistributedStagePlan distributedStagePlan =
              QueryDispatcher.constructDistributedStagePlan(queryPlan, stageId, serverInstance);
          EXECUTOR_SERVICE.submit(() -> {
            _servers.get(serverInstance).processQuery(distributedStagePlan, requestMetadataMap);
          });
        }
      }
    }
    Preconditions.checkNotNull(mailboxReceiveOperator);
    return QueryDispatcher.toResultTable(QueryDispatcher.reduceMailboxReceive(mailboxReceiveOperator),
        queryPlan.getQueryResultFields()).getRows();
  }

  private List<Object[]> queryH2(String sql)
      throws Exception {
    Statement h2statement = _h2Connection.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
    h2statement.execute(sql);
    ResultSet h2ResultSet = h2statement.getResultSet();
    int columnCount = h2ResultSet.getMetaData().getColumnCount();
    List<Object[]> result = new ArrayList<>();
    while (h2ResultSet.next()) {
      Object[] row = new Object[columnCount];
      for (int i = 0; i < columnCount; i++) {
        row[i] = h2ResultSet.getObject(i + 1);
      }
      result.add(row);
    }
    return result;
  }

  private void compareRowEquals(List<Object[]> resultRows, List<Object[]> expectedRows) {
    Assert.assertEquals(resultRows.size(), expectedRows.size());

    Comparator<Object> valueComp = (l, r) -> {
      if (l == null && r == null) {
        return 0;
      } else if (l == null) {
        return -1;
      } else if (r == null) {
        return 1;
      }
      if (l instanceof Integer) {
        return Integer.compare((Integer) l, ((Number) r).intValue());
      } else if (l instanceof Long) {
        return Long.compare((Long) l, ((Number) r).longValue());
      } else if (l instanceof Float) {
        return Float.compare((Float) l, ((Number) r).floatValue());
      } else if (l instanceof Double) {
        return Double.compare((Double) l, ((Number) r).doubleValue());
      } else if (l instanceof String) {
        return ((String) l).compareTo((String) r);
      } else if (l instanceof Boolean) {
        return ((Boolean) l).compareTo((Boolean) r);
      } else {
        throw new RuntimeException("non supported type " + l.getClass());
      }
    };
    Comparator<Object[]> rowComp = (l, r) -> {
      int cmp = 0;
      for (int i = 0; i < l.length; i++) {
        cmp = valueComp.compare(l[i], r[i]);
        if (cmp != 0) {
          return cmp;
        }
      }
      return 0;
    };
    resultRows.sort(rowComp);
    expectedRows.sort(rowComp);
    for (int i = 0; i < resultRows.size(); i++) {
      Object[] resultRow = resultRows.get(i);
      Object[] expectedRow = expectedRows.get(i);
      for (int j = 0; j < resultRow.length; j++) {
        Assert.assertEquals(valueComp.compare(resultRow[j], expectedRow[j]), 0,
            "Not match at (" + i + "," + j + ")! Expected: " + expectedRow[j] + " Actual: " + resultRow[j]);
      }
    }
  }

  @DataProvider(name = "testDataWithSqlToFinalRowCount")
  private Object[][] provideTestSqlAndRowCount() {
    return new Object[][] {
        // using join clause
        new Object[]{"SELECT * FROM a JOIN b USING (col1)", 15},

        // cannot compare with H2 w/o an ORDER BY because ordering is indeterminate
        new Object[]{"SELECT * FROM a LIMIT 2", 2},

        // test dateTrunc
        //   - on leaf stage
        new Object[]{"SELECT dateTrunc('DAY', ts) FROM a LIMIT 10", 10},
        //   - on intermediate stage
        new Object[]{"SELECT dateTrunc('DAY', round(a.ts, b.ts)) FROM a JOIN b "
            + "ON a.col1 = b.col1 AND a.col2 = b.col2", 15},

        // test regexpLike
        new Object[]{"SELECT a.col1, b.col1 FROM a JOIN b ON a.col3 = b.col3 WHERE regexpLike(a.col2, b.col1)", 9},
        new Object[]{"SELECT regexpLike(a.col1, b.col1) FROM a JOIN b ON a.col3 = b.col3", 39},
    };
  }
}
