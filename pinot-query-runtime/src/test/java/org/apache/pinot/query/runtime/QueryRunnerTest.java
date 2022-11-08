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
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.pinot.common.datatable.DataTableFactory;
import org.apache.pinot.common.utils.NamedThreadFactory;
import org.apache.pinot.core.common.datatable.DataTableBuilderFactory;
import org.apache.pinot.core.query.scheduler.resources.ResourceManager;
import org.apache.pinot.core.transport.ServerInstance;
import org.apache.pinot.query.QueryEnvironmentTestBase;
import org.apache.pinot.query.QueryServerEnclosure;
import org.apache.pinot.query.mailbox.GrpcMailboxService;
import org.apache.pinot.query.planner.QueryPlan;
import org.apache.pinot.query.planner.stage.MailboxReceiveNode;
import org.apache.pinot.query.routing.WorkerInstance;
import org.apache.pinot.query.runtime.operator.MailboxReceiveOperator;
import org.apache.pinot.query.runtime.plan.DistributedStagePlan;
import org.apache.pinot.query.service.QueryConfig;
import org.apache.pinot.query.service.QueryDispatcher;
import org.apache.pinot.query.testutils.MockInstanceDataManagerFactory;
import org.apache.pinot.query.testutils.QueryTestUtils;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


public class QueryRunnerTest extends QueryRunnerTestBase {
  private static final ExecutorService EXECUTOR_SERVICE = Executors.newFixedThreadPool(
      ResourceManager.DEFAULT_QUERY_WORKER_THREADS, new NamedThreadFactory("query_server_enclosure"));
  public static final Object[][] ROWS = new Object[][]{
      new Object[]{"foo", "foo", 1},
      new Object[]{"bar", "bar", 42},
      new Object[]{"alice", "alice", 1},
      new Object[]{"bob", "foo", 42},
      new Object[]{"charlie", "bar", 1},
  };
  public static final Schema.SchemaBuilder SCHEMA_BUILDER;
  static {
    SCHEMA_BUILDER = new Schema.SchemaBuilder()
        .addSingleValueDimension("col1", FieldSpec.DataType.STRING, "")
        .addSingleValueDimension("col2", FieldSpec.DataType.STRING, "")
        .addDateTime("ts", FieldSpec.DataType.LONG, "1:MILLISECONDS:EPOCH", "1:HOURS")
        .addMetric("col3", FieldSpec.DataType.INT, 0)
        .setSchemaName("defaultSchemaName");
  }

  public static List<GenericRow> buildRows(String tableName) {
    List<GenericRow> rows = new ArrayList<>(ROWS.length);
    for (int i = 0; i < ROWS.length; i++) {
      GenericRow row = new GenericRow();
      row.putValue("col1", ROWS[i][0]);
      row.putValue("col2", ROWS[i][1]);
      row.putValue("col3", ROWS[i][2]);
      row.putValue("ts", TableType.OFFLINE.equals(TableNameBuilder.getTableTypeFromTableName(tableName))
          ? System.currentTimeMillis() - TimeUnit.DAYS.toMillis(2) : System.currentTimeMillis());
      rows.add(row);
    }
    return rows;
  }

  @BeforeClass
  public void setUp()
      throws Exception {
    DataTableBuilderFactory.setDataTableVersion(DataTableFactory.VERSION_4);
    MockInstanceDataManagerFactory factory1 = new MockInstanceDataManagerFactory("server1")
        .registerTable(SCHEMA_BUILDER.setSchemaName("a").build(), "a_REALTIME")
        .registerTable(SCHEMA_BUILDER.setSchemaName("b").build(), "b_REALTIME")
        .registerTable(SCHEMA_BUILDER.setSchemaName("c").build(), "c_OFFLINE")
        .registerTable(SCHEMA_BUILDER.setSchemaName("d").build(), "d")
        .addSegment("a_REALTIME", buildRows("a_REALTIME"))
        .addSegment("a_REALTIME", buildRows("a_REALTIME"))
        .addSegment("b_REALTIME", buildRows("b_REALTIME"))
        .addSegment("c_OFFLINE", buildRows("c_OFFLINE"))
        .addSegment("d_OFFLINE", buildRows("d_OFFLINE"));
    MockInstanceDataManagerFactory factory2 = new MockInstanceDataManagerFactory("server1")
        .registerTable(SCHEMA_BUILDER.setSchemaName("a").build(), "a_REALTIME")
        .registerTable(SCHEMA_BUILDER.setSchemaName("c").build(), "c_OFFLINE")
        .registerTable(SCHEMA_BUILDER.setSchemaName("d").build(), "d")
        .addSegment("a_REALTIME", buildRows("a_REALTIME"))
        .addSegment("c_OFFLINE", buildRows("c_OFFLINE"))
        .addSegment("c_OFFLINE", buildRows("c_OFFLINE"))
        .addSegment("d_OFFLINE", buildRows("d_OFFLINE"))
        .addSegment("d_REALTIME", buildRows("d_REALTIME"));
    QueryServerEnclosure server1 = new QueryServerEnclosure(factory1);
    QueryServerEnclosure server2 = new QueryServerEnclosure(factory2);

    // Setting up H2 for validation
    setH2Connection();
    addTableToH2(SCHEMA_BUILDER.build(), Arrays.asList("a", "b", "c", "d"));
    addDataToH2(SCHEMA_BUILDER.build(), factory1.buildTableRowsMap());
    addDataToH2(SCHEMA_BUILDER.build(), factory2.buildTableRowsMap());

    _reducerGrpcPort = QueryTestUtils.getAvailablePort();
    _reducerHostname = String.format("Broker_%s", QueryConfig.DEFAULT_QUERY_RUNNER_HOSTNAME);
    Map<String, Object> reducerConfig = new HashMap<>();
    reducerConfig.put(QueryConfig.KEY_OF_QUERY_RUNNER_PORT, _reducerGrpcPort);
    reducerConfig.put(QueryConfig.KEY_OF_QUERY_RUNNER_HOSTNAME, _reducerHostname);
    _mailboxService = new GrpcMailboxService(_reducerHostname, _reducerGrpcPort, new PinotConfiguration(reducerConfig));
    _mailboxService.start();

    _queryEnvironment = QueryEnvironmentTestBase.getQueryEnvironment(_reducerGrpcPort, server1.getPort(),
        server2.getPort(), factory1.buildTableSegmentNameMap(), factory2.buildTableSegmentNameMap());
    server1.start();
    server2.start();
    // this doesn't test the QueryServer functionality so the server port can be the same as the mailbox port.
    // this is only use for test identifier purpose.
    int port1 = server1.getPort();
    int port2 = server2.getPort();
    _servers.put(new WorkerInstance("localhost", port1, port1, port1, port1), server1);
    _servers.put(new WorkerInstance("localhost", port2, port2, port2, port2), server2);
  }

  @AfterClass
  public void tearDown() {
    DataTableBuilderFactory.setDataTableVersion(DataTableBuilderFactory.DEFAULT_VERSION);
    for (QueryServerEnclosure server : _servers.values()) {
      server.shutDown();
    }
    _mailboxService.shutdown();
  }

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

  @Test(dataProvider = "testDataWithSqlExecutionExceptions")
  public void testSqlWithExceptionMsgChecker(String sql, String exceptionMsg) {
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
          _servers.get(serverInstance).processQuery(distributedStagePlan, requestMetadataMap);
        }
      }
    }
    Preconditions.checkNotNull(mailboxReceiveOperator);

    try {
      QueryDispatcher.reduceMailboxReceive(mailboxReceiveOperator);
    } catch (RuntimeException rte) {
      Assert.assertTrue(rte.getMessage().contains("Received error query execution result block"));
      Assert.assertTrue(rte.getMessage().contains(exceptionMsg));
    }
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
        queryPlan.getQueryResultFields(), queryPlan.getQueryStageMap().get(0).getDataSchema()).getRows();
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

  @DataProvider(name = "testDataWithSqlExecutionExceptions")
  private Object[][] provideTestSqlWithExecutionException() {
    return new Object[][] {
        // Function with incorrect argument signature should throw runtime exception
        new Object[]{"SELECT least(a.col2, b.col3) FROM a JOIN b ON a.col1 = b.col1",
            "ArithmeticFunctions.least(double,double) with arguments"},
        // TODO: this error is thrown but not returned through mailbox. need another test for asserting failure
        // during stage runtime init.
        // standard SqlOpTable function that runs out of signature list in actual impl throws not found exception
        // new Object[]{"SELECT CASE WHEN col3 > 10 THEN 1 WHEN col3 > 20 THEN 2 WHEN col3 > 30 THEN 3 "
        //    + "WHEN col3 > 40 THEN 4 WHEN col3 > 50 THEN 5 WHEN col3 > 60 THEN '6' ELSE 0 END FROM a", "caseWhen"},
    };
  }
}
