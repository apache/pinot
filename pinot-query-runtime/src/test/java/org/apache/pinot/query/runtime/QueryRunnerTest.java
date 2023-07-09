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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.pinot.core.common.datatable.DataTableBuilderFactory;
import org.apache.pinot.query.QueryEnvironmentTestBase;
import org.apache.pinot.query.QueryServerEnclosure;
import org.apache.pinot.query.mailbox.MailboxService;
import org.apache.pinot.query.planner.DispatchableSubPlan;
import org.apache.pinot.query.planner.plannode.MailboxReceiveNode;
import org.apache.pinot.query.routing.QueryServerInstance;
import org.apache.pinot.query.runtime.executor.OpChainSchedulerService;
import org.apache.pinot.query.runtime.executor.RoundRobinScheduler;
import org.apache.pinot.query.service.QueryConfig;
import org.apache.pinot.query.service.dispatch.QueryDispatcher;
import org.apache.pinot.query.testutils.MockInstanceDataManagerFactory;
import org.apache.pinot.query.testutils.QueryTestUtils;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


/**
 * all special tests that doesn't fit into {@link org.apache.pinot.query.runtime.queries.ResourceBasedQueriesTest}
 * pattern goes here.
 */
public class QueryRunnerTest extends QueryRunnerTestBase {
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
    MockInstanceDataManagerFactory factory2 = new MockInstanceDataManagerFactory("server2")
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
    Schema schema = SCHEMA_BUILDER.build();
    for (String tableName : Arrays.asList("a", "b", "c", "d")) {
      addTableToH2(tableName, schema);
      addDataToH2(tableName, schema, factory1.buildTableRowsMap().get(tableName));
      addDataToH2(tableName, schema, factory2.buildTableRowsMap().get(tableName));
    }

    _reducerGrpcPort = QueryTestUtils.getAvailablePort();
    _reducerHostname = String.format("Broker_%s", QueryConfig.DEFAULT_QUERY_RUNNER_HOSTNAME);
    Map<String, Object> reducerConfig = new HashMap<>();
    reducerConfig.put(QueryConfig.KEY_OF_QUERY_RUNNER_PORT, _reducerGrpcPort);
    reducerConfig.put(QueryConfig.KEY_OF_QUERY_RUNNER_HOSTNAME, _reducerHostname);
    _reducerScheduler = new OpChainSchedulerService(new RoundRobinScheduler(10_000L), REDUCE_EXECUTOR);
    _mailboxService = new MailboxService(QueryConfig.DEFAULT_QUERY_RUNNER_HOSTNAME, _reducerGrpcPort,
        new PinotConfiguration(reducerConfig), _reducerScheduler::onDataAvailable);
    _reducerScheduler.startAsync();
    _mailboxService.start();

    _queryEnvironment = QueryEnvironmentTestBase.getQueryEnvironment(_reducerGrpcPort, server1.getPort(),
        server2.getPort(), factory1.buildSchemaMap(), factory1.buildTableSegmentNameMap(),
        factory2.buildTableSegmentNameMap());
    server1.start();
    server2.start();
    // this doesn't test the QueryServer functionality so the server port can be the same as the mailbox port.
    // this is only use for test identifier purpose.
    int port1 = server1.getPort();
    int port2 = server2.getPort();
    _servers.put(new QueryServerInstance("localhost", port1, port1), server1);
    _servers.put(new QueryServerInstance("localhost", port2, port2), server2);
  }

  @AfterClass
  public void tearDown() {
    DataTableBuilderFactory.setDataTableVersion(DataTableBuilderFactory.DEFAULT_VERSION);
    for (QueryServerEnclosure server : _servers.values()) {
      server.shutDown();
    }
    _mailboxService.shutdown();
    _reducerScheduler.stopAsync();
  }

  /**
   * Test compares with expected row count only.
   */
  @Test(dataProvider = "testDataWithSqlToFinalRowCount")
  public void testSqlWithFinalRowCountChecker(String sql, int expectedRows)
      throws Exception {
    List<Object[]> resultRows = queryRunner(sql, null);
    Assert.assertEquals(resultRows.size(), expectedRows);
  }

  /**
   * Test automatically compares against H2.
   *
   * @deprecated do not add to this test set. this class will be broken down and clean up.
   *   add your test to the appropriate files in {@link org.apache.pinot.query.runtime.queries} instead.
   */
  @Test(dataProvider = "testSql")
  public void testSqlWithH2Checker(String sql)
      throws Exception {
    List<Object[]> resultRows = queryRunner(sql, null);
    // query H2 for data
    List<Object[]> expectedRows = queryH2(sql);
    compareRowEquals(resultRows, expectedRows);
  }

  /**
   * Test compares against its desired exceptions.
   */
  @Test(dataProvider = "testDataWithSqlExecutionExceptions")
  public void testSqlWithExceptionMsgChecker(String sql, String exceptionMsg) {
    long requestId = RANDOM_REQUEST_ID_GEN.nextLong();
    DispatchableSubPlan dispatchableSubPlan = _queryEnvironment.planQuery(sql);
    Map<String, String> requestMetadataMap =
        ImmutableMap.of(QueryConfig.KEY_OF_BROKER_REQUEST_ID, String.valueOf(requestId),
            QueryConfig.KEY_OF_BROKER_REQUEST_TIMEOUT_MS,
            String.valueOf(CommonConstants.Broker.DEFAULT_BROKER_TIMEOUT_MS));
    int reducerStageId = -1;
    for (int stageId = 0; stageId < dispatchableSubPlan.getQueryStageList().size(); stageId++) {
      if (dispatchableSubPlan.getQueryStageList().get(stageId).getPlanFragment()
          .getFragmentRoot() instanceof MailboxReceiveNode) {
        reducerStageId = stageId;
      } else {
        processDistributedStagePlans(dispatchableSubPlan, stageId, requestMetadataMap);
      }
    }
    Preconditions.checkState(reducerStageId != -1);

    try {
      QueryDispatcher.runReducer(requestId, dispatchableSubPlan, reducerStageId,
          Long.parseLong(requestMetadataMap.get(QueryConfig.KEY_OF_BROKER_REQUEST_TIMEOUT_MS)), _mailboxService,
          _reducerScheduler, null, false);
    } catch (RuntimeException rte) {
      Assert.assertTrue(rte.getMessage().contains("Received error query execution result block"));
      Assert.assertTrue(rte.getMessage().contains(exceptionMsg), "Exception should contain: " + exceptionMsg
          + "! but found: " + rte.getMessage());
    }
  }

  @DataProvider(name = "testDataWithSqlToFinalRowCount")
  private Object[][] provideTestSqlAndRowCount() {
    return new Object[][]{
        new Object[]{"SELECT /*+ joinOptions(join_strategy='dynamic_broadcast') */ col1 FROM a "
            + " WHERE a.col1 IN (SELECT b.col2 FROM b WHERE b.col3 < 10) AND a.col3 > 0", 9},

        // using join clause
        new Object[]{"SELECT * FROM a JOIN b USING (col1)", 15},

        // cannot compare with H2 w/o an ORDER BY because ordering is indeterminate
        new Object[]{"SELECT * FROM a LIMIT 2", 2},

        // test dateTrunc
        //   - on leaf stage
        new Object[]{"SELECT dateTrunc('DAY', ts) FROM a LIMIT 10", 10},
        new Object[]{"SELECT dateTrunc('DAY', CAST(col3 AS BIGINT)) FROM a LIMIT 10", 10},
        //   - on intermediate stage
        new Object[]{
            "SELECT dateTrunc('DAY', round(a.ts, b.ts)) FROM a JOIN b "
                + "ON a.col1 = b.col1 AND a.col2 = b.col2", 15
        },
        new Object[]{"SELECT dateTrunc('DAY', CAST(MAX(a.col3) AS BIGINT)) FROM a", 1},

        // ScalarFunction
        // test function can be used in predicate/leaf/intermediate stage (using regexpLike)
        new Object[]{"SELECT a.col1, b.col1 FROM a JOIN b ON a.col3 = b.col3 WHERE regexpLike(a.col2, b.col1)", 9},
        new Object[]{"SELECT a.col1, b.col1 FROM a JOIN b ON a.col3 = b.col3 WHERE regexp_like(a.col2, b.col1)", 9},
        new Object[]{"SELECT regexpLike(a.col1, b.col1) FROM a JOIN b ON a.col3 = b.col3", 39},
        new Object[]{"SELECT regexp_like(a.col1, b.col1) FROM a JOIN b ON a.col3 = b.col3", 39},

        // test function with @ScalarFunction annotation and alias works (using round_decimal)
        new Object[]{"SELECT roundDecimal(col3) FROM a", 15},
        new Object[]{"SELECT round_decimal(col3) FROM a", 15},
        new Object[]{"SELECT col1, roundDecimal(COUNT(*)) FROM a GROUP BY col1", 5},
        new Object[]{"SELECT col1, round_decimal(COUNT(*)) FROM a GROUP BY col1", 5},

        // test queries with special query options attached
        //   - when leaf limit is set, each server returns multiStageLeafLimit number of rows only.
        new Object[]{"SET multiStageLeafLimit = 1; SELECT * FROM a", 2},
    };
  }

  @DataProvider(name = "testDataWithSqlExecutionExceptions")
  private Object[][] provideTestSqlWithExecutionException() {
    return new Object[][]{
        // query hint with dynamic broadcast pipeline breaker should return error upstream
        new Object[]{
            "SELECT /*+ joinOptions(join_strategy='dynamic_broadcast') */ col1 FROM a "
                + " WHERE a.col1 IN (SELECT b.col2 FROM b WHERE textMatch(col1, 'f')) AND a.col3 > 0",
            "without text index"
        },
        // Timeout exception should occur with this option:
        new Object[]{
            "SET timeoutMs = 1; SELECT * FROM a JOIN b ON a.col1 = b.col1 JOIN c ON a.col1 = c.col1",
            "timeout"
        },

        // Function with incorrect argument signature should throw runtime exception when casting string to numeric
        new Object[]{
            "SELECT least(a.col2, b.col3) FROM a JOIN b ON a.col1 = b.col1",
            "For input string:"
        },

        // Scalar function that doesn't have a valid use should throw an exception on the leaf stage
        //   - predicate only functions:
        new Object[]{"SELECT * FROM a WHERE textMatch(col1, 'f')", "without text index"},
        new Object[]{"SELECT * FROM a WHERE text_match(col1, 'f')", "without text index"},
        new Object[]{"SELECT * FROM a WHERE textContains(col1, 'f')", "supported only on native text index"},
        new Object[]{"SELECT * FROM a WHERE text_contains(col1, 'f')", "supported only on native text index"},

        //  - transform only functions
        new Object[]{"SELECT jsonExtractKey(col1, 'path') FROM a", "was expecting (JSON String"},
        new Object[]{"SELECT json_extract_key(col1, 'path') FROM a", "was expecting (JSON String"},

        //  - PlaceholderScalarFunction registered will throw on intermediate stage, but works on leaf stage.
        //    - checked "Illegal Json Path" as col1 is not actually a json string, but the call is correctly triggered.
        new Object[]{"SELECT CAST(jsonExtractScalar(col1, 'path', 'INT') AS INT) FROM a", "Illegal Json Path"},
        //    - checked function cannot be found b/c there's no intermediate stage impl for json_extract_scalar
        // TODO: re-enable this test once we have implemented constructor time error pipe back.
        // new Object[]{"SELECT CAST(json_extract_scalar(a.col1, b.col2, 'INT') AS INT)"
        //     + "FROM a JOIN b ON a.col1 = b.col1", "Cannot find function with Name: json_extract_scalar"},
    };
  }
}
