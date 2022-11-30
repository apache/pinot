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
import org.apache.pinot.common.datatable.DataTableFactory;
import org.apache.pinot.core.common.datatable.DataTableBuilderFactory;
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
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


/**
 * all legacy tests.
 *
 * @deprecated do not add to this test set. this class will be broken down and clean up.
 * add your test to appropraite files in {@link org.apache.pinot.query.runtime.queries} instead.
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
    _mailboxService = new GrpcMailboxService(_reducerHostname, _reducerGrpcPort, new PinotConfiguration(reducerConfig));
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
        ImmutableMap.of(QueryConfig.KEY_OF_BROKER_REQUEST_ID, String.valueOf(RANDOM_REQUEST_ID_GEN.nextLong()),
            QueryConfig.KEY_OF_BROKER_REQUEST_TIMEOUT_MS,
            String.valueOf(CommonConstants.Broker.DEFAULT_BROKER_TIMEOUT_MS));
    MailboxReceiveOperator mailboxReceiveOperator = null;
    for (int stageId : queryPlan.getStageMetadataMap().keySet()) {
      if (queryPlan.getQueryStageMap().get(stageId) instanceof MailboxReceiveNode) {
        MailboxReceiveNode reduceNode = (MailboxReceiveNode) queryPlan.getQueryStageMap().get(stageId);
        mailboxReceiveOperator = QueryDispatcher.createReduceStageOperator(_mailboxService,
            queryPlan.getStageMetadataMap().get(reduceNode.getSenderStageId()).getServerInstances(),
            Long.parseLong(requestMetadataMap.get(QueryConfig.KEY_OF_BROKER_REQUEST_ID)),
            reduceNode.getSenderStageId(), reduceNode.getDataSchema(), "localhost", _reducerGrpcPort,
            Long.parseLong(requestMetadataMap.get(QueryConfig.KEY_OF_BROKER_REQUEST_TIMEOUT_MS)));
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
      QueryDispatcher.reduceMailboxReceive(mailboxReceiveOperator, CommonConstants.Broker.DEFAULT_BROKER_TIMEOUT_MS);
    } catch (RuntimeException rte) {
      Assert.assertTrue(rte.getMessage().contains("Received error query execution result block"));
      Assert.assertTrue(rte.getMessage().contains(exceptionMsg), "Exception should contain: " + exceptionMsg
          + "! but found: " + rte.getMessage());
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
        new Object[]{"SELECT dateTrunc('DAY', CAST(col3 AS BIGINT)) FROM a LIMIT 10", 10},
        //   - on intermediate stage
        new Object[]{"SELECT dateTrunc('DAY', round(a.ts, b.ts)) FROM a JOIN b "
            + "ON a.col1 = b.col1 AND a.col2 = b.col2", 15},
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
    };
  }

  @DataProvider(name = "testDataWithSqlExecutionExceptions")
  private Object[][] provideTestSqlWithExecutionException() {
    return new Object[][] {
        // Timeout exception should occur with this option:
        new Object[]{"SET timeoutMs = 1; SELECT * FROM a JOIN b ON a.col1 = b.col1 JOIN c ON a.col1 = c.col1",
            "timeout"},

        // Function with incorrect argument signature should throw runtime exception when casting string to numeric
        new Object[]{"SELECT least(a.col2, b.col3) FROM a JOIN b ON a.col1 = b.col1",
            "For input string:"},

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
        new Object[]{"SELECT CAST(jsonExtractScalar(col1, 'path', 'INT') AS INT) FROM a", "Illegal Json Path"},
        new Object[]{"SELECT CAST(json_extract_scalar(a.col1, b.col2, 'INT') AS INT) FROM a JOIN b ON a.col1 = b.col1",
            "PlaceholderScalarFunctions.jsonExtractScalar"},
    };
  }
}
