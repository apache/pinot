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
import com.google.common.collect.Lists;
import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.common.utils.DataTable;
import org.apache.pinot.core.common.datatable.DataTableFactory;
import org.apache.pinot.core.transport.ServerInstance;
import org.apache.pinot.query.QueryEnvironment;
import org.apache.pinot.query.QueryEnvironmentTestUtils;
import org.apache.pinot.query.QueryServerEnclosure;
import org.apache.pinot.query.mailbox.GrpcMailboxService;
import org.apache.pinot.query.planner.QueryPlan;
import org.apache.pinot.query.planner.stage.MailboxReceiveNode;
import org.apache.pinot.query.routing.WorkerInstance;
import org.apache.pinot.query.runtime.operator.MailboxReceiveOperator;
import org.apache.pinot.query.runtime.plan.DistributedStagePlan;
import org.apache.pinot.query.service.QueryConfig;
import org.apache.pinot.query.service.QueryDispatcher;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.apache.pinot.core.query.selection.SelectionOperatorUtils.extractRowFromDataTable;


public class QueryRunnerTest {
  private static final Random RANDOM_REQUEST_ID_GEN = new Random();
  private static final File INDEX_DIR_S1_A = new File(FileUtils.getTempDirectory(), "QueryRunnerTest_server1_tableA");
  private static final File INDEX_DIR_S1_B = new File(FileUtils.getTempDirectory(), "QueryRunnerTest_server1_tableB");
  private static final File INDEX_DIR_S2_A = new File(FileUtils.getTempDirectory(), "QueryRunnerTest_server2_tableA");
  private static final File INDEX_DIR_S1_C = new File(FileUtils.getTempDirectory(), "QueryRunnerTest_server1_tableC");
  private static final File INDEX_DIR_S2_C = new File(FileUtils.getTempDirectory(), "QueryRunnerTest_server2_tableC");

  private QueryEnvironment _queryEnvironment;
  private String _reducerHostname;
  private int _reducerGrpcPort;
  private Map<ServerInstance, QueryServerEnclosure> _servers = new HashMap<>();
  private GrpcMailboxService _mailboxService;

  @BeforeClass
  public void setUp()
      throws Exception {
    DataTableFactory.setDataTableVersion(DataTableFactory.VERSION_4);
    QueryServerEnclosure server1 = new QueryServerEnclosure(Lists.newArrayList("a", "b", "c"),
        ImmutableMap.of("a", INDEX_DIR_S1_A, "b", INDEX_DIR_S1_B, "c", INDEX_DIR_S1_C),
        QueryEnvironmentTestUtils.SERVER1_SEGMENTS);
    QueryServerEnclosure server2 = new QueryServerEnclosure(Lists.newArrayList("a", "c"),
        ImmutableMap.of("a", INDEX_DIR_S2_A, "c", INDEX_DIR_S2_C), QueryEnvironmentTestUtils.SERVER2_SEGMENTS);

    _reducerGrpcPort = QueryEnvironmentTestUtils.getAvailablePort();
    _reducerHostname = String.format("Broker_%s", QueryConfig.DEFAULT_QUERY_RUNNER_HOSTNAME);
    Map<String, Object> reducerConfig = new HashMap<>();
    reducerConfig.put(QueryConfig.KEY_OF_QUERY_RUNNER_PORT, _reducerGrpcPort);
    reducerConfig.put(QueryConfig.KEY_OF_QUERY_RUNNER_HOSTNAME, _reducerHostname);
    _mailboxService = new GrpcMailboxService(_reducerHostname, _reducerGrpcPort);
    _mailboxService.start();

    _queryEnvironment = QueryEnvironmentTestUtils.getQueryEnvironment(_reducerGrpcPort, server1.getPort(),
        server2.getPort());
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
    DataTableFactory.setDataTableVersion(DataTableFactory.DEFAULT_VERSION);
    for (QueryServerEnclosure server : _servers.values()) {
      server.shutDown();
    }
    _mailboxService.shutdown();
  }

  @Test(dataProvider = "testDataWithSqlToFinalRowCount")
  public void testSqlWithFinalRowCountChecker(String sql, int expectedRowCount) {
    QueryPlan queryPlan = _queryEnvironment.planQuery(sql);
    Map<String, String> requestMetadataMap =
        ImmutableMap.of("REQUEST_ID", String.valueOf(RANDOM_REQUEST_ID_GEN.nextLong()));
    MailboxReceiveOperator mailboxReceiveOperator = null;
    for (int stageId : queryPlan.getStageMetadataMap().keySet()) {
      if (queryPlan.getQueryStageMap().get(stageId) instanceof MailboxReceiveNode) {
        MailboxReceiveNode reduceNode = (MailboxReceiveNode) queryPlan.getQueryStageMap().get(stageId);
        mailboxReceiveOperator = QueryDispatcher.createReduceStageOperator(_mailboxService,
            queryPlan.getStageMetadataMap().get(reduceNode.getSenderStageId()).getServerInstances(),
            Long.parseLong(requestMetadataMap.get("REQUEST_ID")), reduceNode.getSenderStageId(), "localhost",
            _reducerGrpcPort);
      } else {
        for (ServerInstance serverInstance : queryPlan.getStageMetadataMap().get(stageId).getServerInstances()) {
          DistributedStagePlan distributedStagePlan =
              QueryDispatcher.constructDistributedStagePlan(queryPlan, stageId, serverInstance);
          _servers.get(serverInstance).processQuery(distributedStagePlan, requestMetadataMap);
        }
      }
    }
    Preconditions.checkNotNull(mailboxReceiveOperator);

    List<Object[]> resultRows = toRows(QueryDispatcher.reduceMailboxReceive(mailboxReceiveOperator));
    Assert.assertEquals(resultRows.size(), expectedRowCount);
  }

  private static List<Object[]> toRows(List<DataTable> dataTables) {
    List<Object[]> resultRows = new ArrayList<>();
    for (DataTable dataTable : dataTables) {
      int numRows = dataTable.getNumberOfRows();
      for (int rowId = 0; rowId < numRows; rowId++) {
        resultRows.add(extractRowFromDataTable(dataTable, rowId));
      }
    }
    return resultRows;
  }

  @DataProvider(name = "testDataWithSqlToFinalRowCount")
  private Object[][] provideTestSqlAndRowCount() {
    return new Object[][] {
        new Object[]{"SELECT * FROM b", 5},
        new Object[]{"SELECT * FROM a", 15},

        // Specifically table A has 15 rows (10 on server1 and 5 on server2) and table B has 5 rows (all on server1),
        // thus the final JOIN result will be 15 x 1 = 15.
        // Next join with table C which has (5 on server1 and 10 on server2), since data is identical. each of the row
        // of the A JOIN B will have identical value of col3 as table C.col3 has. Since the values are cycling between
        // (1, 2, 42, 1, 2). we will have 6 1s, 6 2s, and 3 42s, total result count will be 36 + 36 + 9 = 81
        new Object[]{"SELECT * FROM a JOIN b ON a.col1 = b.col1 JOIN c ON a.col3 = c.col3", 81},

        // Specifically table A has 15 rows (10 on server1 and 5 on server2) and table B has 5 rows (all on server1),
        // thus the final JOIN result will be 15 x 1 = 15.
        new Object[]{"SELECT * FROM a JOIN b on a.col1 = b.col1", 15},

        // Query with function in JOIN keys, table A and B are both (1, 2, 42, 1, 2), with table A cycling 3 times.
        // Final result would have 6 x 2 = 12 (6 (1)s on with MOD result 1, on both tables)
        //     + 9 x 1 = 9 (6 (2)s & 3 (42)s on table A MOD 2 = 0, 1 (42)s on table B MOD 3 = 0): 21 rows in total.
        new Object[]{"SELECT a.col1, a.col3, b.col3 FROM a JOIN b ON MOD(a.col3, 2) = MOD(b.col3, 3)", 21},

        // Specifically table A has 15 rows (10 on server1 and 5 on server2) and table B has 5 rows (all on server1),
        // thus the final JOIN result will be 15 x 1 = 15.
        new Object[]{"SELECT * FROM a JOIN b on a.col1 = b.col1 AND a.col2 = b.col2", 15},

        // Specifically table A has 15 rows (10 on server1 and 5 on server2) and table B has 5 rows (all on server1),
        // but only 1 out of 5 rows from table A will be selected out; and all in table B will be selected.
        // thus the final JOIN result will be 1 x 3 x 1 = 3.
        new Object[]{"SELECT a.col1, a.ts, b.col2, b.col3 FROM a JOIN b ON a.col1 = b.col2 "
            + " WHERE a.col3 >= 0 AND a.col2 = 'alice' AND b.col3 >= 0", 3},

        // Projection pushdown
        new Object[]{"SELECT a.col1, a.col3 + a.col3 FROM a WHERE a.col3 >= 0 AND a.col2 = 'alice'", 3},

        // Aggregation with group by
        new Object[]{"SELECT a.col1, SUM(a.col3) FROM a WHERE a.col3 >= 0 GROUP BY a.col1", 5},

        // Aggregation with multiple group key
        new Object[]{"SELECT a.col2, a.col1, SUM(a.col3) FROM a WHERE a.col3 >= 0 GROUP BY a.col1, a.col2", 5},

        // Aggregation without GROUP BY
        new Object[]{"SELECT COUNT(*) FROM a WHERE a.col3 >= 0 AND a.col2 = 'alice'", 1},

        // project in intermediate stage
        // Specifically table A has 15 rows (10 on server1 and 5 on server2) and table B has 5 rows (all on server1),
        // col1 on both are "foo", "bar", "alice", "bob", "charlie"
        // col2 on both are "foo", "bar", "alice", "foo", "bar",
        //   filtered at :    ^                      ^
        // thus the final JOIN result will have 6 rows: 3 "foo" <-> "foo"; and 3 "bob" <-> "bob"
        new Object[]{"SELECT a.col1, a.col2, a.ts, b.col1, b.col3 FROM a JOIN b ON a.col1 = b.col2 "
            + " WHERE a.col3 >= 0 AND a.col2 = 'foo' AND b.col3 >= 0", 6},

        // Making transform after JOIN, number of rows should be the same as JOIN result.
        new Object[]{"SELECT a.col1, a.ts, a.col3 - b.col3 FROM a JOIN b ON a.col1 = b.col2 "
            + " WHERE a.col3 >= 0 AND b.col3 >= 0", 15},

        // Making transform after GROUP-BY, number of rows should be the same as GROUP-BY result.
        new Object[]{"SELECT a.col1, a.col2, SUM(a.col3) - MIN(a.col3) FROM a"
            + " WHERE a.col3 >= 0 GROUP BY a.col1, a.col2", 5},

        // GROUP BY after JOIN
        // only 3 GROUP BY key exist because b.col2 cycles between "foo", "bar", "alice".
        new Object[]{"SELECT a.col1, SUM(b.col3) FROM a JOIN b ON a.col1 = b.col2 "
            + " WHERE a.col3 >= 0 GROUP BY a.col1", 3},

        // Sub-query
        new Object[]{"SELECT b.col1, b.col3, i.maxVal FROM b JOIN "
            + "  (SELECT a.col2 AS joinKey, MAX(a.col3) AS maxVal FROM a GROUP BY a.col2) AS i "
            + "  ON b.col1 = i.joinKey", 3}
    };
  }
}
