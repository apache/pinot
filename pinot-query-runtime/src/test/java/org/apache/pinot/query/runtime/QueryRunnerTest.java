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
import org.apache.calcite.rel.RelDistribution;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.common.utils.DataTable;
import org.apache.pinot.core.transport.ServerInstance;
import org.apache.pinot.query.QueryEnvironment;
import org.apache.pinot.query.QueryEnvironmentTestUtils;
import org.apache.pinot.query.QueryServerEnclosure;
import org.apache.pinot.query.mailbox.GrpcMailboxService;
import org.apache.pinot.query.planner.QueryPlan;
import org.apache.pinot.query.planner.nodes.MailboxReceiveNode;
import org.apache.pinot.query.routing.WorkerInstance;
import org.apache.pinot.query.runtime.blocks.DataTableBlock;
import org.apache.pinot.query.runtime.blocks.DataTableBlockUtils;
import org.apache.pinot.query.runtime.operator.MailboxReceiveOperator;
import org.apache.pinot.query.runtime.plan.DistributedStagePlan;
import org.apache.pinot.query.service.QueryConfig;
import org.apache.pinot.query.service.QueryDispatcher;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
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

    _queryEnvironment =
        QueryEnvironmentTestUtils.getQueryEnvironment(_reducerGrpcPort, server1.getPort(), server2.getPort());
    server1.start();
    server2.start();
    // this doesn't test the QueryServer functionality so the server port can be the same as the mailbox port.
    // this is only use for test identifier purpose.
    _servers.put(new WorkerInstance("localhost", server1.getPort(), server1.getPort()), server1);
    _servers.put(new WorkerInstance("localhost", server2.getPort(), server2.getPort()), server2);
  }

  @AfterClass
  public void tearDown() {
    for (QueryServerEnclosure server : _servers.values()) {
      server.shutDown();
    }
    _mailboxService.shutdown();
  }

  @Test
  public void testRunningTableScanOnlyQuery()
      throws Exception {
    QueryPlan queryPlan = _queryEnvironment.planQuery("SELECT * FROM b");
    int stageRoodId = QueryEnvironmentTestUtils.getTestStageByServerCount(queryPlan, 1);
    Map<String, String> requestMetadataMap =
        ImmutableMap.of("REQUEST_ID", String.valueOf(RANDOM_REQUEST_ID_GEN.nextLong()));

    ServerInstance serverInstance = queryPlan.getStageMetadataMap().get(stageRoodId).getServerInstances().get(0);
    DistributedStagePlan distributedStagePlan =
        QueryDispatcher.constructDistributedStagePlan(queryPlan, stageRoodId, serverInstance);

    MailboxReceiveOperator mailboxReceiveOperator =
        createReduceStageOperator(queryPlan.getStageMetadataMap().get(stageRoodId).getServerInstances(),
            Long.parseLong(requestMetadataMap.get("REQUEST_ID")), stageRoodId, _reducerGrpcPort);

    // execute this single stage.
    _servers.get(serverInstance).processQuery(distributedStagePlan, requestMetadataMap);

    // get the block back and it should have 5 rows
    List<Object[]> resultRows = reduceMailboxReceive(mailboxReceiveOperator);
    Assert.assertEquals(resultRows.size(), 5);
  }

  @Test
  public void testRunningTableScanMultipleServer()
      throws Exception {
    QueryPlan queryPlan = _queryEnvironment.planQuery("SELECT * FROM a");
    int stageRoodId = QueryEnvironmentTestUtils.getTestStageByServerCount(queryPlan, 2);
    Map<String, String> requestMetadataMap =
        ImmutableMap.of("REQUEST_ID", String.valueOf(RANDOM_REQUEST_ID_GEN.nextLong()));

    for (ServerInstance serverInstance : queryPlan.getStageMetadataMap().get(stageRoodId).getServerInstances()) {
      DistributedStagePlan distributedStagePlan =
          QueryDispatcher.constructDistributedStagePlan(queryPlan, stageRoodId, serverInstance);

      // execute this single stage.
      _servers.get(serverInstance).processQuery(distributedStagePlan, requestMetadataMap);
    }

    MailboxReceiveOperator mailboxReceiveOperator =
        createReduceStageOperator(queryPlan.getStageMetadataMap().get(stageRoodId).getServerInstances(),
            Long.parseLong(requestMetadataMap.get("REQUEST_ID")), stageRoodId, _reducerGrpcPort);

    // assert that all table A segments returned successfully.
    List<Object[]> resultRows = reduceMailboxReceive(mailboxReceiveOperator);
    Assert.assertEquals(resultRows.size(), 15);
  }

  @Test
  public void testJoin()
      throws Exception {
    QueryPlan queryPlan = _queryEnvironment.planQuery("SELECT * FROM a JOIN b on a.col1 = b.col2");
    Map<String, String> requestMetadataMap =
        ImmutableMap.of("REQUEST_ID", String.valueOf(RANDOM_REQUEST_ID_GEN.nextLong()));
    MailboxReceiveOperator mailboxReceiveOperator = null;
    for (int stageId : queryPlan.getStageMetadataMap().keySet()) {
      if (queryPlan.getQueryStageMap().get(stageId) instanceof MailboxReceiveNode) {
        MailboxReceiveNode reduceNode = (MailboxReceiveNode) queryPlan.getQueryStageMap().get(stageId);
        mailboxReceiveOperator = createReduceStageOperator(
            queryPlan.getStageMetadataMap().get(reduceNode.getSenderStageId()).getServerInstances(),
            Long.parseLong(requestMetadataMap.get("REQUEST_ID")), reduceNode.getSenderStageId(), _reducerGrpcPort);
      } else {
        for (ServerInstance serverInstance : queryPlan.getStageMetadataMap().get(stageId).getServerInstances()) {
          DistributedStagePlan distributedStagePlan =
              QueryDispatcher.constructDistributedStagePlan(queryPlan, stageId, serverInstance);
          _servers.get(serverInstance).processQuery(distributedStagePlan, requestMetadataMap);
        }
      }
    }
    Preconditions.checkNotNull(mailboxReceiveOperator);

    // Assert that each of the 5 categories from left table is joined with right table.
    // Specifically table A has 15 rows (10 on server1 and 5 on server2) and table B has 5 rows (all on server1),
    // thus the final JOIN result will be 15 x 1 = 15.
    List<Object[]> resultRows = reduceMailboxReceive(mailboxReceiveOperator);
    Assert.assertEquals(resultRows.size(), 15);
  }

  @Test
  public void testMultipleJoin()
      throws Exception {
    QueryPlan queryPlan =
        _queryEnvironment.planQuery("SELECT * FROM a JOIN b ON a.col1 = b.col2 " + "JOIN c ON a.col3 = c.col3");
    Map<String, String> requestMetadataMap =
        ImmutableMap.of("REQUEST_ID", String.valueOf(RANDOM_REQUEST_ID_GEN.nextLong()));
    MailboxReceiveOperator mailboxReceiveOperator = null;
    for (int stageId : queryPlan.getStageMetadataMap().keySet()) {
      if (queryPlan.getQueryStageMap().get(stageId) instanceof MailboxReceiveNode) {
        MailboxReceiveNode reduceNode = (MailboxReceiveNode) queryPlan.getQueryStageMap().get(stageId);
        mailboxReceiveOperator = createReduceStageOperator(
            queryPlan.getStageMetadataMap().get(reduceNode.getSenderStageId()).getServerInstances(),
            Long.parseLong(requestMetadataMap.get("REQUEST_ID")), reduceNode.getSenderStageId(), _reducerGrpcPort);
      } else {
        for (ServerInstance serverInstance : queryPlan.getStageMetadataMap().get(stageId).getServerInstances()) {
          DistributedStagePlan distributedStagePlan =
              QueryDispatcher.constructDistributedStagePlan(queryPlan, stageId, serverInstance);
          _servers.get(serverInstance).processQuery(distributedStagePlan, requestMetadataMap);
        }
      }
    }
    Preconditions.checkNotNull(mailboxReceiveOperator);

    // Assert that each of the 5 categories from left table is joined with right table.
    // Specifically table A has 15 rows (10 on server1 and 5 on server2) and table B has 5 rows (all on server1),
    // thus the final JOIN result will be 15 x 1 = 15.
    // Next join with table C which has (5 on server1 and 10 on server2), since data is identical. each of the row of
    // the A JOIN B will have identical value of col3 as table C.col3 has. Since the values are cycling between
    // (1, 2, 42, 1, 2). we will have 6 1s, 6 2s, and 3 42s, total result count will be 36 + 36 + 9 = 81
    List<Object[]> resultRows = reduceMailboxReceive(mailboxReceiveOperator);
    Assert.assertEquals(resultRows.size(), 81);
  }

  @Test
  public void testJoinProjectFilterPushDown()
      throws Exception {
    String sqlQuery = "SELECT a.col1, a.ts, b.col2, b.col3 FROM a JOIN b ON a.col1 = b.col2"
        + " WHERE a.col3 >= 0 AND a.col2 = 'foo' AND b.col3 >= 0";
    QueryPlan queryPlan = _queryEnvironment.planQuery(sqlQuery);
    Map<String, String> requestMetadataMap =
        ImmutableMap.of("REQUEST_ID", String.valueOf(RANDOM_REQUEST_ID_GEN.nextLong()));
    MailboxReceiveOperator mailboxReceiveOperator = null;
    for (int stageId : queryPlan.getStageMetadataMap().keySet()) {
      if (queryPlan.getQueryStageMap().get(stageId) instanceof MailboxReceiveNode) {
        MailboxReceiveNode reduceNode = (MailboxReceiveNode) queryPlan.getQueryStageMap().get(stageId);
        mailboxReceiveOperator = createReduceStageOperator(
            queryPlan.getStageMetadataMap().get(reduceNode.getSenderStageId()).getServerInstances(),
            Long.parseLong(requestMetadataMap.get("REQUEST_ID")), reduceNode.getSenderStageId(), _reducerGrpcPort);
      } else {
        for (ServerInstance serverInstance : queryPlan.getStageMetadataMap().get(stageId).getServerInstances()) {
          DistributedStagePlan distributedStagePlan =
              QueryDispatcher.constructDistributedStagePlan(queryPlan, stageId, serverInstance);
          _servers.get(serverInstance).processQuery(distributedStagePlan, requestMetadataMap);
        }
      }
    }
    Preconditions.checkNotNull(mailboxReceiveOperator);

    // Assert that each of the 5 categories from left table is joined with right table.
    // Specifically table A has 15 rows (10 on server1 and 5 on server2) and table B has 5 rows (all on server1),
    // but only 1 out of 5 rows from table A will be selected out; and all in table B will be selected.
    // thus the final JOIN result will be 1 x 3 x 1 = 3.
    List<Object[]> resultRows = reduceMailboxReceive(mailboxReceiveOperator);
    Assert.assertEquals(resultRows.size(), 3);
  }

  protected static List<Object[]> reduceMailboxReceive(MailboxReceiveOperator mailboxReceiveOperator) {
    List<Object[]> resultRows = new ArrayList<>();
    DataTableBlock dataTableBlock;
    while (true) {
      dataTableBlock = mailboxReceiveOperator.nextBlock();
      if (DataTableBlockUtils.isEndOfStream(dataTableBlock)) {
        break;
      }
      if (dataTableBlock.getDataTable() != null) {
        DataTable dataTable = dataTableBlock.getDataTable();
        int numRows = dataTable.getNumberOfRows();
        for (int rowId = 0; rowId < numRows; rowId++) {
          resultRows.add(extractRowFromDataTable(dataTable, rowId));
        }
      }
    }
    return resultRows;
  }

  protected MailboxReceiveOperator createReduceStageOperator(List<ServerInstance> sendingInstances, long jobId,
      int stageId, int port) {
    MailboxReceiveOperator mailboxReceiveOperator =
        new MailboxReceiveOperator(_mailboxService, RelDistribution.Type.ANY, sendingInstances, "localhost", port,
            jobId, stageId);
    return mailboxReceiveOperator;
  }
}
