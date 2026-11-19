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
package org.apache.pinot.query.runtime.queries;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.pinot.common.response.broker.ResultTable;
import org.apache.pinot.core.routing.MockRoutingManagerFactory;
import org.apache.pinot.core.routing.TablePartitionReplicatedServersInfo;
import org.apache.pinot.core.routing.TablePartitionReplicatedServersInfo.PartitionInfo;
import org.apache.pinot.query.QueryEnvironment;
import org.apache.pinot.query.QueryServerEnclosure;
import org.apache.pinot.query.mailbox.MailboxService;
import org.apache.pinot.query.routing.QueryServerInstance;
import org.apache.pinot.query.routing.WorkerManager;
import org.apache.pinot.query.testutils.MockInstanceDataManagerFactory;
import org.apache.pinot.query.testutils.QueryTestUtils;
import org.apache.pinot.spi.config.instance.InstanceType;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.CommonConstants.MultiStageQueryRunner;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;


/// Runs queries over a table that is routed with optional segments, i.e. new segments the broker has not seen online
/// yet. The leaf stage queries the optional segments the server has loaded, and skips without error the ones it has
/// not.
public class OptionalSegmentsQueryRunnerTest extends QueryRunnerTestBase {
  private static final String TABLE_NAME = "tbl";
  private static final String OFFLINE_TABLE_NAME = TABLE_NAME + "_OFFLINE";
  private static final String NEW_TABLE_NAME = "newTbl";
  private static final String NEW_OFFLINE_TABLE_NAME = NEW_TABLE_NAME + "_OFFLINE";
  private static final int NUM_ROWS_PER_SEGMENT = 5;

  @BeforeClass
  public void setUp()
      throws Exception {
    Schema schema = new Schema.SchemaBuilder()
        .setSchemaName(TABLE_NAME)
        .addSingleValueDimension("col1", DataType.STRING, "")
        .addDateTime("ts", DataType.LONG, "1:MILLISECONDS:EPOCH", "1:HOURS")
        .build();
    MockInstanceDataManagerFactory instanceDataManagerFactory = new MockInstanceDataManagerFactory("server");
    instanceDataManagerFactory.registerTable(schema, OFFLINE_TABLE_NAME);
    String segment = instanceDataManagerFactory.addSegment(OFFLINE_TABLE_NAME, buildRows()).getSegmentName();
    String loadedNewSegment = instanceDataManagerFactory.addSegment(OFFLINE_TABLE_NAME, buildRows()).getSegmentName();

    _reducerHostname = "localhost";
    _reducerPort = QueryTestUtils.getAvailablePort();
    _mailboxService = new MailboxService(_reducerHostname, _reducerPort, InstanceType.BROKER, new PinotConfiguration(
        Map.of(MultiStageQueryRunner.KEY_OF_QUERY_RUNNER_HOSTNAME, _reducerHostname,
            MultiStageQueryRunner.KEY_OF_QUERY_RUNNER_PORT, _reducerPort)));
    _mailboxService.start();

    QueryServerEnclosure server = new QueryServerEnclosure(instanceDataManagerFactory);
    server.start();
    int port = server.getPort();
    _servers.put(new QueryServerInstance("Server_localhost_" + port, "localhost", port, port), server);

    MockRoutingManagerFactory routingManagerFactory = new MockRoutingManagerFactory(port);
    routingManagerFactory.registerTable(schema, OFFLINE_TABLE_NAME);
    routingManagerFactory.registerSegment(port, OFFLINE_TABLE_NAME, segment);
    // Two new segments: one the server has loaded, and one it has not
    String notLoadedNewSegment = OFFLINE_TABLE_NAME + "_notLoaded";
    routingManagerFactory.registerOptionalSegment(port, OFFLINE_TABLE_NAME, loadedNewSegment);
    routingManagerFactory.registerOptionalSegment(port, OFFLINE_TABLE_NAME, notLoadedNewSegment);
    // The partition info of the table lists them as the optional segments of its only partition
    PartitionInfo partitionInfo = new PartitionInfo(Set.of(MockRoutingManagerFactory.toHostname(port)),
        List.of(segment), List.of(loadedNewSegment, notLoadedNewSegment));
    TablePartitionReplicatedServersInfo tablePartitionInfo = new TablePartitionReplicatedServersInfo(
        OFFLINE_TABLE_NAME, "col1", "Hashcode", 1, new PartitionInfo[]{partitionInfo}, List.of(), Set.of());
    // Another table whose only segment is new, and which the server has not set up yet
    Schema newTableSchema = new Schema.SchemaBuilder()
        .setSchemaName(NEW_TABLE_NAME)
        .addSingleValueDimension("col1", DataType.STRING, "")
        .build();
    routingManagerFactory.registerTable(newTableSchema, NEW_OFFLINE_TABLE_NAME);
    routingManagerFactory.registerEmptyTable(NEW_OFFLINE_TABLE_NAME);
    routingManagerFactory.registerOptionalSegment(port, NEW_OFFLINE_TABLE_NAME, NEW_OFFLINE_TABLE_NAME + "_new");
    _queryEnvironment = new QueryEnvironment(QueryEnvironment.configBuilder()
        .requestId(-1L)
        .database(CommonConstants.DEFAULT_DATABASE)
        .tableCache(routingManagerFactory.buildTableCache())
        .workerManager(new WorkerManager("Broker_localhost", "localhost", _reducerPort,
            routingManagerFactory.buildRoutingManager(Map.of(OFFLINE_TABLE_NAME, tablePartitionInfo))))
        .isNullHandlingEnabled(true)
        .enableReplicatedLeafOptionalSegments(true)
        .build());
  }

  private static List<GenericRow> buildRows() {
    List<GenericRow> rows = new ArrayList<>(NUM_ROWS_PER_SEGMENT);
    for (int i = 0; i < NUM_ROWS_PER_SEGMENT; i++) {
      GenericRow row = new GenericRow();
      row.putValue("col1", "value" + i);
      row.putValue("ts", System.currentTimeMillis());
      rows.add(row);
    }
    return rows;
  }

  @AfterClass
  public void tearDown() {
    for (QueryServerEnclosure server : _servers.values()) {
      server.shutDown();
    }
    _mailboxService.shutdown();
  }

  @DataProvider
  public static Object[][] usePhysicalOptimizer() {
    return new Object[][]{{false}, {true}};
  }

  @Test(dataProvider = "usePhysicalOptimizer")
  public void testQueriesLoadedOptionalSegments(boolean usePhysicalOptimizer) {
    ResultTable resultTable = queryRunner(
        "SET usePhysicalOptimizer=" + usePhysicalOptimizer + "; SELECT COUNT(*) FROM " + TABLE_NAME, false)
        .getResultTable();
    assertEquals(resultTable.getRows().get(0)[0], 2L * NUM_ROWS_PER_SEGMENT);
  }

  @Test
  public void testPartitionedLeafQueriesLoadedOptionalSegments() {
    ResultTable resultTable = queryRunner("SELECT COUNT(*) FROM " + TABLE_NAME
        + " /*+ tableOptions(partition_function='hashcode', partition_key='col1', partition_size='1') */", false)
        .getResultTable();
    assertEquals(resultTable.getRows().get(0)[0], 2L * NUM_ROWS_PER_SEGMENT);
  }

  @Test
  public void testReplicatedLeafSkipsOptionalSegmentsOfTableNotOnServer() {
    ResultTable resultTable = queryRunner(
        "SELECT /*+ joinOptions(left_distribution_type='local', right_distribution_type='local') */ COUNT(*) FROM "
            + TABLE_NAME + " JOIN " + NEW_TABLE_NAME + " /*+ tableOptions(is_replicated='true') */ ON " + TABLE_NAME
            + ".col1 = " + NEW_TABLE_NAME + ".col1", false).getResultTable();
    assertEquals(resultTable.getRows().get(0)[0], 0L);
  }
}
