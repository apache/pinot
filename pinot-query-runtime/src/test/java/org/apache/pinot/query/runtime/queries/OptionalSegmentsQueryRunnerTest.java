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
import org.apache.pinot.common.response.broker.ResultTable;
import org.apache.pinot.core.routing.MockRoutingManagerFactory;
import org.apache.pinot.query.QueryEnvironment;
import org.apache.pinot.query.QueryServerEnclosure;
import org.apache.pinot.query.mailbox.MailboxService;
import org.apache.pinot.query.routing.QueryServerInstance;
import org.apache.pinot.query.routing.WorkerManager;
import org.apache.pinot.query.testutils.MockInstanceDataManagerFactory;
import org.apache.pinot.query.testutils.QueryTestUtils;
import org.apache.pinot.spi.config.instance.InstanceType;
import org.apache.pinot.spi.data.FieldSpec;
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


/// Runs queries over a table routed with optional segments, i.e. new segments the broker has not seen ONLINE yet. As on
/// the single-stage engine, the leaf stage must query the optional segments the server has, and skip without error the
/// ones it does not have yet.
public class OptionalSegmentsQueryRunnerTest extends QueryRunnerTestBase {
  private static final String TABLE_NAME = "tbl";
  private static final String OFFLINE_TABLE_NAME = TABLE_NAME + "_OFFLINE";
  private static final int NUM_ROWS_PER_SEGMENT = 5;

  @BeforeClass
  public void setUp()
      throws Exception {
    Schema schema = new Schema.SchemaBuilder().setSchemaName(TABLE_NAME)
        .addSingleValueDimension("col1", FieldSpec.DataType.STRING, "")
        .addDateTime("ts", FieldSpec.DataType.LONG, "1:MILLISECONDS:EPOCH", "1:HOURS")
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
    // Two new segments: one the server has already loaded, and one it has not.
    routingManagerFactory.registerOptionalSegment(port, OFFLINE_TABLE_NAME, loadedNewSegment);
    routingManagerFactory.registerOptionalSegment(port, OFFLINE_TABLE_NAME, OFFLINE_TABLE_NAME + "_notLoadedYet");
    _queryEnvironment = new QueryEnvironment(CommonConstants.DEFAULT_DATABASE, routingManagerFactory.buildTableCache(),
        new WorkerManager("Broker_localhost", "localhost", _reducerPort,
            routingManagerFactory.buildRoutingManager(null)));
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
  public static Object[][] physicalOptimizer() {
    return new Object[][]{{false}, {true}};
  }

  @Test(dataProvider = "physicalOptimizer")
  public void testQueriesLoadedOptionalSegmentAndSkipsMissingOne(boolean usePhysicalOptimizer) {
    ResultTable resultTable = queryRunner(
        "SET usePhysicalOptimizer=" + usePhysicalOptimizer + "; SELECT COUNT(*) FROM " + TABLE_NAME, false)
        .getResultTable();
    assertEquals(resultTable.getRows().get(0)[0], 2L * NUM_ROWS_PER_SEGMENT);
  }
}
