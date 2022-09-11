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
import org.apache.pinot.query.routing.WorkerInstance;
import org.apache.pinot.query.service.QueryConfig;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;

import static org.apache.pinot.core.query.selection.SelectionOperatorUtils.extractRowFromDataTable;


public class QueryRunnerTestBase {
  private static final File INDEX_DIR_S1_A = new File(FileUtils.getTempDirectory(), "QueryRunnerTest_server1_tableA");
  private static final File INDEX_DIR_S1_B = new File(FileUtils.getTempDirectory(), "QueryRunnerTest_server1_tableB");
  private static final File INDEX_DIR_S1_C = new File(FileUtils.getTempDirectory(), "QueryRunnerTest_server1_tableC");
  private static final File INDEX_DIR_S1_D = new File(FileUtils.getTempDirectory(), "QueryRunnerTest_server1_tableD");
  private static final File INDEX_DIR_S2_A = new File(FileUtils.getTempDirectory(), "QueryRunnerTest_server2_tableA");
  private static final File INDEX_DIR_S2_C = new File(FileUtils.getTempDirectory(), "QueryRunnerTest_server2_tableC");
  private static final File INDEX_DIR_S2_D = new File(FileUtils.getTempDirectory(), "QueryRunnerTest_server2_tableD");

  protected static final Random RANDOM_REQUEST_ID_GEN = new Random();

  protected QueryEnvironment _queryEnvironment;
  protected String _reducerHostname;
  protected int _reducerGrpcPort;
  protected Map<ServerInstance, QueryServerEnclosure> _servers = new HashMap<>();
  protected GrpcMailboxService _mailboxService;

  protected static List<Object[]> toRows(List<DataTable> dataTables) {
    List<Object[]> resultRows = new ArrayList<>();
    for (DataTable dataTable : dataTables) {
      int numRows = dataTable.getNumberOfRows();
      for (int rowId = 0; rowId < numRows; rowId++) {
        resultRows.add(extractRowFromDataTable(dataTable, rowId));
      }
    }
    return resultRows;
  }

  @BeforeClass
  public void setUp()
      throws Exception {
    DataTableFactory.setDataTableVersion(DataTableFactory.VERSION_4);
    QueryServerEnclosure server1 = new QueryServerEnclosure(Lists.newArrayList("a", "b", "c", "d_O"),
        ImmutableMap.of("a", INDEX_DIR_S1_A, "b", INDEX_DIR_S1_B, "c", INDEX_DIR_S1_C, "d_O", INDEX_DIR_S1_D),
        QueryEnvironmentTestUtils.SERVER1_SEGMENTS);
    QueryServerEnclosure server2 = new QueryServerEnclosure(Lists.newArrayList("a", "c", "d_R", "d_O"),
        ImmutableMap.of("a", INDEX_DIR_S2_A, "c", INDEX_DIR_S2_C, "d_R", INDEX_DIR_S2_D, "d_O", INDEX_DIR_S1_D),
        QueryEnvironmentTestUtils.SERVER2_SEGMENTS);

    _reducerGrpcPort = QueryEnvironmentTestUtils.getAvailablePort();
    _reducerHostname = String.format("Broker_%s", QueryConfig.DEFAULT_QUERY_RUNNER_HOSTNAME);
    Map<String, Object> reducerConfig = new HashMap<>();
    reducerConfig.put(QueryConfig.KEY_OF_QUERY_RUNNER_PORT, _reducerGrpcPort);
    reducerConfig.put(QueryConfig.KEY_OF_QUERY_RUNNER_HOSTNAME, _reducerHostname);
    _mailboxService = new GrpcMailboxService(_reducerHostname, _reducerGrpcPort, new PinotConfiguration(reducerConfig));
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
}
