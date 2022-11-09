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

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.pinot.common.datatable.DataTableFactory;
import org.apache.pinot.core.common.datatable.DataTableBuilderFactory;
import org.apache.pinot.query.QueryEnvironmentTestBase;
import org.apache.pinot.query.QueryServerEnclosure;
import org.apache.pinot.query.mailbox.GrpcMailboxService;
import org.apache.pinot.query.routing.WorkerInstance;
import org.apache.pinot.query.runtime.QueryRunnerTestBase;
import org.apache.pinot.query.service.QueryConfig;
import org.apache.pinot.query.testutils.MockInstanceDataManagerFactory;
import org.apache.pinot.query.testutils.QueryTestUtils;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public abstract class QueryTestCaseRunner extends QueryRunnerTestBase {

  public abstract List<TestCase> getTestCases();

  @BeforeClass
  public void setUp()
      throws Exception {
    DataTableBuilderFactory.setDataTableVersion(DataTableFactory.VERSION_4);
    // Setting up mock server factories.
    MockInstanceDataManagerFactory factory1 = new MockInstanceDataManagerFactory("server1");
    MockInstanceDataManagerFactory factory2 = new MockInstanceDataManagerFactory("server2");
    // Setting up H2 for validation
    setH2Connection();
    for (TestCase testCase : getTestCases()) {
      // table will be registered on both size.
      Map<String, Schema> schemaMap = new HashMap<>();
      for (Map.Entry<String, List<String>> e : testCase._tableSchemas.entrySet()) {
        String tableName = e.getKey();
        // TODO: able to choose table type, now default to OFFLINE
        String tableNameWithType = TableNameBuilder.forType(TableType.OFFLINE).tableNameWithType(tableName);
        org.apache.pinot.spi.data.Schema pinotSchema = constructSchema(tableName, e.getValue());
        schemaMap.put(tableName, pinotSchema);
        factory1.registerTable(pinotSchema, tableNameWithType);
        factory2.registerTable(pinotSchema, tableNameWithType);
      }
      for (Map.Entry<String, List<Object[]>> e : testCase._tableInputs.entrySet()) {
        String tableName = e.getKey();
        String tableNameWithType = TableNameBuilder.forType(TableType.OFFLINE).tableNameWithType(tableName);
        // TODO: able to select add rows to server1 or server2 (now default server1)
        // TODO: able to select add rows to existing segment or create new one (now default create one segment)
        factory1.addSegment(tableNameWithType, toRow(e.getValue()));
      }
      // add all the tables to
      for (Map.Entry<String, Schema> entry: schemaMap.entrySet()) {
        addTableToH2(entry.getValue(), Collections.singletonList(entry.getKey()));
        addDataToH2(entry.getValue(), factory1.buildTableRowsMap());
      }
    }
    QueryServerEnclosure server1 = new QueryServerEnclosure(factory1);
    QueryServerEnclosure server2 = new QueryServerEnclosure(factory2);

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

  @Test
  public void testTestCases()
      throws Exception {
    for (TestCase testCase : getTestCases()) {
      String sql = testCase._sql;
      List<Object[]> resultRows = queryRunner(sql);
      // query H2 for data
      List<Object[]> expectedRows = queryH2(sql);
      compareRowEquals(resultRows, expectedRows);
    }
  }
}
