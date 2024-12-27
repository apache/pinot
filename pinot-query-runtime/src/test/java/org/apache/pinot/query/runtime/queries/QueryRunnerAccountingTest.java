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

import java.util.HashMap;
import java.util.Map;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.common.response.broker.ResultTableRows;
import org.apache.pinot.core.accounting.PerQueryCPUMemAccountantFactory;
import org.apache.pinot.query.QueryEnvironmentTestBase;
import org.apache.pinot.query.QueryServerEnclosure;
import org.apache.pinot.query.mailbox.MailboxService;
import org.apache.pinot.query.routing.QueryServerInstance;
import org.apache.pinot.query.testutils.MockInstanceDataManagerFactory;
import org.apache.pinot.query.testutils.QueryTestUtils;
import org.apache.pinot.spi.accounting.QueryResourceTracker;
import org.apache.pinot.spi.accounting.ThreadResourceUsageAccountant;
import org.apache.pinot.spi.accounting.ThreadResourceUsageProvider;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.exception.EarlyTerminationException;
import org.apache.pinot.spi.trace.Tracing;
import org.apache.pinot.spi.utils.CommonConstants;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class QueryRunnerAccountingTest extends QueryRunnerTestBase {

  @BeforeClass
  public void setUp()
      throws Exception {
    MockInstanceDataManagerFactory factory1 = new MockInstanceDataManagerFactory("server1");
    factory1.registerTable(QueryRunnerTest.SCHEMA_BUILDER.setSchemaName("a").build(), "a_REALTIME");
    factory1.addSegment("a_REALTIME", QueryRunnerTest.buildRows("a_REALTIME"));

    MockInstanceDataManagerFactory factory2 = new MockInstanceDataManagerFactory("server2");
    factory2.registerTable(QueryRunnerTest.SCHEMA_BUILDER.setSchemaName("b").build(), "b_REALTIME");
    factory2.addSegment("b_REALTIME", QueryRunnerTest.buildRows("b_REALTIME"));

    _reducerHostname = "localhost";
    _reducerPort = QueryTestUtils.getAvailablePort();
    Map<String, Object> reducerConfig = new HashMap<>();
    reducerConfig.put(CommonConstants.MultiStageQueryRunner.KEY_OF_QUERY_RUNNER_HOSTNAME, _reducerHostname);
    reducerConfig.put(CommonConstants.MultiStageQueryRunner.KEY_OF_QUERY_RUNNER_PORT, _reducerPort);
    _mailboxService = new MailboxService(_reducerHostname, _reducerPort, new PinotConfiguration(reducerConfig));
    _mailboxService.start();

    QueryServerEnclosure server1 = new QueryServerEnclosure(factory1);
    server1.start();
    // Start server1 to ensure the next server will have a different port.
    QueryServerEnclosure server2 = new QueryServerEnclosure(factory2);
    server2.start();
    // this doesn't test the QueryServer functionality so the server port can be the same as the mailbox port.
    // this is only use for test identifier purpose.
    int port1 = server1.getPort();
    int port2 = server2.getPort();
    _servers.put(new QueryServerInstance("localhost", port1, port1), server1);
    _servers.put(new QueryServerInstance("localhost", port2, port2), server2);

    _queryEnvironment = QueryEnvironmentTestBase.getQueryEnvironment(_reducerPort, server1.getPort(), server2.getPort(),
        factory1.getRegisteredSchemaMap(), factory1.buildTableSegmentNameMap(), factory2.buildTableSegmentNameMap(),
        null);
  }

  @AfterClass
  public void tearDown() {
    for (QueryServerEnclosure server : _servers.values()) {
      server.shutDown();
    }
    _mailboxService.shutdown();
  }

  @Test
  void testWithDefaultThreadAccountant() {
    Tracing.DefaultThreadResourceUsageAccountant accountant = new Tracing.DefaultThreadResourceUsageAccountant();
    try (MockedStatic<Tracing> tracing = Mockito.mockStatic(Tracing.class, Mockito.CALLS_REAL_METHODS)) {
      tracing.when(Tracing::getThreadAccountant).thenReturn(accountant);

      ResultTableRows resultTableRows = queryRunner("SELECT * FROM a LIMIT 2", false).getResultTable();
      Assert.assertEquals(resultTableRows.getRows().size(), 2);

      ThreadResourceUsageAccountant threadAccountant = Tracing.getThreadAccountant();
      Assert.assertTrue(threadAccountant.getThreadResources().isEmpty());
      Assert.assertTrue(threadAccountant.getQueryResources().isEmpty());
    }
  }

  @Test
  void testWithPerQueryAccountantFactory() {
    HashMap<String, Object> configs = getAccountingConfig();

    ThreadResourceUsageProvider.setThreadMemoryMeasurementEnabled(true);
    PerQueryCPUMemAccountantFactory.PerQueryCPUMemResourceUsageAccountant accountant =
        new PerQueryCPUMemAccountantFactory.PerQueryCPUMemResourceUsageAccountant(new PinotConfiguration(configs),
            "testWithPerQueryAccountantFactory");

    try (MockedStatic<Tracing> tracing = Mockito.mockStatic(Tracing.class, Mockito.CALLS_REAL_METHODS)) {
      tracing.when(Tracing::getThreadAccountant).thenReturn(accountant);

      ResultTableRows resultTableRows = queryRunner("SELECT * FROM a LIMIT 2", false).getResultTable();
      Assert.assertEquals(resultTableRows.getRows().size(), 2);

      Map<String, ? extends QueryResourceTracker> resources = accountant.getQueryResources();
      Assert.assertEquals(resources.size(), 1);
      Assert.assertTrue(resources.entrySet().iterator().next().getValue().getAllocatedBytes() > 0);
    }
  }

  @Test
  void testDisableSamplingForMSE() {
    HashMap<String, Object> configs = getAccountingConfig();
    configs.put(CommonConstants.Accounting.CONFIG_OF_ENABLE_THREAD_SAMPLING_MSE, false);

    ThreadResourceUsageProvider.setThreadMemoryMeasurementEnabled(true);
    PerQueryCPUMemAccountantFactory.PerQueryCPUMemResourceUsageAccountant accountant =
        new PerQueryCPUMemAccountantFactory.PerQueryCPUMemResourceUsageAccountant(new PinotConfiguration(configs),
            "testWithPerQueryAccountantFactory");

    try (MockedStatic<Tracing> tracing = Mockito.mockStatic(Tracing.class, Mockito.CALLS_REAL_METHODS)) {
      tracing.when(Tracing::getThreadAccountant).thenReturn(accountant);
      ResultTableRows resultTableRows = queryRunner("SELECT * FROM a LIMIT 2", false).getResultTable();
      Assert.assertEquals(resultTableRows.getRows().size(), 2);

      Map<String, ? extends QueryResourceTracker> resources = accountant.getQueryResources();
      Assert.assertEquals(resources.size(), 1);
      Assert.assertEquals(resources.entrySet().iterator().next().getValue().getAllocatedBytes(), 0);
    }
  }

  public static class InterruptingAccountant
      extends PerQueryCPUMemAccountantFactory.PerQueryCPUMemResourceUsageAccountant {

    public InterruptingAccountant(PinotConfiguration config, String instanceId) {
      super(config, instanceId);
    }

    @Override
    public boolean isAnchorThreadInterrupted() {
      return true;
    }
  }

  @Test(expectedExceptions = EarlyTerminationException.class)
  void testInterrupt() {
    HashMap<String, Object> configs = getAccountingConfig();

    ThreadResourceUsageProvider.setThreadMemoryMeasurementEnabled(true);
    InterruptingAccountant accountant =
        new InterruptingAccountant(new PinotConfiguration(configs), "testWithPerQueryAccountantFactory");

    try (MockedStatic<Tracing> tracing = Mockito.mockStatic(Tracing.class, Mockito.CALLS_REAL_METHODS)) {
      tracing.when(Tracing::getThreadAccountant).thenReturn(accountant);
      queryRunner("SELECT * FROM a LIMIT 2", false).getResultTable();
    }
  }

  private static HashMap<String, Object> getAccountingConfig() {
    HashMap<String, Object> configs = new HashMap<>();
    ServerMetrics.register(Mockito.mock(ServerMetrics.class));
    configs.put(CommonConstants.Accounting.CONFIG_OF_ENABLE_THREAD_MEMORY_SAMPLING, true);
    return configs;
  }
}
