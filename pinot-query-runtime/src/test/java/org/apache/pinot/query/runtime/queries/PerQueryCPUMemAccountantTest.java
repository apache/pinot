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
import org.apache.pinot.common.response.broker.ResultTable;
import org.apache.pinot.core.accounting.PerQueryCPUMemAccountantFactory;
import org.apache.pinot.core.accounting.ResourceUsageAccountantFactory;
import org.apache.pinot.spi.accounting.QueryResourceTracker;
import org.apache.pinot.spi.accounting.ThreadResourceUsageAccountant;
import org.apache.pinot.spi.accounting.ThreadResourceUsageProvider;
import org.apache.pinot.spi.config.instance.InstanceType;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.exception.EarlyTerminationException;
import org.apache.pinot.spi.trace.Tracing;
import org.apache.pinot.spi.utils.CommonConstants;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.testng.Assert.assertNotNull;


public class PerQueryCPUMemAccountantTest extends QueryRunnerAccountingTest {

  @Override
  protected ThreadResourceUsageAccountant getThreadResourceUsageAccountant() {
    HashMap<String, Object> configs = getAccountingConfig();

    ThreadResourceUsageProvider.setThreadMemoryMeasurementEnabled(true);

    return new PerQueryCPUMemAccountantFactory.PerQueryCPUMemResourceUsageAccountant(new PinotConfiguration(configs),
        "testWithPerQueryAccountantFactory", InstanceType.SERVER);
  }

  @Test
  void testWithPerQueryAccountantFactory() {
    try (MockedStatic<Tracing> tracing = Mockito.mockStatic(Tracing.class, Mockito.CALLS_REAL_METHODS)) {
      tracing.when(Tracing::getThreadAccountant).thenReturn(_accountant);

      ResultTable resultTable = queryRunner("SELECT * FROM a LIMIT 2", false).getResultTable();
      Assert.assertEquals(resultTable.getRows().size(), 2);

      Map<String, ? extends QueryResourceTracker> resources = _accountant.getQueryResources();
      Assert.assertEquals(resources.size(), 1);
      Assert.assertTrue(resources.entrySet().iterator().next().getValue().getAllocatedBytes() > 0);
    }
  }

  @Test
  void testWithResourceUsageAccountant() {
    HashMap<String, Object> configs = getAccountingConfig();

    ThreadResourceUsageProvider.setThreadMemoryMeasurementEnabled(true);
    ResourceUsageAccountantFactory.ResourceUsageAccountant accountant =
        new ResourceUsageAccountantFactory.ResourceUsageAccountant(new PinotConfiguration(configs),
            "testWithPerQueryAccountantFactory", InstanceType.SERVER);

    try (MockedStatic<Tracing> tracing = Mockito.mockStatic(Tracing.class, Mockito.CALLS_REAL_METHODS)) {
      tracing.when(Tracing::getThreadAccountant).thenReturn(accountant);

      ResultTable resultTable = queryRunner("SELECT * FROM a LIMIT 2", false).getResultTable();
      Assert.assertEquals(resultTable.getRows().size(), 2);

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
            "testWithPerQueryAccountantFactory", InstanceType.SERVER);

    try (MockedStatic<Tracing> tracing = Mockito.mockStatic(Tracing.class, Mockito.CALLS_REAL_METHODS)) {
      tracing.when(Tracing::getThreadAccountant).thenReturn(accountant);
      ResultTable resultTable = queryRunner("SELECT * FROM a LIMIT 2", false).getResultTable();
      Assert.assertEquals(resultTable.getRows().size(), 2);

      Map<String, ? extends QueryResourceTracker> resources = accountant.getQueryResources();
      Assert.assertEquals(resources.size(), 1);
      Assert.assertEquals(resources.entrySet().iterator().next().getValue().getAllocatedBytes(), 0);
    }
  }

  @Test
  void testDisableSamplingWithResourceUsageAccountantForMSE() {
    HashMap<String, Object> configs = getAccountingConfig();
    configs.put(CommonConstants.Accounting.CONFIG_OF_ENABLE_THREAD_SAMPLING_MSE, false);

    ThreadResourceUsageProvider.setThreadMemoryMeasurementEnabled(true);
    ResourceUsageAccountantFactory.ResourceUsageAccountant accountant =
        new ResourceUsageAccountantFactory.ResourceUsageAccountant(new PinotConfiguration(configs),
            "testWithPerQueryAccountantFactory", InstanceType.SERVER);

    try (MockedStatic<Tracing> tracing = Mockito.mockStatic(Tracing.class, Mockito.CALLS_REAL_METHODS)) {
      tracing.when(Tracing::getThreadAccountant).thenReturn(accountant);
      ResultTable resultTable = queryRunner("SELECT * FROM a LIMIT 2", false).getResultTable();
      Assert.assertEquals(resultTable.getRows().size(), 2);

      Map<String, ? extends QueryResourceTracker> resources = accountant.getQueryResources();
      Assert.assertEquals(resources.size(), 1);
      Assert.assertEquals(resources.entrySet().iterator().next().getValue().getAllocatedBytes(), 0);
    }
  }

  public static class InterruptingAccountant
      extends PerQueryCPUMemAccountantFactory.PerQueryCPUMemResourceUsageAccountant {

    public InterruptingAccountant(PinotConfiguration config, String instanceId, InstanceType instanceType) {
      super(config, instanceId, instanceType);
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
        new InterruptingAccountant(new PinotConfiguration(configs), "testWithPerQueryAccountantFactory",
            InstanceType.SERVER);

    try (MockedStatic<Tracing> tracing = Mockito.mockStatic(Tracing.class, Mockito.CALLS_REAL_METHODS)) {
      tracing.when(Tracing::getThreadAccountant).thenReturn(accountant);
      queryRunner("SELECT * FROM a LIMIT 2", false).getResultTable();
    }
  }

  @Test
  void testCancelCallback() {
    try (MockedStatic<Tracing> tracing = Mockito.mockStatic(Tracing.class, Mockito.CALLS_REAL_METHODS)) {
      tracing.when(Tracing::getThreadAccountant).thenReturn(_accountant);

      ResultTable resultTable = queryRunner("SELECT * FROM a LIMIT 2", false).getResultTable();
      Assert.assertEquals(resultTable.getRows().size(), 2);
      // TODO: How to programmatically get the request id ?
      PerQueryCPUMemAccountantFactory.PerQueryCPUMemResourceUsageAccountant perQueryAccountant =
          (PerQueryCPUMemAccountantFactory.PerQueryCPUMemResourceUsageAccountant) _accountant;
      assertNotNull(perQueryAccountant.getQueryCancelCallback("0"));
    }
  }

  private static HashMap<String, Object> getAccountingConfig() {
    HashMap<String, Object> configs = new HashMap<>();
    configs.put(CommonConstants.Accounting.CONFIG_OF_ENABLE_THREAD_MEMORY_SAMPLING, true);
    return configs;
  }
}
