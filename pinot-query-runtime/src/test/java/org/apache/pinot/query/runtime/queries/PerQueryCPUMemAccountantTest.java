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
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.pinot.common.response.broker.ResultTable;
import org.apache.pinot.core.accounting.PerQueryCPUMemAccountantFactory;
import org.apache.pinot.query.planner.physical.DispatchableSubPlan;
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

import static org.testng.Assert.assertTrue;


public class PerQueryCPUMemAccountantTest extends QueryRunnerAccountingTest {

  public static class ConditionalBlockingAccountant
      extends PerQueryCPUMemAccountantFactory.PerQueryCPUMemResourceUsageAccountant {

    CountDownLatch _latch;
    public ConditionalBlockingAccountant(PinotConfiguration config, String instanceId, InstanceType instanceType) {
      super(config, instanceId, instanceType);
      _latch = null;
    }

    void setLatch(CountDownLatch latch) {
      _latch = latch;
    }

    @Override
    public boolean isAnchorThreadInterrupted() {
      if (_latch != null) {
        try {
          _latch.await();
        } catch (InterruptedException e) {
        }
      }
      return super.isAnchorThreadInterrupted();
    }
  }

  CountDownLatch _submitLatch = new CountDownLatch(1);

  @Override
  protected ThreadResourceUsageAccountant getThreadResourceUsageAccountant() {
    HashMap<String, Object> configs = getAccountingConfig();

    ThreadResourceUsageProvider.setThreadMemoryMeasurementEnabled(true);

    return new ConditionalBlockingAccountant(new PinotConfiguration(configs),
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

  @Override
  protected List<CompletableFuture<?>> processDistributedStagePlans(DispatchableSubPlan dispatchableSubPlan,
      long requestId, int stageId, Map<String, String> requestMetadataMap) {
    List<CompletableFuture<?>> futures = super.processDistributedStagePlans(dispatchableSubPlan, requestId, stageId,
        requestMetadataMap);
    _submitLatch.countDown();
    return futures;
  }

  @Test
  void testCancelCallback()
      throws InterruptedException {
    try (MockedStatic<Tracing> tracing = Mockito.mockStatic(Tracing.class, Mockito.CALLS_REAL_METHODS)) {
      tracing.when(Tracing::getThreadAccountant).thenReturn(_accountant);

      CountDownLatch latch = new CountDownLatch(1);
      ((ConditionalBlockingAccountant) _accountant).setLatch(latch);

      ExecutorService service = Executors.newFixedThreadPool(1);
      service.submit(() -> {
        ResultTable resultTable = queryRunner("SELECT * FROM a LIMIT 2", false).getResultTable();
        Assert.assertEquals(resultTable.getRows().size(), 2);
      });

      _submitLatch.await();
      // TODO: How to programmatically get the request id ?
      final String queryId = "0";
      PerQueryCPUMemAccountantFactory.PerQueryCPUMemResourceUsageAccountant perQueryAccountant =
          (PerQueryCPUMemAccountantFactory.PerQueryCPUMemResourceUsageAccountant) _accountant;
      perQueryAccountant.cancelQuery(queryId, null);
      assertTrue(perQueryAccountant.getCancelSentQueries().contains(queryId));
      latch.countDown();
    }
  }

  private static HashMap<String, Object> getAccountingConfig() {
    HashMap<String, Object> configs = new HashMap<>();
    configs.put(CommonConstants.Accounting.CONFIG_OF_ENABLE_THREAD_MEMORY_SAMPLING, true);
    return configs;
  }
}
