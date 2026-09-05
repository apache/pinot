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
package org.apache.pinot.broker.broker.helix;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixManager;
import org.apache.helix.model.InstanceConfig;
import org.apache.pinot.broker.broker.BrokerDrainManager;
import org.apache.pinot.common.utils.config.TagNameUtils;
import org.apache.pinot.common.utils.helix.HelixHelper;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.CommonConstants.Helix;
import org.mockito.MockedStatic;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;


public class BaseBrokerStarterTest {
  private static final String INSTANCE_ID = "Broker_localhost_1234";

  @Test
  public void testBrokerResourceReconcileRetriedAfterFlagClearAndFailure() {
    HelixDataAccessor helixDataAccessor = mock(HelixDataAccessor.class);
    HelixManager helixManager = mock(HelixManager.class);
    when(helixManager.getHelixDataAccessor()).thenReturn(helixDataAccessor);
    InstanceConfig instanceConfig = new InstanceConfig(INSTANCE_ID);
    instanceConfig.addTag("customTag");
    instanceConfig.getRecord().setListField(Helix.PREVIOUS_TAGS,
        List.of(TagNameUtils.getBrokerTagForTenant(null), "customTag"));
    instanceConfig.getRecord().setBooleanField(Helix.IS_SHUTDOWN_IN_PROGRESS, true);

    PinotConfiguration brokerConf = new PinotConfiguration();
    brokerConf.setProperty(Helix.CONFIG_OF_MULTI_STAGE_ENGINE_ENABLED, false);
    HelixBrokerStarter brokerStarter = new HelixBrokerStarter();
    brokerStarter._brokerConf = brokerConf;
    brokerStarter._participantHelixManager = helixManager;
    brokerStarter._instanceId = INSTANCE_ID;
    brokerStarter._hostname = "localhost";
    brokerStarter._port = 1234;
    brokerStarter._tlsPort = -1;
    brokerStarter._brokerDrainManager =
        new BrokerDrainManager(INSTANCE_ID, () -> helixManager, () -> {
        }, () -> {
        }, 10_000L);

    AtomicInteger reconcileAttempts = new AtomicInteger();
    try (MockedStatic<HelixHelper> helixHelper = mockStatic(HelixHelper.class)) {
      helixHelper.when(() -> HelixHelper.updateInstanceConfig(eq(helixDataAccessor), eq(INSTANCE_ID),
          any())).thenAnswer(invocation -> {
            Consumer<InstanceConfig> updater = invocation.getArgument(2);
            updater.accept(instanceConfig);
            return true;
          });
      helixHelper.when(() -> HelixHelper.updateBrokerResource(eq(helixManager), eq(INSTANCE_ID),
          eq(List.of(TagNameUtils.getBrokerTagForTenant(null))), anyList(), isNull()))
          .thenAnswer(invocation -> {
            assertFalse(instanceConfig.getRecord().getBooleanField(Helix.IS_SHUTDOWN_IN_PROGRESS, false),
                "Broker must be eligible before brokerResource reconciliation");
            assertEquals(instanceConfig.getTags(), List.of(TagNameUtils.getBrokerTagForTenant(null), "customTag"));
            assertFalse(instanceConfig.getRecord().getListFields().containsKey(Helix.PREVIOUS_TAGS));
            if (reconcileAttempts.getAndIncrement() == 0) {
              throw new IllegalStateException("injected brokerResource update failure");
            }
            return null;
          });

      assertThrows(IllegalStateException.class, brokerStarter::updateInstanceConfigAndBrokerResourceIfNeeded);
      assertFalse(instanceConfig.getRecord().getBooleanField(Helix.IS_SHUTDOWN_IN_PROGRESS, false));
      assertThrows(BrokerDrainManager.BrokerStartupInProgressException.class,
          () -> brokerStarter._brokerDrainManager.drain(0L, false));

      // The marker is already false, but the next startup still reconciles brokerResource and recovers.
      brokerStarter.updateInstanceConfigAndBrokerResourceIfNeeded();
      assertThrows(BrokerDrainManager.BrokerStartupInProgressException.class,
          () -> brokerStarter._brokerDrainManager.drain(0L, false));

      assertEquals(reconcileAttempts.get(), 2);
      helixHelper.verify(() -> HelixHelper.updateBrokerResource(eq(helixManager), eq(INSTANCE_ID),
          eq(List.of(TagNameUtils.getBrokerTagForTenant(null))), anyList(), isNull()), times(2));
    }
  }

  @Test
  public void testStopIsSerializedAndIdempotent()
      throws Exception {
    BlockingStopBrokerStarter brokerStarter = new BlockingStopBrokerStarter();
    ExecutorService executor = Executors.newFixedThreadPool(2);
    try {
      Future<?> firstStop = executor.submit(brokerStarter::stop);
      assertTrue(brokerStarter._stopEntered.await(10, TimeUnit.SECONDS));
      Future<?> secondStop = executor.submit(brokerStarter::stop);

      expectThrows(TimeoutException.class, () -> secondStop.get(1, TimeUnit.SECONDS));
      brokerStarter._allowStop.countDown();

      firstStop.get(10, TimeUnit.SECONDS);
      secondStop.get(10, TimeUnit.SECONDS);
      assertEquals(brokerStarter._stopCount.get(), 1);
    } finally {
      brokerStarter._allowStop.countDown();
      executor.shutdownNow();
      assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS));
    }
  }

  @Test
  public void testStopCanBeRetriedAfterFailure() {
    FailOnceStopBrokerStarter brokerStarter = new FailOnceStopBrokerStarter();

    assertThrows(IllegalStateException.class, brokerStarter::stop);
    brokerStarter.stop();
    brokerStarter.stop();

    assertEquals(brokerStarter._stopCount.get(), 2);
  }

  private static final class BlockingStopBrokerStarter extends HelixBrokerStarter {
    private final CountDownLatch _stopEntered = new CountDownLatch(1);
    private final CountDownLatch _allowStop = new CountDownLatch(1);
    private final AtomicInteger _stopCount = new AtomicInteger();

    @Override
    void stopBrokerComponents() {
      _stopCount.incrementAndGet();
      _stopEntered.countDown();
      try {
        if (!_allowStop.await(10, TimeUnit.SECONDS)) {
          throw new IllegalStateException("Timed out waiting to finish broker stop");
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new IllegalStateException("Interrupted while stopping broker", e);
      }
    }
  }

  private static final class FailOnceStopBrokerStarter extends HelixBrokerStarter {
    private final AtomicInteger _stopCount = new AtomicInteger();

    @Override
    void stopBrokerComponents() {
      if (_stopCount.getAndIncrement() == 0) {
        throw new IllegalStateException("injected broker stop failure");
      }
    }
  }
}
