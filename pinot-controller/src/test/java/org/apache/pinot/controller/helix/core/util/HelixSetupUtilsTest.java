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
package org.apache.pinot.controller.helix.core.util;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.helix.HelixAdmin;
import org.apache.helix.manager.zk.ZKHelixAdmin;
import org.apache.helix.model.HelixConfigScope;
import org.apache.helix.model.HelixConfigScope.ConfigScopeProperty;
import org.apache.helix.model.builder.HelixConfigScopeBuilder;
import org.apache.pinot.common.utils.ServiceStartableUtils;
import org.apache.pinot.common.utils.ZkStarter;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.services.ServiceRole;
import org.apache.pinot.spi.utils.CommonConstants;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.expectThrows;


public class HelixSetupUtilsTest {
  private static final String CLUSTER_NAME = "HelixSetupUtilsTest";
  private static final String POLICY_KEY = CommonConstants.Groovy.DISABLE_INGESTION_GROOVY;

  private ZkStarter.ZookeeperInstance _zookeeperInstance;
  private HelixAdmin _helixAdmin;
  private HelixConfigScope _clusterConfigScope;

  @BeforeClass
  public void setUp() {
    _zookeeperInstance = ZkStarter.startLocalZkServer();
    HelixSetupUtils.setupHelixClusterWithDefaultConfigs(_zookeeperInstance.getZkUrl(), CLUSTER_NAME,
        Map.of("testConfig", "testValue"));
    _helixAdmin = new ZKHelixAdmin.Builder().setZkAddress(_zookeeperInstance.getZkUrl()).build();
    _clusterConfigScope =
        new HelixConfigScopeBuilder(ConfigScopeProperty.CLUSTER).forCluster(CLUSTER_NAME).build();
  }

  @AfterClass
  public void tearDown() {
    if (_helixAdmin != null) {
      _helixAdmin.close();
    }
    if (_zookeeperInstance != null) {
      ZkStarter.stopLocalZkServer(_zookeeperInstance);
    }
  }

  @BeforeMethod
  public void clearPolicy() {
    _helixAdmin.removeConfig(_clusterConfigScope, List.of(POLICY_KEY));
  }

  @Test
  public void testFirstControllerSeedsPolicyAndLaterControllersAdoptIt() {
    HelixSetupUtils.reconcileIngestionGroovyPolicy(_zookeeperInstance.getZkUrl(), CLUSTER_NAME, true, false);
    HelixSetupUtils.reconcileIngestionGroovyPolicy(_zookeeperInstance.getZkUrl(), CLUSTER_NAME, false, true);

    assertEquals(getPolicy(), "false");
    PinotConfiguration serverConfig = new PinotConfiguration();
    ServiceStartableUtils.applyClusterConfig(
        serverConfig, _zookeeperInstance.getZkUrl(), CLUSTER_NAME, ServiceRole.SERVER);
    assertEquals(serverConfig.getProperty(POLICY_KEY), "false");
    PinotConfiguration minionConfig = new PinotConfiguration();
    ServiceStartableUtils.applyClusterConfig(
        minionConfig, _zookeeperInstance.getZkUrl(), CLUSTER_NAME, ServiceRole.MINION);
    assertEquals(minionConfig.getProperty(POLICY_KEY), "false");
  }

  @Test
  public void testExplicitPolicyMustMatchAuthoritativeClusterPolicy() {
    HelixSetupUtils.reconcileIngestionGroovyPolicy(_zookeeperInstance.getZkUrl(), CLUSTER_NAME, true, false);

    IllegalStateException error = expectThrows(IllegalStateException.class,
        () -> HelixSetupUtils.reconcileIngestionGroovyPolicy(
            _zookeeperInstance.getZkUrl(), CLUSTER_NAME, true, true));
    assertEquals(error.getMessage(), String.format(
        "Conflicting ingestion Groovy policy: cluster config '%s=false' is authoritative, but the controller config "
            + "resolves to true. Update the cluster config before restarting controllers", POLICY_KEY));
    assertEquals(getPolicy(), "false");
  }

  @Test
  public void testConcurrentControllersCannotInitializeConflictingPolicies()
      throws InterruptedException {
    CountDownLatch startLatch = new CountDownLatch(1);
    AtomicReference<Throwable> disabledControllerError = new AtomicReference<>();
    AtomicReference<Throwable> enabledControllerError = new AtomicReference<>();
    Thread disabledController = new Thread(
        () -> reconcileAfterLatch(startLatch, true, disabledControllerError), "disabled-groovy-controller");
    Thread enabledController = new Thread(
        () -> reconcileAfterLatch(startLatch, false, enabledControllerError), "enabled-groovy-controller");

    disabledController.start();
    enabledController.start();
    startLatch.countDown();
    disabledController.join();
    enabledController.join();

    String policy = getPolicy();
    if (Boolean.parseBoolean(policy)) {
      assertNull(disabledControllerError.get());
      assertNotNull(enabledControllerError.get());
    } else {
      assertNotNull(disabledControllerError.get());
      assertNull(enabledControllerError.get());
    }
  }

  @Test
  public void testConcurrentControllersCanInitializeSamePolicy()
      throws InterruptedException {
    CountDownLatch startLatch = new CountDownLatch(1);
    List<AtomicReference<Throwable>> errors = new ArrayList<>();
    List<Thread> controllers = new ArrayList<>();
    for (int i = 0; i < 8; i++) {
      AtomicReference<Throwable> error = new AtomicReference<>();
      errors.add(error);
      controllers.add(new Thread(
          () -> reconcileAfterLatch(startLatch, true, error), "disabled-groovy-controller-" + i));
    }

    controllers.forEach(Thread::start);
    startLatch.countDown();
    for (Thread controller : controllers) {
      controller.join();
    }

    assertEquals(getPolicy(), "true");
    for (AtomicReference<Throwable> error : errors) {
      assertNull(error.get());
    }
  }

  private void reconcileAfterLatch(CountDownLatch startLatch, boolean disableGroovy,
      AtomicReference<Throwable> error) {
    try {
      startLatch.await();
      HelixSetupUtils.reconcileIngestionGroovyPolicy(
          _zookeeperInstance.getZkUrl(), CLUSTER_NAME, true, disableGroovy);
    } catch (Throwable t) {
      error.set(t);
    }
  }

  private String getPolicy() {
    return _helixAdmin.getConfig(_clusterConfigScope, List.of(POLICY_KEY)).get(POLICY_KEY);
  }
}
