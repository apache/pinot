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
package org.apache.pinot.common.utils.config;

import java.util.Map;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.helix.zookeeper.datamodel.serializer.ZNRecordSerializer;
import org.apache.helix.zookeeper.impl.client.ZkClient;
import org.apache.pinot.common.config.DefaultClusterConfigChangeHandler;
import org.apache.pinot.common.evaluator.GroovyFunctionEvaluator;
import org.apache.pinot.common.utils.ServiceStartableUtils;
import org.apache.pinot.common.utils.ZkStarter;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.services.ServiceRole;
import org.apache.pinot.spi.utils.CommonConstants.Groovy;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.expectThrows;


/// Verifies startup and live cluster snapshots against the actual Groovy compiler gate.
/// Not thread-safe because the compiler switch is process-wide.
@Test(singleThreaded = true)
public class IngestionGroovyConfigListenerTest {
  private static final String KEY = Groovy.DISABLE_INGESTION_GROOVY;
  private static final String EXPRESSION = "Groovy({value + 1}, value)";

  @AfterMethod(alwaysRun = true)
  public void restoreCompilation() {
    GroovyFunctionEvaluator.setIngestionGroovyDisabled(false);
  }

  @Test
  public void testLiveUpdatesAndRemoval() {
    DefaultClusterConfigChangeHandler handler = new DefaultClusterConfigChangeHandler();
    handler.registerClusterConfigChangeListener(new IngestionGroovyConfigListener());
    expectThrows(IllegalStateException.class, () -> new GroovyFunctionEvaluator(EXPRESSION));
    assertEquals(GroovyFunctionEvaluator.forQuery(EXPRESSION).evaluate(new Object[]{1}), 2);

    ClusterConfig config = new ClusterConfig("test");
    config.getRecord().setSimpleField(KEY, " false ");
    handler.onClusterConfigChange(config, null);
    assertEquals(new GroovyFunctionEvaluator(EXPRESSION).evaluate(new Object[]{1}), 2);

    config.getRecord().setSimpleField(KEY, "true");
    handler.onClusterConfigChange(config, null);
    expectThrows(IllegalStateException.class, () -> new GroovyFunctionEvaluator(EXPRESSION));

    config.getRecord().setSimpleField(KEY, "false");
    handler.onClusterConfigChange(config, null);
    config.getRecord().getSimpleFields().remove(KEY);
    handler.onClusterConfigChange(config, null);
    expectThrows(IllegalStateException.class, () -> GroovyFunctionEvaluator.parseGroovyScript(EXPRESSION));
  }

  @Test
  public void testFirstEmptySnapshotAndInvalidValueDisableCompilation() {
    DefaultClusterConfigChangeHandler handler = new DefaultClusterConfigChangeHandler();
    handler.registerClusterConfigChangeListener(new IngestionGroovyConfigListener());
    // The startup read enabled compilation, but the key was deleted before the first Helix callback.
    IngestionGroovyConfigListener.applyClusterConfig(Map.of(KEY, "false"));
    handler.onClusterConfigChange(new ClusterConfig("test"), null);
    expectThrows(IllegalStateException.class, () -> new GroovyFunctionEvaluator(EXPRESSION));

    IngestionGroovyConfigListener.applyClusterConfig(Map.of(KEY, "false"));
    IngestionGroovyConfigListener.applyClusterConfig(Map.of(KEY, "invalid"));
    expectThrows(IllegalStateException.class, () -> new GroovyFunctionEvaluator(EXPRESSION));
  }

  @Test
  public void testStartupUsesRawClusterConfigOnlyForRuntimeRoles() {
    ZkStarter.ZookeeperInstance zk = ZkStarter.startLocalZkServer();
    ZkClient client = new ZkClient.Builder().setZkServer(zk.getZkUrl())
        .setZkSerializer(new ZNRecordSerializer()).build();
    try {
      String cluster = "groovy-policy-test";
      String path = "/" + cluster + "/CONFIGS/CLUSTER/" + cluster;
      client.createPersistent(path, true);
      ZNRecord record = new ZNRecord(cluster);
      record.setSimpleField(KEY, "false");
      client.writeData(path, record);
      PinotConfiguration instance = new PinotConfiguration(Map.of(KEY, "true"));
      ServiceStartableUtils.applyClusterConfig(instance, zk.getZkUrl(), cluster, ServiceRole.SERVER);
      assertEquals(new GroovyFunctionEvaluator(EXPRESSION).evaluate(new Object[]{1}), 2);

      record.getSimpleFields().remove(KEY);
      client.writeData(path, record);
      instance.setProperty(KEY, "false");
      ServiceStartableUtils.applyClusterConfig(instance, zk.getZkUrl(), cluster, ServiceRole.MINION);
      expectThrows(IllegalStateException.class, () -> new GroovyFunctionEvaluator(EXPRESSION));

      GroovyFunctionEvaluator.setIngestionGroovyDisabled(false);
      ServiceStartableUtils.applyClusterConfig(instance, zk.getZkUrl(), cluster, ServiceRole.CONTROLLER);
      assertEquals(new GroovyFunctionEvaluator(EXPRESSION).evaluate(new Object[]{1}), 2);
      ServiceStartableUtils.applyClusterConfig(instance, zk.getZkUrl(), "missing-cluster", ServiceRole.SERVER);
      expectThrows(IllegalStateException.class, () -> new GroovyFunctionEvaluator(EXPRESSION));
    } finally {
      client.close();
      ZkStarter.stopLocalZkServer(zk);
    }
  }
}
