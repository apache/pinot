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

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.Map;
import java.util.TreeMap;
import org.apache.helix.AccessOption;
import org.apache.helix.BaseDataAccessor;
import org.apache.helix.HelixAdmin;
import org.apache.helix.HelixManager;
import org.apache.helix.InstanceType;
import org.apache.helix.PropertyKey;
import org.apache.helix.manager.zk.ZKHelixAdmin;
import org.apache.helix.manager.zk.ZKHelixManager;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState;
import org.apache.helix.zookeeper.api.client.RealmAwareZkClient;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.helix.zookeeper.datamodel.serializer.ZNRecordSerializer;
import org.apache.helix.zookeeper.impl.client.ZkClient;
import org.apache.pinot.common.utils.ZkStarter;
import org.apache.pinot.common.utils.helix.ImmutableSortedArrayMap;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.CommonConstants.Broker;
import org.apache.pinot.spi.utils.CommonConstants.Helix;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;


/// Checks that [CompactAssignmentZkHelixManager] installs
/// [org.apache.pinot.common.utils.helix.CompactZNRecordSerializer] on the client that Helix creates in `connect()`, for
/// table ideal states and external views only.
///
/// This test fails when a Helix upgrade changes how [ZKHelixManager] creates its ZooKeeper client or its serializer,
/// for example by renaming the protected `_zkclient` field (compile error) or by creating the client outside of
/// `connect()` (the records read through the manager would no longer be compact).
public class CompactAssignmentZkHelixManagerTest {
  private static final String CLUSTER_NAME = "CompactAssignmentZkHelixManagerTest";
  private static final String TABLE_NAME = "myTable_OFFLINE";

  private ZkStarter.ZookeeperInstance _zookeeperInstance;
  private ZkClient _zkClient;

  @BeforeClass
  public void setUp() {
    _zookeeperInstance = ZkStarter.startLocalZkServer();
    String zkUrl = _zookeeperInstance.getZkUrl();
    HelixAdmin helixAdmin = new ZKHelixAdmin.Builder().setZkAddress(zkUrl).build();
    try {
      helixAdmin.addCluster(CLUSTER_NAME);
    } finally {
      helixAdmin.close();
    }
    _zkClient = new ZkClient.Builder().setZkServer(zkUrl).setZkSerializer(new ZNRecordSerializer()).build();
    PropertyKey.Builder keyBuilder = new PropertyKey.Builder(CLUSTER_NAME);
    _zkClient.createPersistent(keyBuilder.idealStates(TABLE_NAME).getPath(), assignment(TABLE_NAME));
    _zkClient.createPersistent(keyBuilder.externalView(TABLE_NAME).getPath(), assignment(TABLE_NAME));
    _zkClient.createPersistent(keyBuilder.idealStates(Helix.BROKER_RESOURCE_INSTANCE).getPath(),
        assignment(Helix.BROKER_RESOURCE_INSTANCE));
    _zkClient.createPersistent(keyBuilder.externalView(Helix.BROKER_RESOURCE_INSTANCE).getPath(),
        assignment(Helix.BROKER_RESOURCE_INSTANCE));
  }

  @AfterClass
  public void tearDown() {
    if (_zkClient != null) {
      _zkClient.close();
    }
    ZkStarter.stopLocalZkServer(_zookeeperInstance);
  }

  private static ZNRecord assignment(String id) {
    ZNRecord record = new ZNRecord(id);
    // Compressed, because the compact serializer only parses gzip-compressed znodes itself
    record.setBooleanField(ZNRecord.ENABLE_COMPRESSION_BOOLEAN_FIELD, true);
    record.setSimpleField("REBALANCE_MODE", "CUSTOMIZED");
    record.setMapField("segment_1", Map.of("Server_b_8098", "ONLINE", "Server_a_8098", "ONLINE"));
    record.setMapField("segment_0", Map.of("Server_b_8098", "ONLINE", "Server_a_8098", "ONLINE"));
    return record;
  }

  @Test
  public void testZkClientFieldIsProtected()
      throws NoSuchFieldException {
    Field field = ZKHelixManager.class.getDeclaredField("_zkclient");
    assertTrue(Modifier.isProtected(field.getModifiers()));
    assertEquals(field.getType(), RealmAwareZkClient.class);
  }

  @Test
  public void testFactoryHonorsFlag() {
    String zkUrl = _zookeeperInstance.getZkUrl();
    HelixManager defaultManager =
        CompactAssignmentZkHelixManager.createSpectatorHelixManager(new PinotConfiguration(), CLUSTER_NAME,
            "Broker_localhost_1", zkUrl);
    assertFalse(defaultManager instanceof CompactAssignmentZkHelixManager);
    PinotConfiguration conf = new PinotConfiguration();
    conf.setProperty(Broker.CONFIG_OF_ROUTING_COMPACT_ASSIGNMENT_READER_ENABLED, true);
    HelixManager compactManager =
        CompactAssignmentZkHelixManager.createSpectatorHelixManager(conf, CLUSTER_NAME, "Broker_localhost_1", zkUrl);
    assertTrue(compactManager instanceof CompactAssignmentZkHelixManager);
  }

  @Test
  public void testReadsThroughManagerAreCompact()
      throws Exception {
    CompactAssignmentZkHelixManager manager =
        new CompactAssignmentZkHelixManager(CLUSTER_NAME, "Broker_localhost_2", InstanceType.SPECTATOR,
            _zookeeperInstance.getZkUrl());
    manager.connect();
    try {
      assertReads(manager);
      // A new connect() creates a new client; the serializer must be installed on it again
      manager.disconnect();
      manager.connect();
      assertReads(manager);
    } finally {
      manager.disconnect();
    }
  }

  private static void assertReads(CompactAssignmentZkHelixManager manager) {
    assertNotNull(manager.getInstalledZkSerializer());
    BaseDataAccessor<ZNRecord> accessor = manager.getHelixDataAccessor().getBaseDataAccessor();
    PropertyKey.Builder keyBuilder = new PropertyKey.Builder(CLUSTER_NAME);

    // Table ideal state and external view: compact, equal to the written content, inner maps shared
    for (String path : new String[]{
        keyBuilder.idealStates(TABLE_NAME).getPath(), keyBuilder.externalView(TABLE_NAME).getPath()
    }) {
      ZNRecord record = accessor.get(path, null, AccessOption.PERSISTENT);
      assertTrue(record.getMapFields() instanceof ImmutableSortedArrayMap, path);
      assertEquals(record, assignment(TABLE_NAME));
      assertSame(record.getMapField("segment_0"), record.getMapField("segment_1"));
    }

    // Broker resource: default deserializer
    for (String path : new String[]{
        keyBuilder.idealStates(Helix.BROKER_RESOURCE_INSTANCE).getPath(),
        keyBuilder.externalView(Helix.BROKER_RESOURCE_INSTANCE).getPath()
    }) {
      ZNRecord record = accessor.get(path, null, AccessOption.PERSISTENT);
      assertFalse(record.getMapFields() instanceof ImmutableSortedArrayMap, path);
      assertEquals(record, assignment(Helix.BROKER_RESOURCE_INSTANCE));
    }

    // Readers through the data accessor and the admin of the manager see the compact records too
    IdealState idealState = manager.getHelixDataAccessor().getProperty(keyBuilder.idealStates(TABLE_NAME));
    assertTrue(idealState.getInstanceStateMap("segment_0") instanceof ImmutableSortedArrayMap);
    ExternalView externalView = manager.getClusterManagmentTool().getResourceExternalView(CLUSTER_NAME, TABLE_NAME);
    assertTrue(externalView.getStateMap("segment_0") instanceof ImmutableSortedArrayMap);
    assertEquals(new TreeMap<>(externalView.getRecord().getMapFields()), assignment(TABLE_NAME).getMapFields());
    ExternalView brokerResource =
        manager.getClusterManagmentTool().getResourceExternalView(CLUSTER_NAME, Helix.BROKER_RESOURCE_INSTANCE);
    assertFalse(brokerResource.getStateMap("segment_0") instanceof ImmutableSortedArrayMap);
  }
}
