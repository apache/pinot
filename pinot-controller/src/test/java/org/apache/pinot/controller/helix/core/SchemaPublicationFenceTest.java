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
package org.apache.pinot.controller.helix.core;

import java.io.IOException;
import java.util.ConcurrentModificationException;
import org.apache.helix.zookeeper.datamodel.serializer.ZNRecordSerializer;
import org.apache.helix.zookeeper.impl.client.ZkClient;
import org.apache.helix.zookeeper.zkclient.exception.ZkException;
import org.apache.pinot.common.metadata.ZKMetadataProvider;
import org.apache.pinot.common.utils.ZkStarter;
import org.apache.pinot.common.utils.config.SchemaSerDeUtils;
import org.apache.pinot.controller.api.exception.TableAlreadyExistsException;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Op;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.mockito.AdditionalAnswers.delegatesTo;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;


/// Exercises schema changes between admission and publication against a real ZooKeeper.
public class SchemaPublicationFenceTest {
  private static final String CLUSTER = "schemaFence";
  private static final String PRIMARY = "/" + CLUSTER + "/PROPERTYSTORE";
  private static final String LEGACY = "/" + CLUSTER + "/HELIX_PROPERTYSTORE";

  private ZkStarter.ZookeeperInstance _zk;
  private ZkClient _client;

  @BeforeClass
  public void startZk() {
    _zk = ZkStarter.startLocalZkServer();
    _client = new ZkClient.Builder().setZkServer(_zk.getZkUrl()).setZkSerializer(new ZNRecordSerializer()).build();
    _client.createPersistent(PRIMARY + "/SCHEMAS", true);
    _client.createPersistent(LEGACY + "/SCHEMAS", true);
  }

  @AfterClass(alwaysRun = true)
  public void stopZk() {
    if (_client != null) {
      _client.close();
    }
    if (_zk != null) {
      ZkStarter.stopLocalZkServer(_zk);
    }
  }

  @DataProvider
  public Object[][] recreate() {
    return new Object[][]{{false}, {true}};
  }

  @Test(dataProvider = "recreate")
  public void testChangedSchemaCannotPublish(boolean recreate)
      throws Exception {
    String name = "changed" + recreate;
    String path = PRIMARY + ZKMetadataProvider.constructPropertyStorePathForSchema(name);
    Schema expected = schema(name, "before");
    _client.createPersistent(path, SchemaSerDeUtils.toZNRecord(expected));
    TableConfig table = new TableConfigBuilder(TableType.OFFLINE).setTableName(name).build();
    try (SchemaPublicationFence fence =
        SchemaPublicationFence.acquire(_client, CLUSTER, expected, _client.getStat(path))) {
      Schema changed = schema(name, "after");
      if (recreate) {
        _client.deleteRecursively(path);
        // The replacement and original both have data version zero.
        _client.createPersistent(path, SchemaSerDeUtils.toZNRecord(changed));
      } else {
        _client.writeData(path, SchemaSerDeUtils.toZNRecord(changed));
      }
      assertThrows(IOException.class, () -> TableConfigPublisher.createPhysical(_client, PRIMARY, table, true,
          fence::addChecks));
      assertFalse(_client.exists(PRIMARY + ZKMetadataProvider.constructPropertyStorePathForResourceConfig(
          table.getTableName())));
    }
  }

  @Test
  public void testLegacySchemaShadowedByPrimary()
      throws Exception {
    String name = "legacyShadow";
    String path = ZKMetadataProvider.constructPropertyStorePathForSchema(name);
    Schema expected = schema(name, "legacy");
    _client.createPersistent(LEGACY + path, SchemaSerDeUtils.toZNRecord(expected));
    TableConfig table = new TableConfigBuilder(TableType.OFFLINE).setTableName(name).build();
    try (SchemaPublicationFence fence =
        SchemaPublicationFence.acquire(_client, CLUSTER, expected, _client.getStat(LEGACY + path))) {
      _client.createPersistent(PRIMARY + path, SchemaSerDeUtils.toZNRecord(schema(name, "primary")));
      assertThrows(TableAlreadyExistsException.class, () -> TableConfigPublisher.createPhysical(_client, PRIMARY, table,
          true, fence::addChecks));
    }
    assertTrue(SchemaPublicationFence.deleteIfUnused(_client, CLUSTER, name, _client.getStat(PRIMARY + path)));
    assertFalse(_client.exists(PRIMARY + path));
    assertFalse(_client.exists(LEGACY + path));
  }

  @Test
  public void testDeleteRejectsPendingAndPublishedTable()
      throws Exception {
    String name = "pendingCreate";
    Schema expected = schema(name, "dimension");
    String path = ZKMetadataProvider.constructPropertyStorePathForSchema(name);
    _client.createPersistent(LEGACY + path, SchemaSerDeUtils.toZNRecord(expected));
    TableConfig table = new TableConfigBuilder(TableType.OFFLINE).setTableName(name).build();
    try (SchemaPublicationFence fence =
        SchemaPublicationFence.acquire(_client, CLUSTER, expected, _client.getStat(LEGACY + path))) {
      assertThrows(ConcurrentModificationException.class,
          () -> SchemaPublicationFence.deleteIfUnused(_client, CLUSTER, name, _client.getStat(LEGACY + path)));
      TableConfigPublisher.createPhysical(_client, PRIMARY, table, true, fence::addChecks);
    }
    // No IdealState has been created. The config alone protects its schema.
    assertThrows(ConcurrentModificationException.class,
        () -> SchemaPublicationFence.deleteIfUnused(_client, CLUSTER, name, _client.getStat(LEGACY + path)));
    assertTrue(_client.exists(LEGACY + path));
  }

  @Test
  public void testLostDeleteAcknowledgementPreservesReplacement()
      throws Exception {
    String name = "deleteAck";
    String path = PRIMARY + ZKMetadataProvider.constructPropertyStorePathForSchema(name);
    _client.createPersistent(path, SchemaSerDeUtils.toZNRecord(schema(name, "original")));
    Schema replacement = schema(name, "replacement");
    ZkClient disconnected = mock(ZkClient.class, delegatesTo(_client));
    doAnswer(invocation -> {
      String marker = invocation.getArgument(0);
      _client.createEphemeral(marker);
      throw new ZkException(new KeeperException.ConnectionLossException());
    }).when(disconnected).createEphemeral(anyString());
    doAnswer(invocation -> {
      Iterable<Op> ops = invocation.getArgument(0);
      _client.multi(ops);
      _client.createPersistent(path, SchemaSerDeUtils.toZNRecord(replacement));
      throw new ZkException(new KeeperException.ConnectionLossException());
    }).when(disconnected).multi(any());

    assertTrue(SchemaPublicationFence.deleteIfUnused(disconnected, CLUSTER, name, _client.getStat(path)));
    assertEquals(SchemaSerDeUtils.fromZNRecord(_client.readData(path)), replacement);
  }

  private static Schema schema(String name, String dimension) {
    return new Schema.SchemaBuilder().setSchemaName(name).addSingleValueDimension(dimension, DataType.STRING).build();
  }
}
