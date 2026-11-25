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
import java.io.UncheckedIOException;
import java.util.Map;
import java.util.function.Consumer;
import org.apache.helix.zookeeper.datamodel.serializer.ZNRecordSerializer;
import org.apache.helix.zookeeper.impl.client.ZkClient;
import org.apache.helix.zookeeper.zkclient.exception.ZkException;
import org.apache.pinot.common.metadata.ZKMetadataProvider;
import org.apache.pinot.common.utils.ZkStarter;
import org.apache.pinot.common.utils.config.SchemaSerDeUtils;
import org.apache.pinot.common.utils.config.TableConfigSerDeUtils;
import org.apache.pinot.common.utils.helix.ZkMultiWriteBuilder;
import org.apache.pinot.controller.api.exception.TableAlreadyExistsException;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.LogicalTableConfig;
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
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;


/// Exercises the atomic name checks and recovery from a replayed creation transaction against ZooKeeper.
public class TableConfigPublisherTest {
  private static final String ROOT = "/publisher/PROPERTYSTORE";
  private ZkStarter.ZookeeperInstance _zk;
  private ZkClient _client;

  @BeforeClass
  public void startZk() {
    _zk = ZkStarter.startLocalZkServer();
    _client = new ZkClient.Builder().setZkServer(_zk.getZkUrl()).setZkSerializer(new ZNRecordSerializer()).build();
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
  public Object[][] lostAcknowledgement() {
    return new Object[][]{{true}, {false}};
  }

  @Test(dataProvider = "lostAcknowledgement")
  public void testRecognizesOwnCreateAfterLostAcknowledgement(boolean replay)
      throws Exception {
    TableConfig config = new TableConfigBuilder(TableType.OFFLINE).setTableName("ackLost" + replay).build();
    Consumer<ZkMultiWriteBuilder> schemaCheck = createSchema("ackLost" + replay);
    String path = ROOT + ZKMetadataProvider.constructPropertyStorePathForResourceConfig(config.getTableName());
    ZkClient replaying = mock(ZkClient.class, delegatesTo(_client));
    doAnswer(invocation -> {
      Iterable<Op> ops = invocation.getArgument(0);
      _client.multi(ops);
      // The commit succeeded but its acknowledgement was lost. Another request can update the entire config while
      // the publisher reconnects, before Helix retries exactly the same multi.
      config.setDescription("updated");
      _client.writeData(path, TableConfigSerDeUtils.toZNRecord(config));
      if (replay) {
        return _client.multi(ops);
      }
      throw new ZkException(new KeeperException.ConnectionLossException());
    }).when(replaying).multi(any());

    TableConfigPublisher.createPhysical(replaying, ROOT, config, true, schemaCheck);

    assertEquals(TableConfigSerDeUtils.fromZNRecord(_client.readData(path)).getDescription(), "updated");
    assertTrue(_client.getChildren(path).isEmpty());
    assertThrows(TableAlreadyExistsException.class,
        () -> TableConfigPublisher.createPhysical(_client, ROOT, config, true, schemaCheck));
  }

  @Test
  public void testCompetingPhysicalAndLogicalCreates()
      throws Exception {
    LogicalTableConfig logical = new LogicalTableConfig();
    logical.setBrokerTenant("DefaultTenant");
    logical.setPhysicalTableConfigMap(Map.of());
    logical.setTableName("logicalFirst");
    Consumer<ZkMultiWriteBuilder> logicalSchema = createSchema(logical.getTableName());
    TableConfigPublisher.createLogical(_client, ROOT, logical, logicalSchema);
    for (TableType type : TableType.values()) {
      TableConfig table = new TableConfigBuilder(type).setTableName("logicalFirst").build();
      assertThrows(TableAlreadyExistsException.class,
          () -> TableConfigPublisher.createPhysical(_client, ROOT, table, false, logicalSchema));
      assertFalse(_client.exists(ROOT + ZKMetadataProvider.constructPropertyStorePathForResourceConfig(
          table.getTableName())));
    }

    for (TableType type : TableType.values()) {
      String rawName = "physicalFirst" + type;
      TableConfig table = new TableConfigBuilder(type).setTableName(rawName).build();
      Consumer<ZkMultiWriteBuilder> schemaCheck = createSchema(rawName);
      TableConfigPublisher.createPhysical(_client, ROOT, table, true, schemaCheck);
      logical.setTableName(rawName);
      assertThrows(TableAlreadyExistsException.class,
          () -> TableConfigPublisher.createLogical(_client, ROOT, logical, schemaCheck));
      assertFalse(_client.exists(ROOT + ZKMetadataProvider.constructPropertyStorePathForLogical(rawName)));

      TableType siblingType = type == TableType.OFFLINE ? TableType.REALTIME : TableType.OFFLINE;
      TableConfig sibling = new TableConfigBuilder(siblingType).setTableName(rawName).build();
      assertThrows(TableAlreadyExistsException.class,
          () -> TableConfigPublisher.createPhysical(_client, ROOT, sibling, true, schemaCheck));
      TableConfigPublisher.createPhysical(_client, ROOT, sibling, false, schemaCheck);
    }
  }

  @Test
  public void testSchemaDeletedBeforeOrdinaryPublication()
      throws Exception {
    String name = "schemaDeletedFirst";
    Consumer<ZkMultiWriteBuilder> check = createSchema(name);
    assertTrue(SchemaPublicationFence.deleteIfUnused(_client, "publisher", name,
        _client.getStat(ROOT + ZKMetadataProvider.constructPropertyStorePathForSchema(name))));
    TableConfig physical = new TableConfigBuilder(TableType.REALTIME).setTableName(name).build();
    assertThrows(IOException.class, () -> TableConfigPublisher.createPhysical(_client, ROOT, physical, false, check));
    assertFalse(_client.exists(ROOT + ZKMetadataProvider.constructPropertyStorePathForResourceConfig(
        physical.getTableName())));
    LogicalTableConfig logical = new LogicalTableConfig();
    logical.setTableName(name);
    logical.setBrokerTenant("DefaultTenant");
    logical.setPhysicalTableConfigMap(Map.of());
    assertThrows(UncheckedIOException.class, () -> TableConfigPublisher.createLogical(_client, ROOT, logical, check));
    assertFalse(_client.exists(ROOT + ZKMetadataProvider.constructPropertyStorePathForLogical(name)));
  }

  private Consumer<ZkMultiWriteBuilder> createSchema(String name) {
    String path = ROOT + ZKMetadataProvider.constructPropertyStorePathForSchema(name);
    _client.createPersistent(ROOT + "/SCHEMAS", true);
    _client.createPersistent(path, SchemaSerDeUtils.toZNRecord(new Schema.SchemaBuilder().setSchemaName(name).build()));
    return transaction -> transaction.check(path, ZkMultiWriteBuilder.ANY_VERSION);
  }
}
