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
import java.util.List;
import java.util.UUID;
import java.util.function.Consumer;
import org.apache.helix.util.HelixUtil;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.helix.zookeeper.impl.client.ZkClient;
import org.apache.helix.zookeeper.zkclient.exception.ZkException;
import org.apache.pinot.common.metadata.ZKMetadataProvider;
import org.apache.pinot.common.utils.LogicalTableConfigUtils;
import org.apache.pinot.common.utils.config.TableConfigSerDeUtils;
import org.apache.pinot.common.utils.helix.ZkMultiWriteBuilder;
import org.apache.pinot.controller.api.exception.TableAlreadyExistsException;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.LogicalTableConfig;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/// Atomically publishes a physical or logical table config and checks the competing names in ZooKeeper.
/// Each invocation owns its transaction and can run concurrently with other publishers.
final class TableConfigPublisher {
  private static final Logger LOGGER = LoggerFactory.getLogger(TableConfigPublisher.class);

  private TableConfigPublisher() {
  }

  static void createPhysical(ZkClient client, String root, TableConfig tableConfig, boolean requireRawNameAbsent,
      Consumer<ZkMultiWriteBuilder> schemaChecks)
      throws IOException {
    String tableName = tableConfig.getTableName();
    String rawName = TableNameBuilder.extractRawTableName(tableName);
    String logicalPath = ZKMetadataProvider.constructPropertyStorePathForLogical(rawName);
    TableNameBuilder sibling =
        tableConfig.getTableType() == TableType.OFFLINE ? TableNameBuilder.REALTIME : TableNameBuilder.OFFLINE;
    List<String> competingPaths = requireRawNameAbsent
        ? List.of(logicalPath, physicalPath(sibling.tableNameWithType(rawName)))
        : List.of(logicalPath);
    publish(client, root, physicalPath(tableName), TableConfigSerDeUtils.toZNRecord(tableConfig), competingPaths,
        schemaChecks);
  }

  static void createLogical(ZkClient client, String root, LogicalTableConfig logicalTableConfig,
      Consumer<ZkMultiWriteBuilder> schemaChecks) {
    String tableName = logicalTableConfig.getTableName();
    List<String> competingPaths = List.of(physicalPath(TableNameBuilder.OFFLINE.tableNameWithType(tableName)),
        physicalPath(TableNameBuilder.REALTIME.tableNameWithType(tableName)));
    try {
      publish(client, root, ZKMetadataProvider.constructPropertyStorePathForLogical(tableName),
          LogicalTableConfigUtils.toZNRecord(logicalTableConfig), competingPaths, schemaChecks);
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to create logical table " + tableName, e);
    }
  }

  private static String physicalPath(String tableNameWithType) {
    return ZKMetadataProvider.constructPropertyStorePathForResourceConfig(tableNameWithType);
  }

  private static void publish(ZkClient client, String root, String configPath, ZNRecord config,
      List<String> competingPaths, Consumer<ZkMultiWriteBuilder> schemaChecks)
      throws IOException {
    String path = root + configPath;
    // A child of this incarnation of the config survives a full config update and disappears if the table is deleted.
    String receipt = path + "/create-" + UUID.randomUUID();
    boolean initializedParents = false;
    while (true) {
      ZkMultiWriteBuilder transaction = new ZkMultiWriteBuilder(client, "").create(path, config)
          .create(receipt, new ZNRecord("create"));
      for (String competing : competingPaths) {
        // ZooKeeper has no check-absent operation. The temporary node disappears in the same transaction.
        transaction.create(root + competing, new ZNRecord("absent")).delete(root + competing, 0);
      }
      schemaChecks.accept(transaction);
      try {
        transaction.execute();
        break;
      } catch (KeeperException | ZkException e) {
        // On a lost acknowledgement, Helix may return a connection error or replay the multi and see NODEEXISTS.
        // Only the invocation that created this config can have committed its receipt.
        if (client.exists(receipt)) {
          break;
        }
        if (e instanceof KeeperException.NodeExistsException) {
          throw new TableAlreadyExistsException("Table " + config.getId() + " already exists.", e);
        }
        if (initializedParents || !(e instanceof KeeperException.NoNodeException)) {
          throw new IOException("Failed to create table config for " + config.getId(), e);
        }
        // A new cluster or one with only legacy tables may have neither config parent in the primary store.
        client.createPersistent(HelixUtil.getZkParentPath(path), true);
        for (String competing : competingPaths) {
          client.createPersistent(HelixUtil.getZkParentPath(root + competing), true);
        }
        initializedParents = true;
      }
    }
    try {
      client.delete(receipt);
    } catch (ZkException e) {
      // The config and its guards have already committed. A leftover receipt is removed with the table.
      LOGGER.warn("Could not remove table creation receipt {}", receipt, e);
    }
  }
}
