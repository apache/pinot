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
import java.util.List;
import java.util.UUID;
import javax.annotation.Nullable;
import org.apache.helix.PropertyPathBuilder;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.helix.zookeeper.impl.client.ZkClient;
import org.apache.helix.zookeeper.zkclient.exception.ZkException;
import org.apache.helix.zookeeper.zkclient.exception.ZkNoNodeException;
import org.apache.helix.zookeeper.zkclient.exception.ZkNodeExistsException;
import org.apache.helix.zookeeper.zkclient.exception.ZkTimeoutException;
import org.apache.pinot.common.metadata.ZKMetadataProvider;
import org.apache.pinot.common.utils.config.SchemaSerDeUtils;
import org.apache.pinot.common.utils.helix.ZkMultiWriteBuilder;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/// Pins the identity and data version of a schema until its first table config is published.
/// A znode version alone cannot distinguish a delete followed by a create at version zero.
/// Each operation holds its own ephemeral child, which cannot survive deletion of its parent.
final class SchemaPublicationFence implements AutoCloseable {
  private static final Logger LOGGER = LoggerFactory.getLogger(SchemaPublicationFence.class);

  private final ZkClient _client;
  private final String _primaryPath;
  private final String _schemaPath;
  private final String _marker;
  private final Stat _stat = new Stat();

  private SchemaPublicationFence(ZkClient client, String primaryPath, String schemaPath) {
    _client = client;
    _primaryPath = primaryPath;
    _schemaPath = schemaPath;
    _marker = _schemaPath + "/claim-" + UUID.randomUUID();
    try {
      client.createEphemeral(_marker);
    } catch (ZkNodeExistsException e) {
      // Helix can replay the create after losing its acknowledgement. This UUID belongs to this request.
    } catch (ZkException e) {
      // If the acknowledgement itself timed out, a confirmed marker is still ours to release.
      boolean ambiguous = e instanceof ZkTimeoutException
          || e.getCause() instanceof KeeperException.ConnectionLossException
          || e.getCause() instanceof KeeperException.OperationTimeoutException;
      if (!ambiguous || !client.exists(_marker)) {
        throw e;
      }
    }
  }

  static SchemaPublicationFence acquire(ZkClient client, String clusterName, Schema expected,
      @Nullable Stat effectiveStat) throws IOException {
    String name = expected.getSchemaName();
    String path = ZKMetadataProvider.constructPropertyStorePathForSchema(name);
    SchemaPublicationFence fence;
    try {
      fence = new SchemaPublicationFence(client, PropertyPathBuilder.propertyStore(clusterName) + path,
          schemaPath(client, clusterName, name, effectiveStat));
    } catch (ZkNoNodeException e) {
      throw schemaChanged(name);
    }
    try {
      ZNRecord record = client.readData(fence._schemaPath, fence._stat);
      if (record == null) {
        throw new IOException("Missing schema record at " + fence._schemaPath);
      }
      if (effectiveStat.getCzxid() != fence._stat.getCzxid()
          || !expected.equals(SchemaSerDeUtils.fromZNRecord(record))) {
        throw schemaChanged(name);
      }
      return fence;
    } catch (ZkNoNodeException e) {
      fence.close();
      throw schemaChanged(name);
    } catch (IOException | RuntimeException e) {
      fence.close();
      throw e;
    }
  }

  void addChecks(ZkMultiWriteBuilder transaction) {
    transaction.check(_marker, 0).check(_schemaPath, _stat.getVersion());
    if (!_schemaPath.equals(_primaryPath)) {
      // AutoFallbackPropertyStore reads the primary in preference to the legacy schema.
      checkAbsent(_client, transaction, _primaryPath);
    }
  }

  // Use the schema the manager's property store actually selected. A legacy root created after Helix initialized
  // its AutoFallbackPropertyStore is invisible to that controller.
  static String schemaPath(ZkClient client, String clusterName, String name, @Nullable Stat effectiveStat) {
    if (effectiveStat != null) {
      String path = ZKMetadataProvider.constructPropertyStorePathForSchema(name);
      for (String root : List.of(PropertyPathBuilder.propertyStore(clusterName), legacyStore(clusterName))) {
        Stat stat = client.getStat(root + path);
        if (stat != null && stat.getCzxid() == effectiveStat.getCzxid()) {
          return root + path;
        }
      }
    }
    throw schemaChanged(name);
  }

  static ConcurrentModificationException schemaChanged(String name) {
    return new ConcurrentModificationException("Schema '" + name + "' changed concurrently");
  }

  static boolean deleteIfUnused(ZkClient client, String clusterName, String name, @Nullable Stat effectiveStat) {
    if (effectiveStat == null) {
      return false;
    }
    String primary = PropertyPathBuilder.propertyStore(clusterName);
    String legacy = legacyStore(clusterName);
    String path = ZKMetadataProvider.constructPropertyStorePathForSchema(name);
    String effectivePath = schemaPath(client, clusterName, name, effectiveStat);
    try (SchemaPublicationFence fence = new SchemaPublicationFence(client, primary + path, effectivePath)) {
      Stat current = client.getStat(effectivePath);
      if (current == null || current.getCzxid() != effectiveStat.getCzxid()) {
        throw schemaChanged(name);
      }
      for (int attempt = 0; ; attempt++) {
        // Deleting the private marker first makes a replay fail before touching a replacement schema.
        ZkMultiWriteBuilder transaction = new ZkMultiWriteBuilder(client, "").delete(fence._marker, 0);
        for (String root : List.of(primary, legacy)) {
          if (!client.exists(root)) {
            checkAbsent(client, transaction, root);
            continue;
          }
          for (String table : List.of(TableNameBuilder.OFFLINE.tableNameWithType(name),
              TableNameBuilder.REALTIME.tableNameWithType(name))) {
            String tablePath = ZKMetadataProvider.constructPropertyStorePathForResourceConfig(table);
            checkAbsent(client, transaction, root + tablePath);
          }
          checkAbsent(client, transaction, root + ZKMetadataProvider.constructPropertyStorePathForLogical(name));
          Stat stat = client.getStat(root + path);
          if (stat == null) {
            checkAbsent(client, transaction, root + path);
          } else {
            transaction.delete(root + path, stat.getVersion());
          }
        }
        try {
          transaction.execute();
          return true;
        } catch (KeeperException | ZkException e) {
          // A lost acknowledgement can surface directly or after Helix replays the delete.
          if (!client.exists(fence._marker)) {
            current = client.getStat(effectivePath);
            if (current == null || current.getCzxid() != effectiveStat.getCzxid()) {
              return true;
            }
          } else if (attempt == 0 && e instanceof KeeperException.NodeExistsException) {
            // An unrelated first table can have initialized one of the previously absent parents.
            continue;
          }
          if (e instanceof KeeperException.NodeExistsException || e instanceof KeeperException.NotEmptyException
              || e instanceof KeeperException.BadVersionException || e instanceof KeeperException.NoNodeException) {
            throw new ConcurrentModificationException(
                "Cannot delete schema " + name + " while it is in use or being modified", e);
          }
          throw new IllegalStateException("Failed to delete schema: " + name, e);
        }
      }
    } catch (ZkNoNodeException e) {
      return false;
    }
  }

  private static String legacyStore(String clusterName) {
    return "/" + clusterName + "/HELIX_PROPERTYSTORE";
  }

  // ZooKeeper has no check-for-absence op. If parents are missing, assert absence of the first missing ancestor.
  // The transient node is created and deleted within one multi, including on legacy-only clusters.
  private static void checkAbsent(ZkClient client, ZkMultiWriteBuilder transaction, String path) {
    String parent = path.substring(0, path.lastIndexOf('/'));
    while (!parent.isEmpty() && !client.exists(parent)) {
      path = parent;
      parent = path.substring(0, path.lastIndexOf('/'));
    }
    transaction.create(path, new ZNRecord("absent")).delete(path, 0);
  }

  @Override
  public void close() {
    try {
      _client.delete(_marker);
    } catch (RuntimeException e) {
      // A successful config transaction must not become a failed table create if marker cleanup loses connectivity.
      // The controller's ZooKeeper session also removes the ephemeral marker on expiry.
      LOGGER.warn("Failed to release ephemeral schema claim {}", _marker, e);
    }
  }
}
