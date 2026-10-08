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
package org.apache.pinot.controller.helix.core.ingest;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.helix.AccessOption;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.helix.zookeeper.zkclient.exception.ZkBadVersionException;
import org.apache.pinot.common.metadata.ZKMetadataProvider;
import org.apache.pinot.spi.ingest.InsertRequest;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/// ZooKeeper storage for synchronous ROW insert manifests and create-only requestId reservations.
/// Reservations are retained for the table lifetime. Each accepted statement has one writer;
/// reads and writes are atomic in ZooKeeper. Instances are thread-safe.
public class InsertStatementStore {
  private static final Logger LOGGER = LoggerFactory.getLogger(InsertStatementStore.class);
  private static final String INSERT_STATEMENTS_PREFIX = ZKMetadataProvider.PROPERTYSTORE_INSERT_STATEMENTS_PREFIX;
  private static final String REQUEST_IDS_PREFIX = ZKMetadataProvider.PROPERTYSTORE_INSERT_REQUEST_IDS_PREFIX;
  private static final String MANIFEST_FIELD = "manifest";
  private static final String STATEMENT_ID_FIELD = "statementId";
  private final ZkHelixPropertyStore<ZNRecord> _propertyStore;

  public InsertStatementStore(ZkHelixPropertyStore<ZNRecord> propertyStore) {
    _propertyStore = propertyStore;
  }

  /// Persists a new statement manifest in ZK. Fails if the node already exists.
  ///
  /// @return true if creation succeeded, false if the statement already exists
  public boolean createStatement(InsertStatementManifest manifest) {
    String path = buildPath(manifest.getTableNameWithType(), manifest.getStatementId());
    try {
      ZNRecord record = toZNRecord(manifest);
      return _propertyStore.create(path, record, AccessOption.PERSISTENT);
    } catch (Exception e) {
      LOGGER.error("Failed to create insert statement manifest for statementId={}", manifest.getStatementId(), e);
      return false;
    }
  }

  /// Updates an existing manifest in ZK using optimistic concurrency (version check).
  ///
  /// @return true if the update succeeded, false on version conflict or other failure
  public boolean updateStatement(InsertStatementManifest manifest) {
    String path = buildPath(manifest.getTableNameWithType(), manifest.getStatementId());
    try {
      Stat stat = new Stat();
      ZNRecord existing = _propertyStore.get(path, stat, AccessOption.PERSISTENT);
      if (existing == null) {
        LOGGER.warn("Cannot update non-existent insert statement: {}", manifest.getStatementId());
        return false;
      }
      ZNRecord record = toZNRecord(manifest);
      return _propertyStore.set(path, record, stat.getVersion(), AccessOption.PERSISTENT);
    } catch (ZkBadVersionException e) {
      /// Version conflicts are expected under concurrent CAS retries; caller retries. Log at DEBUG
      /// to avoid log spam in production. Hard failures still log WARN/ERROR below.
      LOGGER.debug("Version conflict updating insert statement: {}", manifest.getStatementId());
      return false;
    } catch (Exception e) {
      LOGGER.error("Failed to update insert statement manifest for statementId={}", manifest.getStatementId(), e);
      return false;
    }
  }

  /// Reads a statement manifest from ZK.
  ///
  /// @return the manifest, or null if not found
  @Nullable
  public InsertStatementManifest getStatement(String tableNameWithType, String statementId) {
    String path = buildPath(tableNameWithType, statementId);
    try {
      ZNRecord record = _propertyStore.get(path, null, AccessOption.PERSISTENT);
      if (record == null) {
        return null;
      }
      return fromZNRecord(record);
    } catch (Exception e) {
      LOGGER.error("Failed to read insert statement manifest for statementId={}", statementId, e);
      throw new RuntimeException("Cannot read insert statement " + statementId, e);
    }
  }

  /// Lists the persisted results; read failures are surfaced instead of returning an incomplete list.
  public List<InsertStatementManifest> listStatements(String tableNameWithType) {
    try {
      List<ZNRecord> records = _propertyStore.getChildren(buildTablePath(tableNameWithType), null,
          AccessOption.PERSISTENT, 0, 0);
      if (records == null) {
        return List.of();
      }
      List<InsertStatementManifest> manifests = new ArrayList<>(records.size());
      for (ZNRecord record : records) {
        if (record != null) {
          manifests.add(fromZNRecord(record));
        }
      }
      return manifests;
    } catch (Exception e) {
      throw new RuntimeException("Cannot list insert statements for " + tableNameWithType, e);
    }
  }

  /// Atomically reserves a requestId for a given table. If the requestId is already reserved,
  /// returns the existing statementId. Otherwise, creates a ZK node to reserve the mapping.
  ///
  /// This uses ZK's atomic `create` to prevent two concurrent retries from both
  /// creating statements for the same requestId. If the reservation cannot be reliably
  /// determined (ZK failure), throws an exception to fail closed rather than allowing
  /// duplicate executions.
  ///
  /// @param tableNameWithType the table name with type
  /// @param requestId         the client-supplied request ID for idempotency
  /// @param statementId       the statement ID to associate with this request
  /// @return null if the reservation succeeded (this caller wins), or the existing statementId
  ///         if already reserved by a prior request
  /// @throws RuntimeException if the reservation state cannot be determined due to ZK failure
  @Nullable
  public String reserveRequestId(String tableNameWithType, String requestId, String statementId) {
    String path = buildRequestIdPath(tableNameWithType, requestId);
    try {
      ZNRecord record = new ZNRecord(requestId);
      record.setSimpleField(STATEMENT_ID_FIELD, statementId);
      boolean created = _propertyStore.create(path, record, AccessOption.PERSISTENT);
      if (created) {
        return null;  /// This caller wins the reservation
      }
      ZNRecord existing = _propertyStore.get(path, null, AccessOption.PERSISTENT);
      if (existing != null) {
        return requireStatementId(existing);
      }
      /// create returned false but node not readable — fail closed
      throw new RuntimeException("RequestId reservation in indeterminate state for requestId=" + requestId);
    } catch (RuntimeException e) {
      throw e;
    } catch (Exception e) {
      /// ZK create failed — try to read to distinguish "already exists" from "ZK down"
      try {
        ZNRecord existing = _propertyStore.get(path, null, AccessOption.PERSISTENT);
        if (existing != null) {
          return requireStatementId(existing);
        }
      } catch (Exception readEx) {
        LOGGER.error("Failed to read existing requestId reservation for requestId={}", requestId, readEx);
      }
      /// Cannot determine reservation state — fail closed to prevent duplicate execution
      throw new RuntimeException(
          "Failed to reserve requestId=" + requestId + " for statementId=" + statementId
              + "; failing closed to prevent duplicate execution", e);
    }
  }

  private static String requireStatementId(ZNRecord record) {
    String statementId = record.getSimpleField(STATEMENT_ID_FIELD);
    if (statementId == null || !InsertRequest.ID_PATTERN.matcher(statementId).matches()) {
      throw new IllegalStateException("Malformed requestId reservation: " + record.getId());
    }
    return statementId;
  }

  private static String buildTablePath(String tableNameWithType) {
    return INSERT_STATEMENTS_PREFIX + "/" + tableNameWithType;
  }

  private static String buildPath(String tableNameWithType, String statementId) {
    return INSERT_STATEMENTS_PREFIX + "/" + tableNameWithType + "/" + validateIdForPath("statementId", statementId);
  }

  private static String buildRequestIdPath(String tableNameWithType, String requestId) {
    return REQUEST_IDS_PREFIX + "/" + tableNameWithType + "/" + validateIdForPath("requestId", requestId);
  }

  /// Defense-in-depth for ids that become znode names. [InsertRequest] already rejects ids that
  /// don't match [InsertRequest#ID_PATTERN] at construction, but this store is also called by
  /// internal callers; a stray '/' here would create nested znodes
  /// that break getChildren-based listing and pruning, and could collide with another caller's
  /// subtree.
  private static String validateIdForPath(String name, String id) {
    if (id == null || !InsertRequest.ID_PATTERN.matcher(id).matches()) {
      throw new IllegalArgumentException(
          name + " must match " + InsertRequest.ID_PATTERN.pattern() + "; got: " + id);
    }
    return id;
  }

  private static ZNRecord toZNRecord(InsertStatementManifest manifest) throws IOException {
    ZNRecord record = new ZNRecord(manifest.getStatementId());
    record.setSimpleField(MANIFEST_FIELD, manifest.toJsonString());
    return record;
  }

  private static InsertStatementManifest fromZNRecord(ZNRecord record) throws IOException {
    String json = record.getSimpleField(MANIFEST_FIELD);
    if (json == null) {
      throw new IOException("Manifest record is missing 'manifest': " + record.getId());
    }
    return InsertStatementManifest.fromJsonString(json);
  }
}
