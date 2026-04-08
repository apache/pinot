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
import java.util.Collections;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.helix.AccessOption;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.helix.zookeeper.zkclient.exception.ZkBadVersionException;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * ZooKeeper-backed persistence layer for {@link InsertStatementManifest} objects.
 *
 * <p>Manifests are stored as ZNRecords under the path
 * {@code /INSERT_STATEMENTS/{tableNameWithType}/{statementId}}. The manifest JSON is stored
 * in a simple field of the ZNRecord so that the full object can be round-tripped without loss.
 *
 * <p>This class follows the same property-store pattern used by
 * {@link org.apache.pinot.common.lineage.SegmentLineageAccessHelper}.
 *
 * <p>Thread-safety: individual read/write operations are atomic at the ZK level.
 * Higher-level read-modify-write sequences must be coordinated by the caller.
 */
public class InsertStatementStore {
  private static final Logger LOGGER = LoggerFactory.getLogger(InsertStatementStore.class);

  private static final String INSERT_STATEMENTS_PREFIX = "/INSERT_STATEMENTS";
  private static final String REQUEST_IDS_PREFIX = "/INSERT_REQUEST_IDS";
  private static final String MANIFEST_FIELD = "manifest";
  private static final String STATEMENT_ID_FIELD = "statementId";

  private final ZkHelixPropertyStore<ZNRecord> _propertyStore;

  public InsertStatementStore(ZkHelixPropertyStore<ZNRecord> propertyStore) {
    _propertyStore = propertyStore;
  }

  /**
   * Persists a new statement manifest in ZK. Fails if the node already exists.
   *
   * @return true if creation succeeded, false if the statement already exists
   */
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

  /**
   * Updates an existing manifest in ZK using optimistic concurrency (version check).
   *
   * @return true if the update succeeded, false on version conflict or other failure
   */
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
      LOGGER.warn("Version conflict updating insert statement: {}", manifest.getStatementId());
      return false;
    } catch (Exception e) {
      LOGGER.error("Failed to update insert statement manifest for statementId={}", manifest.getStatementId(), e);
      return false;
    }
  }

  /**
   * Reads a statement manifest from ZK.
   *
   * @return the manifest, or null if not found
   */
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
      return null;
    }
  }

  /**
   * Lists all statement manifests for a given table.
   *
   * @return list of manifests (never null; may be empty)
   */
  public List<InsertStatementManifest> listStatements(String tableNameWithType) {
    String parentPath = buildTablePath(tableNameWithType);
    try {
      List<String> children = _propertyStore.getChildNames(parentPath, AccessOption.PERSISTENT);
      if (children == null || children.isEmpty()) {
        return Collections.emptyList();
      }
      List<InsertStatementManifest> manifests = new ArrayList<>();
      for (String statementId : children) {
        InsertStatementManifest manifest = getStatement(tableNameWithType, statementId);
        if (manifest != null) {
          manifests.add(manifest);
        }
      }
      return manifests;
    } catch (Exception e) {
      LOGGER.error("Failed to list insert statements for table={}", tableNameWithType, e);
      return Collections.emptyList();
    }
  }

  /**
   * Deletes a statement manifest from ZK.
   *
   * @return true if deletion succeeded
   */
  public boolean deleteStatement(String tableNameWithType, String statementId) {
    String path = buildPath(tableNameWithType, statementId);
    try {
      return _propertyStore.remove(path, AccessOption.PERSISTENT);
    } catch (Exception e) {
      LOGGER.error("Failed to delete insert statement manifest for statementId={}", statementId, e);
      return false;
    }
  }

  /**
   * Atomically reserves a requestId for a given table. If the requestId is already reserved,
   * returns the existing statementId. Otherwise, creates a ZK node to reserve the mapping.
   *
   * <p>This uses ZK's atomic {@code create} to prevent two concurrent retries from both
   * creating statements for the same requestId.
   *
   * @param tableNameWithType the table name with type
   * @param requestId         the client-supplied request ID for idempotency
   * @param statementId       the statement ID to associate with this request
   * @return null if the reservation succeeded (this caller wins), or the existing statementId
   *         if already reserved by a prior request
   */
  @Nullable
  public String reserveRequestId(String tableNameWithType, String requestId, String statementId) {
    String path = REQUEST_IDS_PREFIX + "/" + tableNameWithType + "/" + requestId;
    try {
      ZNRecord record = new ZNRecord(requestId);
      record.setSimpleField(STATEMENT_ID_FIELD, statementId);
      boolean created = _propertyStore.create(path, record, AccessOption.PERSISTENT);
      if (created) {
        return null;  // This caller wins the reservation
      }
      // Node already exists — read the existing statementId
      ZNRecord existing = _propertyStore.get(path, null, AccessOption.PERSISTENT);
      if (existing != null) {
        return existing.getSimpleField(STATEMENT_ID_FIELD);
      }
      return null;
    } catch (Exception e) {
      // If create failed because node exists, read the existing entry
      try {
        ZNRecord existing = _propertyStore.get(path, null, AccessOption.PERSISTENT);
        if (existing != null) {
          return existing.getSimpleField(STATEMENT_ID_FIELD);
        }
      } catch (Exception readEx) {
        LOGGER.error("Failed to read existing requestId reservation for requestId={}", requestId, readEx);
      }
      LOGGER.error("Failed to reserve requestId={} for statementId={}", requestId, statementId, e);
      return null;
    }
  }

  /**
   * Removes a requestId reservation. Called when a statement is GC'd or aborted.
   */
  public void releaseRequestId(String tableNameWithType, String requestId) {
    if (requestId == null) {
      return;
    }
    String path = REQUEST_IDS_PREFIX + "/" + tableNameWithType + "/" + requestId;
    try {
      _propertyStore.remove(path, AccessOption.PERSISTENT);
    } catch (Exception e) {
      LOGGER.warn("Failed to release requestId reservation for requestId={}", requestId, e);
    }
  }

  /**
   * Finds a statement by requestId across all statements for a table.
   * Used for idempotency checks.
   *
   * @return the matching manifest, or null if not found
   */
  @Nullable
  public InsertStatementManifest findByRequestId(String tableNameWithType, String requestId) {
    // First check the atomic reservation index
    String path = REQUEST_IDS_PREFIX + "/" + tableNameWithType + "/" + requestId;
    try {
      ZNRecord record = _propertyStore.get(path, null, AccessOption.PERSISTENT);
      if (record != null) {
        String statementId = record.getSimpleField(STATEMENT_ID_FIELD);
        if (statementId != null) {
          InsertStatementManifest manifest = getStatement(tableNameWithType, statementId);
          if (manifest != null) {
            return manifest;
          }
        }
      }
    } catch (Exception e) {
      LOGGER.debug("Failed to lookup requestId reservation for requestId={}", requestId, e);
    }

    // Fallback: scan manifests (for backward compatibility with pre-existing manifests)
    List<InsertStatementManifest> manifests = listStatements(tableNameWithType);
    for (InsertStatementManifest manifest : manifests) {
      if (requestId.equals(manifest.getRequestId())) {
        return manifest;
      }
    }
    return null;
  }

  /**
   * Lists all table names that have at least one insert statement manifest in ZK.
   *
   * @return list of table names with type suffix (never null; may be empty)
   */
  public List<String> listTablesWithStatements() {
    try {
      List<String> children = _propertyStore.getChildNames(INSERT_STATEMENTS_PREFIX, AccessOption.PERSISTENT);
      if (children == null || children.isEmpty()) {
        return Collections.emptyList();
      }
      return children;
    } catch (Exception e) {
      LOGGER.error("Failed to list tables with insert statements", e);
      return Collections.emptyList();
    }
  }

  /**
   * Searches all tables for a statement with the given statementId.
   *
   * @return the manifest if found, or null
   */
  @Nullable
  public InsertStatementManifest findStatementAcrossTables(String statementId) {
    List<String> tables = listTablesWithStatements();
    for (String table : tables) {
      InsertStatementManifest manifest = getStatement(table, statementId);
      if (manifest != null) {
        return manifest;
      }
    }
    return null;
  }

  private static String buildTablePath(String tableNameWithType) {
    return INSERT_STATEMENTS_PREFIX + "/" + tableNameWithType;
  }

  private static String buildPath(String tableNameWithType, String statementId) {
    return INSERT_STATEMENTS_PREFIX + "/" + tableNameWithType + "/" + statementId;
  }

  private static ZNRecord toZNRecord(InsertStatementManifest manifest)
      throws IOException {
    ZNRecord record = new ZNRecord(manifest.getStatementId());
    record.setSimpleField(MANIFEST_FIELD, manifest.toJsonString());
    return record;
  }

  private static InsertStatementManifest fromZNRecord(ZNRecord record)
      throws IOException {
    String json = record.getSimpleField(MANIFEST_FIELD);
    return InsertStatementManifest.fromJsonString(json);
  }
}
