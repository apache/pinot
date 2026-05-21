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
package org.apache.pinot.materializedview.rewrite;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import javax.annotation.Nullable;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.helix.AccessOption;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.helix.zookeeper.zkclient.IZkChildListener;
import org.apache.helix.zookeeper.zkclient.IZkDataListener;
import org.apache.pinot.common.metadata.ZKMetadataProvider;
import org.apache.pinot.common.request.Expression;
import org.apache.pinot.common.request.PinotQuery;
import org.apache.pinot.materializedview.metadata.MaterializedViewDefinitionMetadata;
import org.apache.pinot.materializedview.metadata.MaterializedViewDefinitionMetadata.MaterializedViewSplitSpec;
import org.apache.pinot.materializedview.metadata.MaterializedViewRuntimeMetadata;
import org.apache.pinot.materializedview.metadata.PartitionInfo;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.apache.pinot.sql.parsers.CalciteSqlParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/// Broker-side cache that maintains a reverse index from base table names to their
/// materialized view entries. Subscribes to two ZK paths:
///
///   - `/CONFIGS/MATERIALIZED_VIEW/DEFINITION` — low-frequency changes trigger SQL recompilation
///   - `/CONFIGS/MATERIALIZED_VIEW/RUNTIME` — high-frequency changes only update mutable
///       runtime state (watermarkMs, partitions map) without any SQL parsing
///
///
/// Thread-safety: all mutations go through synchronized ZK listener callbacks;
/// reads use a [ConcurrentHashMap] and are lock-free.
public class MaterializedViewMetadataCache {
  private static final Logger LOGGER = LoggerFactory.getLogger(MaterializedViewMetadataCache.class);

  private static final String MATERIALIZED_VIEW_DEFINITION_PARENT_PATH =
      ZKMetadataProvider.getPropertyStorePathForMaterializedViewDefinitionPrefix();
  private static final String MATERIALIZED_VIEW_DEFINITION_PATH_PREFIX = MATERIALIZED_VIEW_DEFINITION_PARENT_PATH + "/";
  private static final String MATERIALIZED_VIEW_RUNTIME_PARENT_PATH =
      ZKMetadataProvider.getPropertyStorePathForMaterializedViewRuntimePrefix();
  private static final String MATERIALIZED_VIEW_RUNTIME_PATH_PREFIX = MATERIALIZED_VIEW_RUNTIME_PARENT_PATH + "/";

  private final ZkHelixPropertyStore<ZNRecord> _propertyStore;
  private final ZkDefinitionListener _definitionListener = new ZkDefinitionListener();
  private final ZkRuntimeListener _runtimeListener = new ZkRuntimeListener();
  // Shared lock so that definition and runtime listener callbacks are mutually exclusive,
  // preventing a concurrent putDefinitionEntry from overwriting a runtime update that arrived
  // between the _materializedViewEntryMap.get and _materializedViewEntryMap.put in putDefinitionEntry.
  private final Object _cacheLock = new Object();

  private final Map<String, MaterializedViewCacheEntry> _materializedViewEntryMap = new ConcurrentHashMap<>();
  /// Holds runtime znode payloads that arrive BEFORE the matching definition.  ZK does not
  /// guarantee ordering between sibling child-change notifications, and on broker startup the
  /// runtime znode listener may fire ahead of the definition znode listener.  Dropping the
  /// runtime update on the floor would leave the MV in a cold-start state until the next
  /// scheduler tick re-publishes the runtime znode.  Pending entries are consumed (and removed)
  /// when [#putDefinitionEntry] eventually constructs the cache entry.
  ///
  /// All reads and writes of this map happen inside `synchronized (_cacheLock)`, so a plain
  /// HashMap is sufficient — using ConcurrentHashMap here would imply lock-free reads which the
  /// rest of the code does not rely on.
  private final Map<String, MaterializedViewRuntimeMetadata> _pendingRuntimeStates = new HashMap<>();
  private final Map<String, List<MaterializedViewCacheEntry>> _baseTableToMaterializedViewMap =
      new ConcurrentHashMap<>();

  public MaterializedViewMetadataCache(ZkHelixPropertyStore<ZNRecord> propertyStore) {
    _propertyStore = propertyStore;

    synchronized (_cacheLock) {
      _propertyStore.subscribeChildChanges(MATERIALIZED_VIEW_DEFINITION_PARENT_PATH, _definitionListener);
      _propertyStore.subscribeChildChanges(MATERIALIZED_VIEW_RUNTIME_PARENT_PATH, _runtimeListener);

      List<String> defChildren =
          _propertyStore.getChildNames(MATERIALIZED_VIEW_DEFINITION_PARENT_PATH, AccessOption.PERSISTENT);
      if (CollectionUtils.isNotEmpty(defChildren)) {
        List<String> defPaths = new ArrayList<>(defChildren.size());
        List<String> rtPaths = new ArrayList<>(defChildren.size());
        for (String viewTableName : defChildren) {
          defPaths.add(MATERIALIZED_VIEW_DEFINITION_PATH_PREFIX + viewTableName);
          rtPaths.add(MATERIALIZED_VIEW_RUNTIME_PATH_PREFIX + viewTableName);
        }
        addDefinitions(defPaths);
        loadRuntimeStates(rtPaths);
      }
    }

    LOGGER.info("Initialized MaterializedViewMetadataCache with {} materialized view entries",
        _materializedViewEntryMap.size());
  }

  @Nullable
  public List<MaterializedViewCacheEntry> getMaterializedViewEntriesForBaseTable(String rawBaseTableName) {
    return _baseTableToMaterializedViewMap.get(rawBaseTableName);
  }

  /// Removes all MV cache entries that reference the given raw base table name. Called when a base
  /// table is deleted so stale reverse-index entries do not survive to contaminate queries against
  /// a newly created table with the same name.
  ///
  /// Every MV that lists `rawBaseTableName` among its base tables is evicted, regardless of whether
  /// the MV references a single base table or multiple. The drop+recreate pattern (operator drops
  /// `tableA`, recreates with a new schema) would otherwise leave the broker holding a stale
  /// `compiledQuery` that targets a column shape that no longer matches the new `tableA`, leading
  /// to silent wrong results. The MV's definition ZNode listener will repopulate the cache only
  /// when an authoritative new definition is published.
  public void invalidateBaseTable(String rawBaseTableName) {
    // Hold _cacheLock across the entire invalidate so that the (unsubscribe + map
    // mutation) operation is atomic with respect to concurrent ZK listener callbacks
    // (which also synchronize on _cacheLock). The Helix ZkHelixPropertyStore's
    // subscribe/unsubscribe methods only mutate internal listener maps under their
    // own short critical sections; they do not call back into user code, so there is
    // no lock-ordering cycle with _cacheLock. Splitting the work across two critical
    // sections (with unsubscribe outside the lock) reintroduces races where a
    // concurrent putDefinitionEntry between the unsubscribe and the second lock
    // acquisition would leave a fresh entry with no live data subscription.
    synchronized (_cacheLock) {
      // Evict every MV that lists `rawBaseTableName` among its base tables.
      List<MaterializedViewCacheEntry> entries = _baseTableToMaterializedViewMap.get(rawBaseTableName);
      if (entries != null) {
        for (MaterializedViewCacheEntry entry : new ArrayList<>(entries)) {
          removeDefinitionEntry(
              MATERIALIZED_VIEW_DEFINITION_PATH_PREFIX + entry.getMaterializedViewTableNameWithType());
        }
      }
      // Also evict any MV cache entry whose own tableNameWithType starts with `rawBaseTableName_`.
      // The broker resource state model fires invalidate{Base,}Table on every table that goes
      // ONLINE→OFFLINE/DROPPED, including MV tables themselves (MVs are OFFLINE Pinot tables in
      // Helix). Without this, a dropped MV remains in `_materializedViewEntryMap` until the
      // definition znode listener fires; during the gap the broker may compile queries against
      // the now-offline MV.
      //
      // `removeDefinitionEntry` unsubscribes BOTH the definition and the paired runtime data
      // listeners (see implementation), so a definition znode that hasn't been deleted in ZK
      // will not auto-resubscribe.  The parent-path child-change listener stays subscribed —
      // that's required to detect new MV creations and is correct.  Operator guidance: drop
      // the MV definition znode before taking the MV table OFFLINE in Helix to avoid the brief
      // QUERY_REWRITE_EXCEPTIONS noise between the OFFLINE transition and the znode delete.
      String offlineMvName = TableNameBuilder.OFFLINE.tableNameWithType(rawBaseTableName);
      if (_materializedViewEntryMap.containsKey(offlineMvName)) {
        removeDefinitionEntry(MATERIALIZED_VIEW_DEFINITION_PATH_PREFIX + offlineMvName);
      }
    }
  }

  /// Number of MV entries currently held in the cache.  Surfaced so the broker can expose this
  /// as a gauge for unbounded-growth detection — a cluster with K MVs should plateau near K;
  /// sustained growth signals a leak in the ZK listener / drop path.
  public int size() {
    return _materializedViewEntryMap.size();
  }

  /// Rebuilds cache entries that were evicted during an earlier ONLINE→OFFLINE transition for the
  /// given table.  Called from the broker resource state model on OFFLINE→ONLINE transitions so a
  /// previous cycle (or a transient broker-resource rebalance) does not leave this broker
  /// permanently unable to consider an MV until the definition znode is republished.
  ///
  /// Two complementary scenarios are handled because the broker resource state model fires for
  /// BOTH base tables and MV tables — and `invalidateBaseTable` evicts cache entries in both
  /// cases (an MV evicted because its base table cycled is the more common one):
  ///
  ///   1. **MV table cycled** — the transitioning table IS the MV. Look up its own definition
  ///      znode under `<rawTableName>_OFFLINE` and reload it.
  ///   2. **Base table cycled** — the transitioning table is a base referenced by one or more
  ///      MVs.  Walk every MV definition znode currently in ZK, decode its base-table list, and
  ///      reload any whose base tables include either `rawTableName_OFFLINE` or
  ///      `rawTableName_REALTIME` and that are missing from the in-memory cache.  Without this
  ///      path, a base-table OFFLINE→ONLINE bounce permanently silences MV rewrite on this broker.
  ///
  /// Idempotent: entries already in the cache are skipped; absent definition znodes (genuine
  /// drop) are also skipped.  Subscribes the matching runtime listener + rebuilds runtime state
  /// alongside the definition.
  public void refreshTable(String rawTableName) {
    String offlineMvName = TableNameBuilder.OFFLINE.tableNameWithType(rawTableName);
    String offlineMvDefPath = MATERIALIZED_VIEW_DEFINITION_PATH_PREFIX + offlineMvName;
    synchronized (_cacheLock) {
      // Case 1: the transitioning table is an MV — direct rehydrate of its own entry.
      if (!_materializedViewEntryMap.containsKey(offlineMvName)
          && _propertyStore.exists(offlineMvDefPath, AccessOption.PERSISTENT)) {
        addDefinitions(List.of(offlineMvDefPath));
        String offlineMvRuntimePath = MATERIALIZED_VIEW_RUNTIME_PATH_PREFIX + offlineMvName;
        if (_propertyStore.exists(offlineMvRuntimePath, AccessOption.PERSISTENT)) {
          loadRuntimeStates(List.of(offlineMvRuntimePath));
        }
      }

      // Case 2: the transitioning table may be a BASE table referenced by other MVs whose
      // entries were evicted during the matching OFFLINE invalidate.  Walk every MV definition
      // child znode (a low-frequency operation, only on resource state transitions) and reload
      // any whose base-table list mentions this table and whose in-memory entry is gone.
      List<String> defChildren =
          _propertyStore.getChildNames(MATERIALIZED_VIEW_DEFINITION_PARENT_PATH, AccessOption.PERSISTENT);
      if (CollectionUtils.isEmpty(defChildren)) {
        return;
      }
      // baseTables list stores the raw base-table name (no type suffix); compare against the raw
      // form of the transitioning table. We also accept typed siblings ("<raw>_OFFLINE" /
      // "<raw>_REALTIME") so a definition that was created with a typed name (e.g. older format
      // or operator typo) is still picked up by the refresh.
      String rawSibling = TableNameBuilder.extractRawTableName(rawTableName);
      String offlineSibling = TableNameBuilder.OFFLINE.tableNameWithType(rawSibling);
      String realtimeSibling = TableNameBuilder.REALTIME.tableNameWithType(rawSibling);
      List<String> defPathsToReload = new ArrayList<>();
      List<String> runtimePathsToReload = new ArrayList<>();
      for (String mvViewTableName : defChildren) {
        if (_materializedViewEntryMap.containsKey(mvViewTableName)) {
          continue;
        }
        String defPath = MATERIALIZED_VIEW_DEFINITION_PATH_PREFIX + mvViewTableName;
        ZNRecord znRecord = _propertyStore.get(defPath, null, AccessOption.PERSISTENT);
        if (znRecord == null) {
          continue;
        }
        MaterializedViewDefinitionMetadata definition = MaterializedViewDefinitionMetadata.fromZNRecord(znRecord);
        List<String> baseTables = definition.getBaseTables();
        if (baseTables == null) {
          continue;
        }
        if (baseTables.contains(rawSibling)
            || baseTables.contains(offlineSibling)
            || baseTables.contains(realtimeSibling)) {
          defPathsToReload.add(defPath);
          runtimePathsToReload.add(MATERIALIZED_VIEW_RUNTIME_PATH_PREFIX + mvViewTableName);
        }
      }
      if (!defPathsToReload.isEmpty()) {
        addDefinitions(defPathsToReload);
        // loadRuntimeStates already tolerates missing znodes (the get returns null and the entry
        // stays at cold-start until the runtime listener fires the first update).
        loadRuntimeStates(runtimePathsToReload);
      }
    }
  }

  // -----------------------------------------------------------------------
  //  Definition path handling (low frequency — triggers SQL recompilation)
  // -----------------------------------------------------------------------

  private void addDefinitions(List<String> paths) {
    for (String path : paths) {
      _propertyStore.subscribeDataChanges(path, _definitionListener);
    }
    List<ZNRecord> znRecords = _propertyStore.get(paths, null, AccessOption.PERSISTENT);
    for (ZNRecord znRecord : znRecords) {
      if (znRecord != null) {
        try {
          putDefinitionEntry(znRecord);
        } catch (Exception e) {
          LOGGER.error("Failed to add MV definition for ZNRecord: {}", znRecord.getId(), e);
        }
      }
    }
  }

  private void putDefinitionEntry(ZNRecord znRecord) {
    MaterializedViewDefinitionMetadata definition = MaterializedViewDefinitionMetadata.fromZNRecord(znRecord);
    String viewTableNameWithType = definition.getMaterializedViewTableNameWithType();

    PinotQuery compiledQuery = null;
    String definedSql = definition.getDefinedSql();
    if (definedSql != null && !definedSql.isEmpty()) {
      try {
        compiledQuery = CalciteSqlParser.compileToPinotQuery(definedSql);
      } catch (Exception e) {
        // ERROR (not WARN): a null compiledQuery silently disables MV rewrite for this view —
        // every match attempt returns null without logging. Operators must be alerted at
        // ingestion time. The cache entry is still stored (with compiledQuery=null) so the
        // controller-facing definition listing remains visible; only rewrite is disabled.
        LOGGER.error("Failed to compile definedSql for MV {}; rewrite disabled until the SQL is "
            + "fixed and the definition znode is updated. SQL: {}", viewTableNameWithType, definedSql, e);
      }
    }

    MaterializedViewCacheEntry existing = _materializedViewEntryMap.get(viewTableNameWithType);
    MaterializedViewCacheEntry newEntry;
    if (existing != null) {
      newEntry = new MaterializedViewCacheEntry(definition, compiledQuery,
          existing.getWatermarkMs(), existing.getPartitions());
    } else {
      // Honor a runtime znode that arrived ahead of this definition znode (ZK does not
      // guarantee sibling-listener ordering).  Without this, the new entry would start at
      // (watermarkMs=0, partitions=Map.of()) and the broker would treat the MV as cold until
      // the next scheduler tick re-publishes the runtime znode.
      MaterializedViewRuntimeMetadata pending = _pendingRuntimeStates.remove(viewTableNameWithType);
      if (pending != null) {
        newEntry = new MaterializedViewCacheEntry(definition, compiledQuery,
            pending.getWatermarkMs(), pending.getPartitions());
      } else {
        newEntry = new MaterializedViewCacheEntry(definition, compiledQuery, 0L, Map.of());
      }
    }

    MaterializedViewCacheEntry oldEntry = _materializedViewEntryMap.put(viewTableNameWithType, newEntry);
    if (oldEntry != null) {
      removeFromReverseIndex(oldEntry);
    }
    addToReverseIndex(newEntry);
  }

  private void removeDefinitionEntry(String path) {
    _propertyStore.unsubscribeDataChanges(path, _definitionListener);
    String viewTableNameWithType = path.substring(MATERIALIZED_VIEW_DEFINITION_PATH_PREFIX.length());
    // Also unsubscribe the paired runtime listener and drop any pending runtime payload — an MV
    // drop leaves no consumer for either, and the runtime znode may linger if the operator only
    // deleted the definition.  Without this, every dropped MV leaks one Helix watcher slot.
    _propertyStore.unsubscribeDataChanges(MATERIALIZED_VIEW_RUNTIME_PATH_PREFIX + viewTableNameWithType,
        _runtimeListener);
    _pendingRuntimeStates.remove(viewTableNameWithType);
    MaterializedViewCacheEntry removed = _materializedViewEntryMap.remove(viewTableNameWithType);
    if (removed != null) {
      removeFromReverseIndex(removed);
    }
  }

  // -----------------------------------------------------------------------
  //  Runtime path handling (high frequency — only updates scalar fields)
  // -----------------------------------------------------------------------

  private void loadRuntimeStates(List<String> paths) {
    for (String path : paths) {
      _propertyStore.subscribeDataChanges(path, _runtimeListener);
    }
    List<ZNRecord> znRecords = _propertyStore.get(paths, null, AccessOption.PERSISTENT);
    for (ZNRecord znRecord : znRecords) {
      if (znRecord != null) {
        try {
          updateRuntimeState(znRecord);
        } catch (Exception e) {
          LOGGER.error("Failed to load MV runtime for ZNRecord: {}", znRecord.getId(), e);
        }
      }
    }
  }

  private void updateRuntimeState(ZNRecord znRecord) {
    String viewTableNameWithType = znRecord.getId();
    MaterializedViewRuntimeMetadata runtime = MaterializedViewRuntimeMetadata.fromZNRecord(znRecord);
    MaterializedViewCacheEntry entry = _materializedViewEntryMap.get(viewTableNameWithType);
    if (entry == null) {
      // Definition znode listener may fire after the runtime znode listener; buffer this update
      // until the matching definition arrives via [#putDefinitionEntry].  See _pendingRuntimeStates.
      LOGGER.debug("Buffering runtime update for MV pending definition load: {}", viewTableNameWithType);
      _pendingRuntimeStates.put(viewTableNameWithType, runtime);
      return;
    }
    entry.setRuntimeState(runtime.getWatermarkMs(), runtime.getPartitions());
  }

  // -----------------------------------------------------------------------
  //  Reverse index management
  // -----------------------------------------------------------------------

  private void addToReverseIndex(MaterializedViewCacheEntry entry) {
    String mvTableName = entry.getDefinition().getMaterializedViewTableNameWithType();
    for (String baseTable : entry.getDefinition().getBaseTables()) {
      _baseTableToMaterializedViewMap.compute(baseTable, (key, existing) -> {
        // Always return an unmodifiableList — the broker query path iterates without copying, so a
        // mutable singleton would race with a concurrent compute() that copies-and-replaces.
        // Dedupe on viewTableName so a duplicate add (e.g. cold-start initial-load racing with
        // a queued listener callback for the same MV) does not produce a doubled candidate that
        // the rewrite engine would evaluate twice per query.
        if (existing == null) {
          return List.of(entry);
        }
        for (MaterializedViewCacheEntry existingEntry : existing) {
          if (mvTableName.equals(existingEntry.getDefinition().getMaterializedViewTableNameWithType())) {
            // Replace in place so the latest entry wins (covers definition-update flows).
            List<MaterializedViewCacheEntry> updated = new ArrayList<>(existing);
            updated.replaceAll(e -> mvTableName.equals(e.getDefinition().getMaterializedViewTableNameWithType())
                ? entry : e);
            return Collections.unmodifiableList(updated);
          }
        }
        List<MaterializedViewCacheEntry> updated = new ArrayList<>(existing);
        updated.add(entry);
        return Collections.unmodifiableList(updated);
      });
    }
  }

  private void removeFromReverseIndex(MaterializedViewCacheEntry entry) {
    for (String baseTable : entry.getDefinition().getBaseTables()) {
      _baseTableToMaterializedViewMap.compute(baseTable, (key, existing) -> {
        if (existing == null) {
          return null;
        }
        List<MaterializedViewCacheEntry> updated = new ArrayList<>(existing);
        updated.removeIf(e -> e.getDefinition().getMaterializedViewTableNameWithType()
            .equals(entry.getDefinition().getMaterializedViewTableNameWithType()));
        return updated.isEmpty() ? null : Collections.unmodifiableList(updated);
      });
    }
  }

  // -----------------------------------------------------------------------
  //  ZK listeners
  // -----------------------------------------------------------------------

  private class ZkDefinitionListener implements IZkChildListener, IZkDataListener {
    @Override
    public void handleChildChange(String path, List<String> children) {
      synchronized (_cacheLock) {
        Set<String> newChildSet = CollectionUtils.isNotEmpty(children)
            ? new HashSet<>(children) : Collections.emptySet();

        // Evict entries that are no longer present in ZK (MV table deleted).
        for (String existing : new ArrayList<>(_materializedViewEntryMap.keySet())) {
          if (!newChildSet.contains(existing)) {
            removeDefinitionEntry(MATERIALIZED_VIEW_DEFINITION_PATH_PREFIX + existing);
          }
        }

        // Register newly added entries.
        List<String> defPathsToAdd = new ArrayList<>();
        List<String> rtPathsToAdd = new ArrayList<>();
        for (String viewTableName : newChildSet) {
          if (!_materializedViewEntryMap.containsKey(viewTableName)) {
            defPathsToAdd.add(MATERIALIZED_VIEW_DEFINITION_PATH_PREFIX + viewTableName);
            rtPathsToAdd.add(MATERIALIZED_VIEW_RUNTIME_PATH_PREFIX + viewTableName);
          }
        }
        if (!defPathsToAdd.isEmpty()) {
          addDefinitions(defPathsToAdd);
          loadRuntimeStates(rtPathsToAdd);
        }
      }
    }

    @Override
    public void handleDataChange(String path, Object data) {
      synchronized (_cacheLock) {
        if (data != null) {
          try {
            putDefinitionEntry((ZNRecord) data);
          } catch (Exception e) {
            LOGGER.error("Failed to refresh MV definition for: {}", path, e);
          }
        }
      }
    }

    @Override
    public void handleDataDeleted(String path) {
      synchronized (_cacheLock) {
        removeDefinitionEntry(path);
      }
    }
  }

  private class ZkRuntimeListener implements IZkChildListener, IZkDataListener {
    @Override
    public void handleChildChange(String path, List<String> children) {
      synchronized (_cacheLock) {
        Set<String> newChildSet = CollectionUtils.isNotEmpty(children)
            ? new HashSet<>(children) : Collections.emptySet();

        // Unsubscribe data listeners for runtime nodes that are no longer present.
        for (String viewTableName : _materializedViewEntryMap.keySet()) {
          if (!newChildSet.contains(viewTableName)) {
            markRuntimeStateMissing(viewTableName);
            _propertyStore.unsubscribeDataChanges(MATERIALIZED_VIEW_RUNTIME_PATH_PREFIX + viewTableName,
                _runtimeListener);
          }
        }
        // Evict pending runtime payloads whose runtime znode has disappeared without a matching
        // definition ever arriving — they would otherwise leak indefinitely.
        _pendingRuntimeStates.keySet().removeIf(key -> !newChildSet.contains(key));

        // Subscribe for any newly visible runtime nodes.
        List<String> rtPathsToAdd = new ArrayList<>();
        for (String viewTableName : newChildSet) {
          if (_materializedViewEntryMap.containsKey(viewTableName)) {
            rtPathsToAdd.add(MATERIALIZED_VIEW_RUNTIME_PATH_PREFIX + viewTableName);
          }
        }
        if (!rtPathsToAdd.isEmpty()) {
          loadRuntimeStates(rtPathsToAdd);
        }
      }
    }

    @Override
    public void handleDataChange(String path, Object data) {
      synchronized (_cacheLock) {
        if (data != null) {
          try {
            updateRuntimeState((ZNRecord) data);
          } catch (Exception e) {
            LOGGER.error("Failed to refresh MV runtime for: {}", path, e);
          }
        }
      }
    }

    @Override
    public void handleDataDeleted(String path) {
      synchronized (_cacheLock) {
        String viewTableNameWithType = path.substring(MATERIALIZED_VIEW_RUNTIME_PATH_PREFIX.length());
        markRuntimeStateMissing(viewTableNameWithType);
        // Drop any pending payload too — an orphan runtime znode (no matching definition ever
        // arrived) leaves an entry in _pendingRuntimeStates that would otherwise live forever.
        _pendingRuntimeStates.remove(viewTableNameWithType);
        _propertyStore.unsubscribeDataChanges(path, _runtimeListener);
      }
    }
  }

  private void markRuntimeStateMissing(String viewTableNameWithType) {
    MaterializedViewCacheEntry entry = _materializedViewEntryMap.get(viewTableNameWithType);
    if (entry != null) {
      entry.setRuntimeState(0L, Map.of());
    }
  }

  // -----------------------------------------------------------------------
  //  Cache entry
  // -----------------------------------------------------------------------

  /// A cached entry holding the MV definition (with pre-compiled [PinotQuery]) and a
  /// volatile atomic reference to runtime state (`watermarkMs` + per-partition map).
  ///
  /// Runtime state is published through a single `volatile` field so readers always
  /// see a consistent `(watermarkMs, partitions)` pair — never a mix of values from
  /// different ZK updates.
  ///
  /// Thread-safety: ZK listener mutations are synchronized on the cache lock.  Reads
  /// of the reverse index and entry map are unsynchronized; a transient window during
  /// definition updates may show both old and new entries for the same base table, but
  /// this is safe (worst case: a duplicate candidate is evaluated and discarded).
  public static class MaterializedViewCacheEntry {
    private final MaterializedViewDefinitionMetadata _definition;
    private final PinotQuery _compiledQuery;
    /// Cached projection map (alias-stripped expr → MV column name) computed once at entry
    /// construction. Empty map when `_compiledQuery` is null. The map is unmodifiable so it
    /// can be shared across query threads without copying.
    private final Map<Expression, String> _viewProjectionMap;
    /// Cached flattened-AND conjunct set of the MV's WHERE clause. Empty set when the MV
    /// has no filter or `_compiledQuery` is null. Unmodifiable for safe cross-thread sharing.
    private final Set<Expression> _viewFilterConjuncts;

    /// Immutable pair published atomically.
    private volatile RuntimeState _runtimeState;

    public MaterializedViewCacheEntry(MaterializedViewDefinitionMetadata definition,
        @Nullable PinotQuery compiledQuery, long watermarkMs, Map<Long, PartitionInfo> partitions) {
      _definition = definition;
      _compiledQuery = compiledQuery;
      _runtimeState = new RuntimeState(watermarkMs, partitions);
      if (compiledQuery == null) {
        _viewProjectionMap = Map.of();
        _viewFilterConjuncts = Set.of();
      } else {
        _viewProjectionMap =
            Collections.unmodifiableMap(MaterializedViewMatchUtils.buildMaterializedViewProjectionMap(compiledQuery));
        Expression viewFilter = compiledQuery.getFilterExpression();
        _viewFilterConjuncts = viewFilter == null
            ? Set.of()
            : Collections.unmodifiableSet(MaterializedViewMatchUtils.flattenAnd(viewFilter));
      }
    }

    public MaterializedViewDefinitionMetadata getDefinition() {
      return _definition;
    }

    @Nullable
    public PinotQuery getCompiledQuery() {
      return _compiledQuery;
    }

    public Map<Expression, String> getViewProjectionMap() {
      return _viewProjectionMap;
    }

    public Set<Expression> getViewFilterConjuncts() {
      return _viewFilterConjuncts;
    }

    /// Scheduling watermark.  Under Design C the broker uses this as the split point for
    /// the 2-way SPLIT_REWRITE (MV-side: `time < watermarkMs`; base-side: `time >= watermarkMs`).
    /// Per-bucket eligibility is checked against `getPartitions()` for finer routing in V2.
    public long getWatermarkMs() {
      return _runtimeState._watermarkMs;
    }

    /// Immutable snapshot of `bucketStart → PartitionInfo`.  A bucket's absence means the MV
    /// does not cover that time range.
    public Map<Long, PartitionInfo> getPartitions() {
      return _runtimeState._partitions;
    }

    /// Atomically replaces both runtime scalars.
    void setRuntimeState(long watermarkMs, Map<Long, PartitionInfo> partitions) {
      _runtimeState = new RuntimeState(watermarkMs, partitions);
    }

    private static final class RuntimeState {
      final long _watermarkMs;
      final Map<Long, PartitionInfo> _partitions;

      RuntimeState(long watermarkMs, Map<Long, PartitionInfo> partitions) {
        _watermarkMs = watermarkMs;
        _partitions = partitions.isEmpty() ? Map.of() : Map.copyOf(partitions);
      }
    }

    @Nullable
    public MaterializedViewSplitSpec getSplitSpec() {
      return _definition.getSplitSpec();
    }

    public String getMaterializedViewTableNameWithType() {
      return _definition.getMaterializedViewTableNameWithType();
    }
  }
}
