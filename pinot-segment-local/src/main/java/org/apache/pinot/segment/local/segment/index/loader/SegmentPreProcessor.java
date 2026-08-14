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
package org.apache.pinot.segment.local.segment.index.loader;

import com.google.common.base.Preconditions;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.commons.configuration2.PropertiesConfiguration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.common.metrics.ServerMeter;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.segment.local.segment.index.IndexSizeUtils;
import org.apache.pinot.segment.local.segment.index.loader.columnminmaxvalue.ColumnMinMaxValueGenerator;
import org.apache.pinot.segment.local.segment.index.loader.columnminmaxvalue.ColumnMinMaxValueGeneratorMode;
import org.apache.pinot.segment.local.segment.index.loader.defaultcolumn.DefaultColumnHandler;
import org.apache.pinot.segment.local.segment.index.loader.defaultcolumn.DefaultColumnHandlerFactory;
import org.apache.pinot.segment.local.segment.index.loader.invertedindex.LegacyRawValueInvertedIndexCleanup;
import org.apache.pinot.segment.local.segment.index.loader.invertedindex.MultiColumnTextIndexHandler;
import org.apache.pinot.segment.local.startree.StarTreeBuilderUtils;
import org.apache.pinot.segment.local.startree.v2.builder.MultipleTreesBuilder;
import org.apache.pinot.segment.local.startree.v2.builder.StarTreeV2BuilderConfig;
import org.apache.pinot.segment.local.utils.SegmentOperationsThrottlerSet;
import org.apache.pinot.segment.spi.ColumnMetadata;
import org.apache.pinot.segment.spi.V1Constants;
import org.apache.pinot.segment.spi.index.IndexHandler;
import org.apache.pinot.segment.spi.index.IndexService;
import org.apache.pinot.segment.spi.index.IndexType;
import org.apache.pinot.segment.spi.index.StandardIndexes;
import org.apache.pinot.segment.spi.index.metadata.SegmentMetadataImpl;
import org.apache.pinot.segment.spi.index.multicolumntext.MultiColumnTextIndexConstants;
import org.apache.pinot.segment.spi.index.multicolumntext.MultiColumnTextMetadata;
import org.apache.pinot.segment.spi.index.startree.StarTreeV2Metadata;
import org.apache.pinot.segment.spi.store.SegmentDirectory;
import org.apache.pinot.segment.spi.store.SegmentDirectoryPaths;
import org.apache.pinot.segment.spi.utils.SegmentMetadataUtils;
import org.apache.pinot.spi.config.table.MultiColumnTextIndexConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/// Use mmap to load the segment and perform all pre-processing steps. (This can be slow)
///
/// Pre-processing steps include:
///
/// - Use [InvertedIndexHandler] to create inverted indices
/// - Use [DefaultColumnHandler] to update auto-generated default columns
/// - Use [ColumnMinMaxValueGenerator] to add min/max value to column metadata
public class SegmentPreProcessor implements AutoCloseable {
  private static final Logger LOGGER = LoggerFactory.getLogger(SegmentPreProcessor.class);

  private final SegmentDirectory _segmentDirectory;
  private final IndexLoadingConfig _indexLoadingConfig;
  private final TableConfig _tableConfig;
  private final Schema _schema;

  public SegmentPreProcessor(SegmentDirectory segmentDirectory, IndexLoadingConfig indexLoadingConfig) {
    _segmentDirectory = segmentDirectory;
    _indexLoadingConfig = indexLoadingConfig;
    _tableConfig = indexLoadingConfig.getTableConfig();
    Preconditions.checkArgument(_tableConfig != null, "Table config must be provided");
    _schema = indexLoadingConfig.getSchema();
    Preconditions.checkArgument(_schema != null, "Schema must be provided");
  }

  @Override
  public void close()
      throws Exception {
    _segmentDirectory.close();
  }

  public void process()
      throws Exception {
    process(null);
  }

  // TODO: Reduce segment metadata reload, and reload it only if it is modified.
  public void process(@Nullable SegmentOperationsThrottlerSet segmentOperationsThrottlerSet)
      throws Exception {
    SegmentMetadataImpl segmentMetadata = _segmentDirectory.getSegmentMetadata();
    String segmentName = segmentMetadata.getName();
    if (segmentMetadata.getTotalDocs() == 0) {
      LOGGER.info("Skip preprocessing empty segment: {}", segmentName);
      return;
    }

    // Segment processing has to be done with a local directory.
    File indexDir = new File(_segmentDirectory.getIndexDir());

    // This fixes the issue of temporary files not getting deleted after creating new inverted indexes.
    removeInvertedIndexTempFiles(indexDir);

    try (SegmentDirectory.Writer segmentWriter = _segmentDirectory.createWriter()) {
      // Backward-compat shim: invalidate any legacy raw-value embedded-dictionary inverted indexes left over from
      // PR #17060 (reverted by PR #18410) so the standard handlers can rebuild them in the dict-id format. Must
      // run before any handler that may try to read the inverted-index buffer. Safe to delete after Pinot 1.7;
      // see [LegacyRawValueInvertedIndexCleanup] javadoc for the full sunset checklist.
      LegacyRawValueInvertedIndexCleanup.removeLegacyRawValueInvertedIndexes(segmentWriter);

      // Update default columns according to the schema.
      DefaultColumnHandler defaultColumnHandler =
          DefaultColumnHandlerFactory.getDefaultColumnHandler(indexDir, segmentMetadata, _indexLoadingConfig,
              segmentWriter);
      defaultColumnHandler.updateDefaultColumns();
      _segmentDirectory.reloadMetadata();

      // Resolve per-key index configs for OPEN_STRUCT child columns so index handlers don't strip
      // inverted/range indexes that the OpenStructColumnSplitter wrote during segment creation.
      _indexLoadingConfig.addOpenStructChildConfigs(
          (SegmentMetadataImpl) _segmentDirectory.getSegmentMetadata());

      // Update single-column indices, like inverted index, json index etc.
      List<IndexHandler> indexHandlers = new ArrayList<>();

      // We cannot just create all the index handlers in a random order.
      // Specifically, ForwardIndexHandler MUST run first. It is the only handler that:
      //   (a) creates the shared dictionary for a RAW forward index column when a secondary index requires one
      //       (ENABLE_DICTIONARY operation in ForwardIndexHandler.createDictionaryForRawForwardIndex);
      //   (b) updates the segment metadata's HAS_DICTIONARY / FORWARD_INDEX_ENCODING properties accordingly.
      // The InvertedIndexHandler / RangeIndexHandler / FSTIndexHandler then read the freshly-reloaded metadata and
      // build dict-id-based indexes on top of the new shared dictionary. If this order is violated, downstream
      // handlers fail with an IllegalStateException because the dictionary they require does not yet exist.
      // Any future change to handler scheduling MUST preserve: ForwardIndexHandler → reloadMetadata → other handlers.
      IndexHandler forwardHandler = createHandler(StandardIndexes.forward());
      indexHandlers.add(forwardHandler);
      forwardHandler.updateIndices(segmentWriter);
      _segmentDirectory.reloadMetadata();

      // Now that ForwardIndexHandler.updateIndices has been updated, we can run all other indexes in any order
      for (IndexType<?, ?, ?> type : IndexService.getInstance().getAllIndexes()) {
        if (type != StandardIndexes.forward()) {
          IndexHandler handler = createHandler(type);
          indexHandlers.add(handler);
          handler.updateIndices(segmentWriter);
        }
      }

      // Perform post-cleanup operations on the index handlers.
      for (IndexHandler handler : indexHandlers) {
        handler.postUpdateIndicesCleanup(segmentWriter);
      }

      // Index handler might modify the segment metadata, so we need to fetch it again
      segmentMetadata = _segmentDirectory.getSegmentMetadata();

      // Add min/max value to column metadata according to the prune mode.
      ColumnMinMaxValueGeneratorMode columnMinMaxValueGeneratorMode =
          _indexLoadingConfig.getColumnMinMaxValueGeneratorMode();
      if (columnMinMaxValueGeneratorMode != ColumnMinMaxValueGeneratorMode.NONE) {
        ColumnMinMaxValueGenerator columnMinMaxValueGenerator =
            new ColumnMinMaxValueGenerator(segmentMetadata, segmentWriter, columnMinMaxValueGeneratorMode);
        columnMinMaxValueGenerator.addColumnMinMaxValue();
        _segmentDirectory.reloadMetadata();
      }

      segmentWriter.save();
    }

    // Startree creation will load the segment again, so we need to close and re-open the segment writer to make sure
    // that the other required indices (e.g. forward index) are up-to-date.
    IndexPresenceSnapshot indexPresenceSnapshot = null;
    try (SegmentDirectory.Writer segmentWriter = _segmentDirectory.createWriter()) {
      if (processStarTrees(indexDir, segmentOperationsThrottlerSet)) {
        _segmentDirectory.reloadMetadata();
        segmentWriter.save();
      }
      // Create/modify/remove multi-col text index if required.
      if (processMultiColTextIndex(indexDir, segmentWriter, segmentOperationsThrottlerSet)) {
        _segmentDirectory.reloadMetadata();
        segmentWriter.save();
      }

      // Snapshot which (column, indexType) pairs exist right now, straight from this still-open writer -- not from
      // a later independent metadata read -- so refreshPersistedIndexSizes() reconciles against the exact on-disk
      // layout this reload just produced. See its javadoc for why a live snapshot matters here.
      if (_tableConfig.getIndexingConfig().isIndexSizeStatsEnabled()) {
        try {
          List<IndexType<?, ?, ?>> allIndexTypes = IndexService.getInstance().getAllIndexes();
          IndexPresenceSnapshot snapshot = snapshotIndexTypeIds(segmentWriter,
              _segmentDirectory.getSegmentMetadata().getColumnMetadataMap().keySet(), allIndexTypes);
          if (snapshot.getColumnToIndexTypeIds().isEmpty()) {
            // A non-empty segment always has a forward index or dictionary on every column, so an empty snapshot
            // means the backing SegmentDirectory answered "no indexes anywhere" rather than reporting a genuine
            // state -- e.g. SegmentLocalFSDirectory#getColumnsWithIndex returns Set.of() for every type while its
            // column-index directory is not loaded. Treat it the same as a snapshot failure: skip the refresh
            // rather than let a spurious empty answer clear every persisted size below.
            LOGGER.warn("Post-reload index snapshot for segment: {} was unexpectedly empty; skipping index size "
                + "stats refresh for this reload", segmentName);
          } else {
            // Only publish a fully-validated snapshot, so that a failure anywhere above -- including inside this
            // same try block, e.g. the LOGGER.warn call above throwing -- always leaves indexPresenceSnapshot null
            // and the refresh skipped below, with no dependence on how far the try body got before failing.
            indexPresenceSnapshot = snapshot;
          }
        } catch (Exception e) {
          // Advisory stats must never fail a segment load: skip the size refresh for this reload entirely rather
          // than let the whole process() call fail. Per-index-type probe failures are handled inside
          // snapshotIndexTypeIds() itself and do not reach this catch; this remains as a backstop for anything
          // else unexpected (e.g. IndexService.getAllIndexes() itself misbehaving).
          LOGGER.warn("Failed to snapshot post-reload index sizes for segment: {}; skipping index size stats "
              + "refresh for this reload", segmentName, e);
        }
      }
    }

    // Every index handler has finished, so the on-disk layout is final: refresh the persisted per-index sizes.
    // This is opportunistic only: it rides along with whatever reload just ran for some other reason, and never
    // itself decides that a reload is needed. See needProcess() javadoc for why index size stats are excluded from
    // that decision entirely.
    if (indexPresenceSnapshot != null) {
      refreshPersistedIndexSizes(indexDir, indexPresenceSnapshot);
    }
  }

  /// Result of [#snapshotIndexTypeIds]: for every column with at least one index, the set of [IndexType#getId]
  /// values present on it right now, plus the set of index type ids that were actually, successfully probed while
  /// building that map. The two are not redundant: a probe failure for one index type (see
  /// [#snapshotIndexTypeIds]'s javadoc) makes that type's presence unknown for this reload, not absent, so
  /// [#refreshPersistedIndexSizes] must be able to tell "probed and confirmed absent" apart from "never probed."
  private static final class IndexPresenceSnapshot {
    private final Map<String, Set<String>> _columnToIndexTypeIds;
    private final Set<String> _probedIndexTypeIds;

    private IndexPresenceSnapshot(Map<String, Set<String>> columnToIndexTypeIds, Set<String> probedIndexTypeIds) {
      _columnToIndexTypeIds = columnToIndexTypeIds;
      _probedIndexTypeIds = probedIndexTypeIds;
    }

    private Map<String, Set<String>> getColumnToIndexTypeIds() {
      return _columnToIndexTypeIds;
    }

    private Set<String> getPresentIndexTypeIds(String column) {
      return _columnToIndexTypeIds.getOrDefault(column, Set.of());
    }

    private boolean wasProbed(String indexTypeId) {
      return _probedIndexTypeIds.contains(indexTypeId);
    }
  }

  /// Returns, for every column in `columnsToInclude` that currently has at least one index, the set of
  /// [IndexType#getId] values present on it right now. Always read from a live [SegmentDirectory.Reader] (a
  /// [SegmentDirectory.Writer] qualifies too), never from an independent `SegmentMetadataImpl` re-read of
  /// `metadata.properties` or `v3/index_map` -- see [#refreshPersistedIndexSizes] for why that distinction matters.
  ///
  /// Loops index types in the outer loop and calls `SegmentDirectory#getColumnsWithIndex` per type rather than
  /// looping columns and calling `hasIndexFor` per (column, indexType) pair, but this does not make the call cheap:
  /// depending on the backing store, `getColumnsWithIndex` itself may scan every column for every call
  /// (`FilePerIndexDirectory`) or every entry for every call (`SingleFileIndexDirectory`), so total cost still scales
  /// with `allIndexTypes.size()` times the backing store's per-call cost, not with the number of present indexes.
  ///
  /// A per-index-type probe failure (e.g. `FilePerIndexDirectory#getColumnsWithIndex` throwing because a registered
  /// [IndexType#getFileExtensions] returns an empty list) is caught here and only drops that one index type from the
  /// snapshot; it does not fail the whole call. This matters because that failure mode recurs on every future reload
  /// for the same segment and index type, so treating it as "snapshot failed, skip the whole refresh" would
  /// permanently stop refreshing every other index type on this segment too, not just this one.
  private static IndexPresenceSnapshot snapshotIndexTypeIds(SegmentDirectory.Reader reader,
      Set<String> columnsToInclude, List<IndexType<?, ?, ?>> allIndexTypes) {
    Map<String, Set<String>> columnToIndexTypeIds = new HashMap<>();
    Set<String> probedIndexTypeIds = new HashSet<>();
    SegmentDirectory segmentDirectory = reader.toSegmentDirectory();
    for (IndexType<?, ?, ?> indexType : allIndexTypes) {
      Set<String> columnsWithIndex;
      try {
        columnsWithIndex = segmentDirectory.getColumnsWithIndex(indexType);
      } catch (Exception e) {
        LOGGER.warn("Failed to probe index type: {} while snapshotting post-reload index presence; treating its "
            + "presence as unknown for this reload rather than absent", indexType.getId(), e);
        continue;
      }
      probedIndexTypeIds.add(indexType.getId());
      for (String column : columnsWithIndex) {
        if (columnsToInclude.contains(column)) {
          columnToIndexTypeIds.computeIfAbsent(column, c -> new HashSet<>()).add(indexType.getId());
        }
      }
    }
    return new IndexPresenceSnapshot(columnToIndexTypeIds, probedIndexTypeIds);
  }

  /// Updates the `column.<column>.indexSizeInBytes.<indexTypeId>` entries in `metadata.properties` to match
  /// `indexPresenceSnapshot`, the live post-reload index presence for every column, and leaves every other entry
  /// untouched.
  ///
  /// Without this the values would stay a build-time snapshot: reload can add, drop or re-compress an index, and a
  /// stale size is indistinguishable from a current one. This is the only hook, deliberately: index sizes span every
  /// index type, so updating them per handler -- as `compressionStatsEnabled` does in `ForwardIndexHandler`, where a
  /// single index is involved -- would require every future handler to remember to participate.
  ///
  /// For every column and every currently-registered [IndexType]:
  /// - Present (per `indexPresenceSnapshot`): (re)sized the same way segment creation would size it -- from the
  ///   packed [ColumnMetadata#getIndexSize] position if there is one, else from the index's own file/directory --
  ///   which is reliable here specifically because this reload just wrote the current layout, locally, moments ago.
  ///   The size is only written if it differs from what is already persisted, so a reload that left an index
  ///   untouched writes nothing. Refreshing every present index rather than only newly-added ones is deliberate: a
  ///   handler can remove and recreate an index of the same type within one reload -- e.g.
  ///   `LegacyRawValueInvertedIndexCleanup` dropping a legacy-format inverted index for `InvertedIndexHandler` to
  ///   rebuild, or `ForwardIndexHandler` changing a raw column's compression codec -- which leaves presence
  ///   unchanged across the reload while the actual size changes. Presence alone cannot see that; comparing the
  ///   freshly computed size against the persisted one can.
  /// - Absent (per `indexPresenceSnapshot`) among the index types that were actually, successfully probed while
  ///   building that snapshot: its persisted key, if any, is cleared. This intentionally does not require having
  ///   observed the index as present on some earlier reload: a successfully probed index type reports every column
  ///   with at least one index, live, right now, so that index type being absent from a column's present set is
  ///   confirmed absent for that column, not merely unobserved. Clearing unconditionally on that basis is what
  ///   reconciles a phantom size left behind by, for example, a reload that dropped an index while
  ///   `indexSizeStatsEnabled` was off (which skips this method entirely) followed by a later reload with the flag
  ///   back on. An index type that failed to probe (see [#snapshotIndexTypeIds]) is excluded from this clearing:
  ///   its absence from a column's present set means "unknown," not "confirmed absent," so its persisted entry, if
  ///   any, is left alone -- the same "leave unchanged" treatment as an unmeasurable present index below. This also
  ///   covers an index type this node has no plugin for at all: such an id can never appear as probed, since probing
  ///   only iterates locally-registered [IndexType]s, so a size persisted by a node or version with a plugin this
  ///   node lacks survives reload here rather than being wiped by a node that cannot even see it exists.
  /// No tier logic anywhere here: the same rule applies to every segment format and every storage tier.
  ///
  /// Only called by [#process] with a non-null, already-validated `indexPresenceSnapshot` (non-empty, and produced
  /// without the snapshotting step itself throwing), so this method re-checks neither `indexSizeStatsEnabled` nor
  /// emptiness. Failures
  /// are logged and swallowed: these statistics are advisory and must never fail a segment load, and -- because
  /// nothing outside this method ever asks "are the persisted sizes still accurate," see [#needProcess] -- must
  /// never force one either. Refreshing only happens as a side effect of a reload that some other check already
  /// decided was needed.
  ///
  /// Runs after `process()`'s own [SegmentDirectory.Writer] has already closed, and reads `indexDir` directly off
  /// disk rather than through the writer, so it relies on the same invariant every other unguarded step of
  /// `process()` does: the caller holds the per-segment lock (see `BaseTableDataManager`'s segment locks) for the
  /// duration of this call, so nothing else concurrently mutates this segment directory.
  private void refreshPersistedIndexSizes(File indexDir, IndexPresenceSnapshot indexPresenceSnapshot) {
    try {
      // Read the metadata fresh off disk rather than using _segmentDirectory.getSegmentMetadata(): that returns a
      // cached instance describing the layout as it was before the handlers ran. Only used below to size present
      // indexes, never to decide what is present -- that comes from indexPresenceSnapshot.
      SegmentMetadataImpl segmentMetadata = new SegmentMetadataImpl(indexDir);
      if (segmentMetadata.getTotalDocs() == 0) {
        return;
      }
      IndexService indexService = IndexService.getInstance();
      PropertiesConfiguration properties = SegmentMetadataUtils.getPropertiesConfiguration(indexDir);
      File segmentContentDir = SegmentDirectoryPaths.findSegmentDirectory(indexDir);
      Map<String, ColumnMetadata> columnMetadataMap = segmentMetadata.getColumnMetadataMap();

      boolean propertiesChanged = false;
      for (Map.Entry<String, ColumnMetadata> columnEntry : columnMetadataMap.entrySet()) {
        String column = columnEntry.getKey();
        ColumnMetadata columnMetadata = columnEntry.getValue();
        Set<String> presentIndexTypeIds = indexPresenceSnapshot.getPresentIndexTypeIds(column);

        // Clear the persisted size for every previously-persisted index type confirmed absent on this column --
        // successfully probed but not present -- regardless of whether this particular reload is the one that
        // removed it; see the javadoc above for why this is safe and why it is also what reconciles a
        // flag-off/flag-on toggle. An index type that was never successfully probed (unregistered on this node, or a
        // probe failure) is left untouched instead: see the javadoc above. Walking only the keys already on disk,
        // rather than every registered index type, keeps this proportional to what is actually persisted.
        for (String indexTypeId : columnMetadata.getPersistedIndexSizesInBytes().keySet()) {
          if (indexPresenceSnapshot.wasProbed(indexTypeId) && !presentIndexTypeIds.contains(indexTypeId)) {
            properties.clearProperty(V1Constants.MetadataKeys.Column.getIndexSizeKeyFor(column, indexTypeId));
            propertiesChanged = true;
          }
        }

        for (String indexTypeId : presentIndexTypeIds) {
          // Scoped per (column, index type): a misbehaving index type must only drop its own entry, not the rest
          // of this column or segment's refresh.
          try {
            // presentIndexTypeIds only ever contains ids produced by iterating IndexService.getAllIndexes() (see
            // snapshotIndexTypeIds), so this lookup is expected to always succeed; kept defensive rather than
            // orElseThrow() so a future change to that invariant degrades to skipping this one entry, inside this
            // method's per-(column, index type) failure scope, rather than aborting the whole refresh.
            IndexType<?, ?, ?> indexType = indexService.getOptional(indexTypeId).orElse(null);
            if (indexType == null) {
              continue;
            }
            String key = V1Constants.MetadataKeys.Column.getIndexSizeKeyFor(column, indexTypeId);
            long size = columnMetadata.getIndexSizeFor(indexType);
            if (size == ColumnMetadata.UNAVAILABLE) {
              size = IndexSizeUtils.sizeOfFileOrDirIndex(indexType, segmentContentDir, column, 0, columnMetadata);
              if (size == ColumnMetadata.UNAVAILABLE) {
                // Present per the live post-reload snapshot but currently unmeasurable (e.g. a sizing failure):
                // leave whatever is already persisted untouched rather than discarding a previously-good value, or
                // pinning this column on "missing" forever if one was never successfully measured.
                LOGGER.warn("Could not determine size of index {} on column {} in segment {}; leaving persisted "
                    + "value, if any, unchanged", indexTypeId, column, segmentMetadata.getName());
                continue;
              }
            }
            // Only write when the freshly computed size actually differs from what's persisted, so a reload that
            // left this index alone does not dirty metadata.properties for no reason. A malformed persisted value
            // (e.g. hand-edited metadata.properties) is treated as "differs" so it gets corrected rather than
            // aborting the refresh for every other column and index type in this segment.
            long persistedSize;
            try {
              persistedSize = properties.getLong(key, -1L);
            } catch (Exception e) {
              LOGGER.debug("Malformed persisted index size for key: {} in segment: {}; overwriting", key,
                  segmentMetadata.getName(), e);
              persistedSize = -1L;
            }
            if (persistedSize != size) {
              properties.setProperty(key, String.valueOf(size));
              propertiesChanged = true;
            }
          } catch (Exception e) {
            LOGGER.warn("Failed to refresh persisted index size for column {}, index type {} in segment {}", column,
                indexTypeId, segmentMetadata.getName(), e);
          }
        }
      }
      if (propertiesChanged) {
        SegmentMetadataUtils.savePropertiesConfiguration(properties, indexDir);
      }
    } catch (Exception e) {
      LOGGER.warn("Failed to refresh persisted index sizes for segment: {}", _segmentDirectory.getSegmentMetadata()
          .getName(), e);
    }
  }

  private IndexHandler createHandler(IndexType<?, ?, ?> type) {
    return type.createIndexHandler(_segmentDirectory, _indexLoadingConfig.getFieldIndexConfigByColName(), _schema,
        _tableConfig);
  }

  /// This method checks if there is any discrepancy between the segment and current table config and schema.
  /// If so, it returns true indicating the segment needs to be reprocessed. Right now, the default columns,
  /// all types of indices and column min/max values are checked against what's set in table config and schema.
  public boolean needProcess()
      throws Exception {
    SegmentMetadataImpl segmentMetadata = _segmentDirectory.getSegmentMetadata();
    if (segmentMetadata.getTotalDocs() == 0) {
      return false;
    }
    String segmentName = segmentMetadata.getName();
    try (SegmentDirectory.Reader segmentReader = _segmentDirectory.createReader()) {
      // Check if there is need to update default columns according to the schema.
      DefaultColumnHandler defaultColumnHandler =
          DefaultColumnHandlerFactory.getDefaultColumnHandler(null, segmentMetadata, _indexLoadingConfig, null);
      if (defaultColumnHandler.needUpdateDefaultColumns()) {
        LOGGER.info("Found default columns need updates in segment: {}", segmentName);
        return true;
      }
      // Check if there is need to update single-column indices, like inverted index, json index etc.
      for (IndexType<?, ?, ?> type : IndexService.getInstance().getAllIndexes()) {
        if (createHandler(type).needUpdateIndices(segmentReader)) {
          LOGGER.info("Found index type: {} needs updates in segment: {}", type, segmentName);
          return true;
        }
      }
      // Check if there is need to create/modify/remove star-trees.
      if (needProcessStarTrees()) {
        LOGGER.info("Found startree index needs updates in segment: {}", segmentName);
        return true;
      }

      // Check if there is need to create/modify/remove multi-col text index
      if (needProcessMultiColumnTextIndex()) {
        LOGGER.info("Found multi-column text index needs updates in segment: {}", segmentName);
        return true;
      }

      // Check if there is need to update column min max value.
      List<String> columnMinMaxValueUpdates = columnMinMaxValueUpdates();
      if (!columnMinMaxValueUpdates.isEmpty()) {
        LOGGER.info("Found min max values need updates for columns: {} in segment: {}", columnMinMaxValueUpdates,
            segmentName);
        return true;
      }

      // Deliberately no check for missing index size stats here: refreshPersistedIndexSizes() backfills them
      // opportunistically whenever a reload runs for one of the reasons above, but a missing or stale size must
      // never, on its own, be a reason to trigger one. needProcess() == true drives a full segment-directory copy
      // and reprocess (see BaseTableDataManager), so treating "just turned indexSizeStatsEnabled on" as a reload
      // reason would force that cost across every segment in the table for an advisory statistic; worse, a present
      // index that is persistently unmeasurable (e.g. a directory FileUtils.sizeOfDirectory cannot read) would
      // never satisfy the check, so every future load of that segment would copy and reprocess it again forever.
    }
    return false;
  }

  private List<String> columnMinMaxValueUpdates() {
    ColumnMinMaxValueGeneratorMode columnMinMaxValueGeneratorMode =
        _indexLoadingConfig.getColumnMinMaxValueGeneratorMode();
    if (columnMinMaxValueGeneratorMode == ColumnMinMaxValueGeneratorMode.NONE) {
      return List.of();
    }
    ColumnMinMaxValueGenerator columnMinMaxValueGenerator =
        new ColumnMinMaxValueGenerator(_segmentDirectory.getSegmentMetadata(), null, columnMinMaxValueGeneratorMode);
    return columnMinMaxValueGenerator.columnMinMaxValueUpdates();
  }

  private boolean needProcessStarTrees() {
    // Check if there is need to create/modify/remove star-trees.
    if (!_indexLoadingConfig.isEnableDynamicStarTreeCreation()) {
      return false;
    }

    SegmentMetadataImpl segmentMetadata = _segmentDirectory.getSegmentMetadata();
    List<StarTreeV2BuilderConfig> starTreeBuilderConfigs =
        StarTreeBuilderUtils.generateBuilderConfigs(_indexLoadingConfig.getStarTreeIndexConfigs(),
            _indexLoadingConfig.isEnableDefaultStarTree(), segmentMetadata);
    List<StarTreeV2Metadata> starTreeMetadataList = segmentMetadata.getStarTreeV2MetadataList();
    // There are existing star-trees, but if they match the builder configs exactly,
    // then there is no need to generate the star-trees

    // We need reprocessing if existing configs are to be removed, or new configs have been added
    if (starTreeMetadataList != null) {
      return StarTreeBuilderUtils.shouldModifyExistingStarTrees(starTreeBuilderConfigs, starTreeMetadataList);
    }
    return !starTreeBuilderConfigs.isEmpty();
  }

  private boolean needProcessMultiColumnTextIndex() {
    MultiColumnTextIndexConfig newConfig = _indexLoadingConfig.getMultiColTextIndexConfig();
    MultiColumnTextMetadata oldConfig = _segmentDirectory.getSegmentMetadata().getMultiColumnTextMetadata();
    return MultiColumnTextIndexHandler.shouldModifyMultiColTextIndex(newConfig, oldConfig);
  }

  private boolean processMultiColTextIndex(File indexDir, SegmentDirectory.Writer segmentWriter,
      @Nullable SegmentOperationsThrottlerSet segmentOperationsThrottlerSet)
      throws Exception {
    SegmentMetadataImpl segmentMetadata = _segmentDirectory.getSegmentMetadata();
    String segmentName = segmentMetadata.getName();
    MultiColumnTextMetadata oldConfig = segmentMetadata.getMultiColumnTextMetadata();
    MultiColumnTextIndexConfig newConfig = _indexLoadingConfig.getMultiColTextIndexConfig();
    boolean remove = false;
    boolean create = newConfig != null;

    if (oldConfig != null) {
      if (newConfig == null) {
        remove = true;
      } else {
        if (MultiColumnTextIndexHandler.shouldModifyMultiColTextIndex(newConfig, oldConfig)) {
          LOGGER.info("Change detected in multi-column text index for segment: {}", segmentName);
        } else {
          create = false;
        }
      }
    }
    if (!remove && !create) {
      LOGGER.info("No change detected in multi-column text index for segment: {}", segmentName);
      return false;
    }

    if (segmentOperationsThrottlerSet != null) {
      segmentOperationsThrottlerSet.getSegmentMultiColTextIndexPreprocessThrottler().acquire();
    }
    try {
      if (remove) {
        LOGGER.info("Removing multi-column text index from segment: {}", segmentName);
        removeMultiColumnTextIndex(indexDir);
      } else if (create) {
        if (oldConfig != null) {
          // Drop existing multi-column text index before creating a new one
          // TODO: check if it's possible to only add/remove select columns
          removeMultiColumnTextIndex(indexDir);
        }
        MultiColumnTextIndexHandler handler =
            new MultiColumnTextIndexHandler(_segmentDirectory, _indexLoadingConfig, newConfig);
        handler.updateIndices(segmentWriter);
        handler.postUpdateIndicesCleanup(segmentWriter);
      }
    } finally {
      if (segmentOperationsThrottlerSet != null) {
        segmentOperationsThrottlerSet.getSegmentMultiColTextIndexPreprocessThrottler().release();
      }
    }
    return true;
  }

  private void removeMultiColumnTextIndex(File indexDir)
      throws ConfigurationException, IOException {
    // Remove the multi-col text index metadata
    PropertiesConfiguration metadataProperties = SegmentMetadataUtils.getPropertiesConfiguration(indexDir);
    metadataProperties.subset(MultiColumnTextIndexConstants.MetadataKey.ROOT_SUBSET).clear();
    SegmentMetadataUtils.savePropertiesConfiguration(metadataProperties, indexDir);

    // Remove the index file and index map file
    File segmentDirectory = SegmentDirectoryPaths.findSegmentDirectory(indexDir);
    File textIdxDir =
        SegmentDirectoryPaths.findTextIndexIndexFile(segmentDirectory, MultiColumnTextIndexConstants.INDEX_DIR_NAME);

    if (textIdxDir != null && textIdxDir.exists()) {
      FileUtils.forceDelete(textIdxDir);
    }
    File mappingFile = new File(segmentDirectory, MultiColumnTextIndexConstants.DOCID_MAPPING_FILE_NAME);
    if (mappingFile.exists()) {
      FileUtils.forceDelete(mappingFile);
    }
  }

  private boolean processStarTrees(File indexDir,
      @Nullable SegmentOperationsThrottlerSet segmentOperationsThrottlerSet)
      throws Exception {
    if (!_indexLoadingConfig.isEnableDynamicStarTreeCreation()) {
      return false;
    }

    SegmentMetadataImpl segmentMetadata = _segmentDirectory.getSegmentMetadata();
    String segmentName = segmentMetadata.getName();
    List<StarTreeV2BuilderConfig> starTreeBuilderConfigs =
        StarTreeBuilderUtils.generateBuilderConfigs(_indexLoadingConfig.getStarTreeIndexConfigs(),
            _indexLoadingConfig.isEnableDefaultStarTree(), segmentMetadata);

    boolean shouldGenerateStarTree = !starTreeBuilderConfigs.isEmpty();
    boolean shouldRemoveStarTree = false;
    List<StarTreeV2Metadata> starTreeMetadataList = segmentMetadata.getStarTreeV2MetadataList();
    if (starTreeMetadataList != null) {
      // There are existing star-trees
      if (!shouldGenerateStarTree) {
        // Newer config does not have star-trees. Delete all existing star-trees.
        shouldRemoveStarTree = true;
      } else if (StarTreeBuilderUtils.shouldModifyExistingStarTrees(starTreeBuilderConfigs, starTreeMetadataList)) {
        // Existing and newer both have star-trees, but they don't match. Rebuild the star-trees.
        LOGGER.info("Change detected in star-trees for segment: {}", segmentName);
      } else {
        // Existing star-trees match the builder configs, no need to generate the star-trees
        shouldGenerateStarTree = false;
      }
    }
    if (!shouldGenerateStarTree && !shouldRemoveStarTree) {
      return false;
    }

    if (segmentOperationsThrottlerSet != null) {
      segmentOperationsThrottlerSet.getSegmentStarTreePreprocessThrottler().acquire();
    }
    try {
      if (shouldRemoveStarTree) {
        // 'shouldGenerateStarTree' should be false if they need to be removed
        LOGGER.info("Removing star-trees from segment: {}", segmentName);
        StarTreeBuilderUtils.removeStarTrees(indexDir);
      } else {
        // NOTE: Always use OFF_HEAP mode on server side.
        // Pass _indexLoadingConfig so downstream readers can resolve table-level configs we set
        MultipleTreesBuilder builder = new MultipleTreesBuilder(starTreeBuilderConfigs, indexDir,
            MultipleTreesBuilder.BuildMode.OFF_HEAP, _indexLoadingConfig);
        // We don't create the builder using the try-with-resources pattern because builder.close() performs
        // some clean-up steps to roll back the star-tree index to the previous state if it exists. If this goes wrong
        // the star-tree index can be in an inconsistent state. To prevent that, when builder.close() throws an
        // exception we want to propagate that up instead of ignoring it. This can get clunky when using
        // try-with-resources as in this scenario the close() exception will be added to the suppressed exception list
        // rather than thrown as the main exception, even though the original exception thrown on build() is ignored.
        try {
          builder.build();
        } catch (Exception e) {
          String tableNameWithType = _tableConfig.getTableName();
          LOGGER.error("Failed to build star-tree index for table: {}, skipping", tableNameWithType, e);
          ServerMetrics.get().addMeteredTableValue(tableNameWithType, ServerMeter.STAR_TREE_INDEX_BUILD_FAILURES, 1);
        } finally {
          builder.close();
        }
      }
    } finally {
      if (segmentOperationsThrottlerSet != null) {
        segmentOperationsThrottlerSet.getSegmentStarTreePreprocessThrottler().release();
      }
    }
    return true;
  }

  /// Remove all the existing inverted index temp files before loading segments, by looking
  /// for all files in the directory and remove the ones with  '.bitmap.inv.tmp' extension.
  private void removeInvertedIndexTempFiles(File indexDir) {
    File[] directoryListing = indexDir.listFiles();
    if (directoryListing == null) {
      return;
    }
    String tempFileExtension = V1Constants.Indexes.BITMAP_INVERTED_INDEX_FILE_EXTENSION + ".tmp";
    for (File child : directoryListing) {
      if (child.getName().endsWith(tempFileExtension)) {
        FileUtils.deleteQuietly(child);
      }
    }
  }
}
