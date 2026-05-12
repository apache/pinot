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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import java.io.File;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.segment.local.io.util.PinotDataBitSet;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentDictionaryCreator;
import org.apache.pinot.segment.local.segment.creator.impl.fwd.MultiValueVarByteRawIndexCreator;
import org.apache.pinot.segment.local.segment.creator.impl.stats.AbstractColumnStatisticsCollector;
import org.apache.pinot.segment.local.segment.creator.impl.stats.BigDecimalColumnPreIndexStatsCollector;
import org.apache.pinot.segment.local.segment.creator.impl.stats.BytesColumnPreIndexStatsCollector;
import org.apache.pinot.segment.local.segment.creator.impl.stats.DoubleColumnPreIndexStatsCollector;
import org.apache.pinot.segment.local.segment.creator.impl.stats.FloatColumnPreIndexStatsCollector;
import org.apache.pinot.segment.local.segment.creator.impl.stats.IntColumnPreIndexStatsCollector;
import org.apache.pinot.segment.local.segment.creator.impl.stats.LongColumnPreIndexStatsCollector;
import org.apache.pinot.segment.local.segment.creator.impl.stats.MapColumnPreIndexStatsCollector;
import org.apache.pinot.segment.local.segment.creator.impl.stats.NoDictColumnStatisticsCollector;
import org.apache.pinot.segment.local.segment.creator.impl.stats.StringColumnPreIndexStatsCollector;
import org.apache.pinot.segment.local.segment.index.dictionary.DictionaryIndexType;
import org.apache.pinot.segment.local.segment.readers.PinotSegmentColumnReader;
import org.apache.pinot.segment.local.utils.ClusterConfigForTable;
import org.apache.pinot.segment.spi.ColumnMetadata;
import org.apache.pinot.segment.spi.V1Constants;
import org.apache.pinot.segment.spi.compression.ChunkCompressionType;
import org.apache.pinot.segment.spi.compression.DictIdCompressionType;
import org.apache.pinot.segment.spi.creator.IndexCreationContext;
import org.apache.pinot.segment.spi.creator.SegmentVersion;
import org.apache.pinot.segment.spi.creator.StatsCollectorConfig;
import org.apache.pinot.segment.spi.index.DictionaryIndexConfig;
import org.apache.pinot.segment.spi.index.FieldIndexConfigs;
import org.apache.pinot.segment.spi.index.FieldIndexConfigsUtil;
import org.apache.pinot.segment.spi.index.ForwardIndexConfig;
import org.apache.pinot.segment.spi.index.IndexReaderFactory;
import org.apache.pinot.segment.spi.index.IndexType;
import org.apache.pinot.segment.spi.index.StandardIndexes;
import org.apache.pinot.segment.spi.index.creator.ForwardIndexCreator;
import org.apache.pinot.segment.spi.index.metadata.SegmentMetadataImpl;
import org.apache.pinot.segment.spi.index.reader.Dictionary;
import org.apache.pinot.segment.spi.index.reader.ForwardIndexReader;
import org.apache.pinot.segment.spi.index.reader.ForwardIndexReaderContext;
import org.apache.pinot.segment.spi.store.SegmentDirectory;
import org.apache.pinot.segment.spi.utils.SegmentMetadataUtils;
import org.apache.pinot.spi.config.table.FieldConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.ComplexFieldSpec;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.pinot.segment.spi.V1Constants.MetadataKeys.Column.*;


/**
 * Helper class used by {@link SegmentPreProcessor} to make changes to forward index and dictionary configs. Note
 * that this handler only works for segment versions >= 3.0. Support for segment version < 3.0 is not added because
 * majority of the usecases are in versions >= 3.0 and this avoids adding tech debt. The currently supported
 * operations are:
 * 1. Change compression type for a raw column
 * 2. Enable dictionary
 * 3. Disable dictionary
 * 4. Disable forward index
 * 5. Rebuild the forward index for a forwardIndexDisabled column
 *
 *  TODO: Add support for the following:
 *  1. Segment versions < V3
 */
public class ForwardIndexHandler extends BaseIndexHandler {
  private static final Logger LOGGER = LoggerFactory.getLogger(ForwardIndexHandler.class);

  // This should contain a list of all indexes that need to be rewritten if the dictionary is enabled or disabled
  private static final List<IndexType<?, ?, ?>> DICTIONARY_BASED_INDEXES_TO_REWRITE =
      Arrays.asList(StandardIndexes.range(), StandardIndexes.fst(), StandardIndexes.inverted());

  /// Re-enable operations are split by target encoding so the intent is explicit at the operation level: a
  /// `forwardIndex.disabled` column being re-enabled may want to come back as either dict-encoded or raw,
  /// depending on the new config. Both variants flow through the same regenerate-from-inverted-index path
  /// (only the output writer differs), but having them as distinct operations makes the call site readable
  /// and the test assertions specific.
  protected enum Operation {
    DISABLE_FORWARD_INDEX, ENABLE_DICT_FORWARD_INDEX, ENABLE_RAW_FORWARD_INDEX, DISABLE_DICTIONARY,
    ENABLE_DICTIONARY, CHANGE_INDEX_COMPRESSION_TYPE
  }

  @VisibleForTesting
  public ForwardIndexHandler(SegmentDirectory segmentDirectory, IndexLoadingConfig indexLoadingConfig) {
    this(segmentDirectory, indexLoadingConfig.getFieldIndexConfigByColName(), indexLoadingConfig.getTableConfig(),
        indexLoadingConfig.getSchema());
  }

  public ForwardIndexHandler(SegmentDirectory segmentDirectory, Map<String, FieldIndexConfigs> fieldIndexConfigs,
      TableConfig tableConfig, Schema schema) {
    super(segmentDirectory, fieldIndexConfigs, tableConfig, schema);
  }

  @Override
  public boolean needUpdateIndices(SegmentDirectory.Reader segmentReader)
      throws Exception {
    Map<String, List<Operation>> columnOperationsMap = computeOperations(segmentReader);
    return !columnOperationsMap.isEmpty();
  }

  @Override
  public void updateIndices(SegmentDirectory.Writer segmentWriter)
      throws Exception {
    Map<String, List<Operation>> columnOperationsMap = computeOperations(segmentWriter);
    if (columnOperationsMap.isEmpty()) {
      return;
    }

    Set<String> forwardIndexDisabledColumns =
        FieldIndexConfigsUtil.columnsWithIndexDisabled(_fieldIndexConfigs.keySet(), StandardIndexes.forward(),
            _fieldIndexConfigs);
    for (Map.Entry<String, List<Operation>> entry : columnOperationsMap.entrySet()) {
      String column = entry.getKey();
      List<Operation> operations = entry.getValue();

      for (Operation operation : operations) {
        switch (operation) {
          case DISABLE_FORWARD_INDEX:
            // Deletion of the forward index will be handled outside the index handler to ensure that other index
            // handlers that need the forward index to construct their own indexes will have it available.
            _tmpForwardIndexColumns.add(column);
            break;
          case ENABLE_DICT_FORWARD_INDEX: {
            // Rebuilding a dict-encoded forward index requires the dictionary to be present afterwards — the
            // forward index stores dict ids that resolve through it.
            ColumnMetadata columnMetadata = createForwardIndexIfNeeded(segmentWriter, column, false);
            if (!columnMetadata.hasDictionary() || !segmentWriter.hasIndexFor(column, StandardIndexes.dictionary())) {
              throw new IllegalStateException(String.format(
                  "Dictionary should still exist after rebuilding dict-encoded forward index for column: %s", column));
            }
            break;
          }
          case ENABLE_RAW_FORWARD_INDEX: {
            // Two on-disk shapes can land here:
            //   (a) Forward index is absent → rebuild from dict + inverted as raw forward (no dict change).
            //   (b) Forward index exists and is DICT-encoded → flip the encoding to RAW in place, keeping the
            //       dictionary because another enabled index requires it (e.g. dict + inverted + raw forward).
            // The dictionary stays untouched in either case; if the new config wants the dictionary gone, the
            // dict-transition step queues DISABLE_DICTIONARY separately (which on a dict-encoded forward
            // already takes the convert-and-drop path, so the planner suppresses this op in that combo).
            if (segmentWriter.hasIndexFor(column, StandardIndexes.forward())
                && isForwardIndexDictionaryEncoded(column)) {
              convertDictForwardToRawKeepingDictionary(column, segmentWriter);
            } else {
              createForwardIndexIfNeeded(segmentWriter, column, false);
            }
            if (!segmentWriter.hasIndexFor(column, StandardIndexes.forward())) {
              throw new IllegalStateException(
                  String.format("Forward index was not created for column: %s", column));
            }
            break;
          }
          case DISABLE_DICTIONARY:
            if (forwardIndexDisabledColumns.contains(column)) {
              disableDictionary(column, segmentWriter, "Disable dictionary when no forward index exists");
              if (segmentWriter.hasIndexFor(column, StandardIndexes.dictionary())) {
                throw new IllegalStateException(
                    String.format("Dictionary should not exist after disabling dictionary for column: %s", column));
              }
            } else if (isForwardIndexDictionaryEncoded(column)) {
              disableDictionaryAndCreateRawForwardIndex(column, segmentWriter);
            } else {
              disableDictionary(column, segmentWriter, "Disable dictionary while keeping raw forward index");
            }
            break;
          case ENABLE_DICTIONARY:
            if (_fieldIndexConfigs.get(column).getConfig(StandardIndexes.forward()).getEncodingType()
                == FieldConfig.EncodingType.DICTIONARY) {
              createDictBasedForwardIndex(column, segmentWriter);
            } else {
              createDictionaryForRawForwardIndex(column, segmentWriter);
            }
            if (!segmentWriter.hasIndexFor(column, StandardIndexes.forward())) {
              throw new IllegalStateException(String.format("Forward index was not created for column: %s", column));
            }
            break;
          case CHANGE_INDEX_COMPRESSION_TYPE:
            rewriteForwardIndexForCompressionChange(column, segmentWriter);
            break;
          default:
            throw new IllegalStateException("Unsupported operation for column " + column);
        }
      }
    }
  }

  @VisibleForTesting
  Map<String, List<Operation>> computeOperations(SegmentDirectory.Reader segmentReader)
      throws Exception {
    Map<String, List<Operation>> columnOperationsMap = new HashMap<>();

    // Does not work for segment versions < V3.
    if (_segmentDirectory.getSegmentMetadata().getVersion().compareTo(SegmentVersion.v3) < 0) {
      return columnOperationsMap;
    }

    Set<String> existingAllColumns = _segmentDirectory.getSegmentMetadata().getSchema().getPhysicalColumnNames();
    Set<String> existingDictColumns = _segmentDirectory.getColumnsWithIndex(StandardIndexes.dictionary());
    Set<String> existingForwardIndexColumns = _segmentDirectory.getColumnsWithIndex(StandardIndexes.forward());
    Set<String> existingInvertedIndexColumns =
        segmentReader.toSegmentDirectory().getColumnsWithIndex(StandardIndexes.inverted());
    String segmentName = _segmentDirectory.getSegmentMetadata().getName();

    for (String column : existingAllColumns) {
      if (!_schema.hasColumn(column)) {
        // _schema will be null only in tests
        LOGGER.info("Column: {} of segment: {} is not in schema, skipping updating forward index", column, segmentName);
        continue;
      }
      // Skip complex types (MAP, etc.) — they do not support dictionary encoding
      FieldSpec fieldSpec = _schema.getFieldSpecFor(column);
      if (fieldSpec instanceof ComplexFieldSpec) {
        continue;
      }

      // Forward-index encoding is the source-of-truth for "does forward exist and how is it laid out". Three
      // states: DICTIONARY (dict-encoded forward), RAW (raw forward), null (forward disabled / not on disk).
      FieldConfig.EncodingType existingFwdEncoding = existingForwardIndexColumns.contains(column)
          ? _segmentDirectory.getSegmentMetadata().getColumnMetadataFor(column).getForwardIndexEncoding() : null;
      ForwardIndexConfig newFwdConf = _fieldIndexConfigs.get(column).getConfig(StandardIndexes.forward());
      FieldConfig.EncodingType newFwdEncoding = newFwdConf.isEnabled() ? newFwdConf.getEncodingType() : null;

      List<Operation> ops = computeColumnOperations(column, fieldSpec, segmentReader,
          existingDictColumns.contains(column), existingFwdEncoding, newFwdEncoding,
          existingInvertedIndexColumns.contains(column), segmentName);
      if (!ops.isEmpty()) {
        columnOperationsMap.put(column, ops);
      }
    }
    if (!columnOperationsMap.isEmpty()) {
      LOGGER.info("Need to apply columnOperations: {} for forward index for segment: {}", columnOperationsMap,
          segmentName);
    }
    return columnOperationsMap;
  }

  /// Compute the operations needed to bring one column from its existing on-disk state to the desired state in
  /// the new config. The logic decomposes into four orthogonal questions, computed in this order:
  ///
  /// 1. **Forward-index transition** — based on `existingFwdEncoding` vs `newFwdEncoding` (each is one of
  ///    `DICTIONARY`, `RAW`, or `null` for "forward index disabled / not on disk").
  /// 2. **Dictionary transition** — based on `existingHasDict` vs `desiredDict`, where
  ///    `desiredDict = newIsDict || any-enabled-index-requires-dict`. The "force on if required" rule is the
  ///    only place this method consults other indexes — once `desiredDict` is computed, the rest of the logic
  ///    treats it as the source of truth.
  /// 3. **Compression-type change** — only when no encoding change happened (forward + dict both unchanged).
  /// 4. **Cross-cutting guards** — sorted columns can't toggle forward; range index format is incompatible
  ///    with disabling the dictionary; enabling forward needs dict + inverted on disk; enabling dict needs
  ///    forward to be on so the dict can be bootstrapped.
  private List<Operation> computeColumnOperations(String column, FieldSpec fieldSpec,
      SegmentDirectory.Reader segmentReader, boolean existingHasDict,
      @Nullable FieldConfig.EncodingType existingFwdEncoding, @Nullable FieldConfig.EncodingType newFwdEncoding,
      boolean existingHasInverted, String segmentName)
      throws Exception {
    FieldIndexConfigs newConf = _fieldIndexConfigs.get(column);
    boolean newIsDict = newConf.getConfig(StandardIndexes.dictionary()).isEnabled();
    boolean newIsRange = newConf.getConfig(StandardIndexes.range()).isEnabled();
    ColumnMetadata columnMetadata = _segmentDirectory.getSegmentMetadata().getColumnMetadataFor(column);
    boolean existingHasFwd = existingFwdEncoding != null;
    boolean newHasFwd = newFwdEncoding != null;

    // Force the dictionary on whenever any enabled index requires it — otherwise that index would be left
    // orphaned after reload. This is the only place where index-level dependencies feed into the dictionary
    // decision; the rest of this method treats `desiredDict` as authoritative.
    boolean dictRequiredByIndex = fieldSpec != null && DictionaryIndexConfig.requiresDictionary(fieldSpec, newConf);
    boolean desiredDict = newIsDict || dictRequiredByIndex;

    List<Operation> ops = new ArrayList<>(2);

    // 1. Forward-index transition. Three sub-cases:
    //    (a) existing has forward, new disables it      → DISABLE_FORWARD_INDEX
    //    (b) existing disabled, new re-enables it       → ENABLE_{DICT|RAW}_FORWARD_INDEX (rebuild from dict+inverted)
    //    (c) forward stays on but encoding flips DICT⇄RAW → ENABLE_{DICT|RAW}_FORWARD_INDEX (rewrite in place)
    //  When the dictionary needs to stay (because another index requires it) the encoding-flip handler keeps
    //  it. When the dictionary needs to go, the dict transition step (#2) queues DISABLE_DICTIONARY; the
    //  existing DISABLE_DICTIONARY handler covers the DICT→RAW + drop-dict path on its own, so we don't
    //  queue an ENABLE_RAW_FORWARD_INDEX in that combo to avoid double-rewrite.
    if (existingHasFwd != newHasFwd) {
      if (columnMetadata != null && columnMetadata.isSorted()) {
        LOGGER.warn("Trying to {} the forward index for a sorted column: {} of segment: {}, ignoring",
            existingHasFwd ? "disable" : "enable", column, segmentName);
        return ops;
      }
      if (existingHasFwd) {
        ops.add(Operation.DISABLE_FORWARD_INDEX);
      } else {
        // Re-creating the forward index requires both dict and inverted on disk. After re-enable the dict
        // status stays whatever was needed for the rebuild — independent dict transitions don't run in this
        // combo (the rebuild needs the dict, so dropping it in the same reload is not supported).
        if (!existingHasDict || !existingHasInverted) {
          LOGGER.warn(
              "Trying to enable the forward index for a column: {} of segment: {} missing either the dictionary ({}) "
                  + "and / or the inverted index ({}) is not possible. Either a refresh or back-fill is required to "
                  + "get the forward index, ignoring", column, segmentName, existingHasDict ? "enabled" : "disabled",
              existingHasInverted ? "enabled" : "disabled");
          return ops;
        }
        ops.add(newFwdEncoding == FieldConfig.EncodingType.RAW
            ? Operation.ENABLE_RAW_FORWARD_INDEX : Operation.ENABLE_DICT_FORWARD_INDEX);
        return ops;
      }
    } else if (existingHasFwd && existingFwdEncoding != newFwdEncoding && existingHasDict == desiredDict) {
      // Encoding flip with forward staying on AND the dictionary state staying the same. The other two
      // possibilities — DICT→RAW + drop-dict and RAW→DICT + create-dict — are already handled by the
      // dict-transition step below: DISABLE_DICTIONARY's handler converts dict-encoded forward to raw and
      // drops the dict together, and ENABLE_DICTIONARY's handler builds the dictionary and converts raw
      // forward to dict-encoded. Only the dict-stays cases need this explicit op:
      //   - DICT→RAW with dict kept (because another index requires it, e.g. inverted)
      //   - RAW→DICT with shared dict already present
      if (columnMetadata != null && columnMetadata.isSorted()) {
        LOGGER.warn("Trying to flip forward-index encoding (DICT⇄RAW) for a sorted column: {} of segment: {}, "
            + "ignoring", column, segmentName);
        return ops;
      }
      ops.add(newFwdEncoding == FieldConfig.EncodingType.RAW
          ? Operation.ENABLE_RAW_FORWARD_INDEX : Operation.ENABLE_DICT_FORWARD_INDEX);
    }

    // 2. Dictionary transition.
    if (existingHasDict && !desiredDict) {
      // When the forward index is being kept, the range index can be rebuilt against raw values, additional
      // guards (auto-generated cardinality-1 columns, on-disk inverted/FST) apply via shouldDisableDictionary.
      // When the forward index is also being removed — or was already off — the range index can't be rebuilt,
      // so dropping the dictionary while range stays in the new config leaves the segment inconsistent.
      if (!existingHasFwd) {
        Preconditions.checkState(!newIsRange, String.format(
            "Must disable range index (enabled) to disable the dictionary for a forwardIndexDisabled column: %s "
                + "of segment: %s or refresh / back-fill the forward index", column, segmentName));
        ops.add(Operation.DISABLE_DICTIONARY);
      } else if (ops.contains(Operation.DISABLE_FORWARD_INDEX)) {
        Preconditions.checkState(!newIsRange, String.format(
            "Must disable range index (enabled) to disable the dictionary and forward index for column: %s of "
                + "segment: %s or refresh / back-fill the forward index", column, segmentName));
        ops.add(Operation.DISABLE_DICTIONARY);
      } else if (shouldDisableDictionary(column, columnMetadata, fieldSpec, newConf)) {
        ops.add(Operation.DISABLE_DICTIONARY);
      }
    } else if (!existingHasDict && desiredDict) {
      // Dict can only be created from forward values, so refuse if forward will be off in the final state.
      Preconditions.checkState(existingHasFwd || newHasFwd, String.format(
          "Cannot regenerate the dictionary for column: %s of segment: %s with forward index disabled. Please "
              + "refresh or back-fill the data to add back the forward index", column, segmentName));
      // If an index requires the dict, always enable it. Otherwise apply the optimize-dictionary heuristic so
      // reload doesn't force a dictionary onto a high-cardinality column that segment creation would have kept raw.
      if (dictRequiredByIndex || shouldEnableDictionaryHeuristically(column, fieldSpec, newConf, columnMetadata,
          segmentName)) {
        ops.add(Operation.ENABLE_DICTIONARY);
      }
    }

    // 3. Compression-type change (only when no encoding change happened).
    if (ops.isEmpty() && existingFwdEncoding != null && existingFwdEncoding == newFwdEncoding
        && existingHasDict == desiredDict) {
      if (existingFwdEncoding == FieldConfig.EncodingType.RAW) {
        // TODO: Also check if raw index version needs to be changed
        if (shouldChangeRawCompressionType(column, segmentReader)) {
          ops.add(Operation.CHANGE_INDEX_COMPRESSION_TYPE);
        }
      } else if (shouldChangeDictIdCompressionType(column, segmentReader)) {
        ops.add(Operation.CHANGE_INDEX_COMPRESSION_TYPE);
      }
    }

    return ops;
  }

  /// Optimize-dictionary heuristic for ENABLE_DICTIONARY. Used when the new config wants a dictionary but no
  /// secondary index actually requires it; the heuristic decides whether the column's cardinality / size
  /// characteristics make a dictionary worthwhile, mirroring what segment creation would have chosen.
  private boolean shouldEnableDictionaryHeuristically(String column, FieldSpec fieldSpec, FieldIndexConfigs newConf,
      @Nullable ColumnMetadata colMeta, String segmentName) {
    if (fieldSpec == null || colMeta == null || colMeta.isSorted()) {
      return true;
    }
    boolean keepDict = DictionaryIndexType.ignoreDictionaryOverride(
        _tableConfig.getIndexingConfig().isOptimizeDictionary(),
        _tableConfig.getIndexingConfig().isOptimizeDictionaryForMetrics(),
        _tableConfig.getIndexingConfig().getNoDictionarySizeRatioThreshold(),
        _tableConfig.getIndexingConfig().getNoDictionaryCardinalityRatioThreshold(),
        fieldSpec, newConf, colMeta.getCardinality(), colMeta.getTotalNumberOfEntries());
    if (!keepDict) {
      LOGGER.info("Skipping ENABLE_DICTIONARY for column: {} of segment: {} because optimize-dictionary "
          + "heuristics determined dictionary is not beneficial (cardinality={}, totalNumberOfEntries={})",
          column, segmentName, colMeta.getCardinality(), colMeta.getTotalNumberOfEntries());
    }
    return keepDict;
  }

  private void disableDictionary(String column, SegmentDirectory.Writer segmentWriter, String reason)
      throws Exception {
    // Remove the dictionary and update the metadata to indicate that the dictionary is no longer present
    segmentWriter.removeIndex(column, StandardIndexes.dictionary());
    String segmentName = _segmentDirectory.getSegmentMetadata().getName();
    LOGGER.info("{}: Updating metadata properties for segment={} and column={}", reason, segmentName, column);
    Map<String, String> metadataProperties = new HashMap<>();
    metadataProperties.put(getKeyFor(column, HAS_DICTIONARY), String.valueOf(false));
    metadataProperties.put(getKeyFor(column, FORWARD_INDEX_ENCODING), FieldConfig.EncodingType.RAW.name());
    metadataProperties.put(getKeyFor(column, DICTIONARY_ELEMENT_SIZE), String.valueOf(0));
    // TODO: See https://github.com/apache/pinot/pull/16921 for details
    // metadataProperties.put(getKeyFor(column, BITS_PER_ELEMENT), String.valueOf(-1));
    SegmentMetadataUtils.updateMetadataProperties(_segmentDirectory, metadataProperties);

    // Remove the inverted index, FST index and range index
    removeDictRelatedIndexes(column, segmentWriter);

    LOGGER.info("Removed dictionary and all corresponding indexes for segment: {}, column: {}", segmentName, column);
  }

  private boolean isForwardIndexDictionaryEncoded(String column) {
    return _segmentDirectory.getSegmentMetadata().getColumnMetadataFor(column).getForwardIndexEncoding()
        == FieldConfig.EncodingType.DICTIONARY;
  }

  /// Returns {@code true} when removing the dictionary on this column would leave another enabled index
  /// orphaned. Aggregates {@link IndexType#requiresDictionary} over every enabled index in the new config
  /// via {@link DictionaryIndexConfig#requiresDictionary(FieldSpec, FieldIndexConfigs)}, so this automatically
  /// covers inverted, FST, IFST, and any future dict-requiring index type. Only the new config matters — any
  /// existing on-disk index no longer present in the new config is removed by its own handler later in
  /// preprocessing and does not need to keep the dictionary alive.
  private boolean dictionaryRequiredByOtherIndex(String column, @Nullable FieldSpec fieldSpec,
      FieldIndexConfigs newConf) {
    if (fieldSpec != null && DictionaryIndexConfig.requiresDictionary(fieldSpec, newConf)) {
      LOGGER.warn("Cannot disable dictionary for column={} because at least one enabled index in the new "
          + "config still requires a dictionary.", column);
      return true;
    }
    return false;
  }

  private boolean shouldDisableDictionary(String column, ColumnMetadata existingColumnMetadata,
      @Nullable FieldSpec fieldSpec, FieldIndexConfigs newConf) {
    if (existingColumnMetadata.isAutoGenerated() && existingColumnMetadata.getCardinality() == 1) {
      LOGGER.warn("Cannot disable dictionary for auto-generated column={} with cardinality=1.", column);
      return false;
    }

    if (dictionaryRequiredByOtherIndex(column, fieldSpec, newConf)) {
      return false;
    }

    if (hasIndex(column, StandardIndexes.inverted()) || hasIndex(column, StandardIndexes.fst())) {
      LOGGER.warn("Cannot disable dictionary as column={} has FST index or inverted index or both.", column);
      return false;
    }

    if (existingColumnMetadata.isSorted()) {
      LOGGER.warn("Disabling dictionary for sorted column={}. The sorted index will not be used for query "
          + "filtering (equality/range predicates will fall back to column scans). Consider adding a range index "
          + "on this column to maintain query performance.", column);
    }

    return true;
  }

  private boolean shouldChangeRawCompressionType(String column, SegmentDirectory.Reader segmentReader)
      throws Exception {
    // The compression type for an existing segment can only be determined by reading the forward index header.
    ColumnMetadata existingColMetadata = _segmentDirectory.getSegmentMetadata().getColumnMetadataFor(column);
    ChunkCompressionType existingCompressionType;

    // Get the forward index reader factory and create a reader
    IndexReaderFactory<ForwardIndexReader> readerFactory = StandardIndexes.forward().getReaderFactory();
    try (ForwardIndexReader<?> fwdIndexReader = readerFactory.createIndexReader(segmentReader,
        _fieldIndexConfigs.get(column), existingColMetadata)) {
      existingCompressionType = fwdIndexReader.getCompressionType();
      Preconditions.checkState(existingCompressionType != null,
          "Existing compressionType cannot be null for raw forward index column=" + column);
    }

    // Get the new compression type.
    ChunkCompressionType newCompressionType =
        _fieldIndexConfigs.get(column).getConfig(StandardIndexes.forward()).getChunkCompressionType();

    // Note that default compression type (PASS_THROUGH for metric and LZ4 for dimension) is not considered if the
    // compressionType is not explicitly provided in tableConfig. This is to avoid incorrectly rewriting all the
    // forward indexes during segmentReload when the default compressionType changes.
    return newCompressionType != null && existingCompressionType != newCompressionType;
  }

  private boolean shouldChangeDictIdCompressionType(String column, SegmentDirectory.Reader segmentReader)
      throws Exception {
    // The compression type for an existing segment can only be determined by reading the forward index header.
    ColumnMetadata existingColMetadata = _segmentDirectory.getSegmentMetadata().getColumnMetadataFor(column);
    DictIdCompressionType existingCompressionType;
    // Get the forward index reader factory and create a reader
    IndexReaderFactory<ForwardIndexReader> readerFactory = StandardIndexes.forward().getReaderFactory();
    try (ForwardIndexReader<?> fwdIndexReader = readerFactory.createIndexReader(segmentReader,
        _fieldIndexConfigs.get(column), existingColMetadata)) {
      existingCompressionType = fwdIndexReader.getDictIdCompressionType();
    }

    // Get the new compression type.
    DictIdCompressionType newCompressionType =
        _fieldIndexConfigs.get(column).getConfig(StandardIndexes.forward()).getDictIdCompressionType();
    if (newCompressionType != null && !newCompressionType.isApplicable(existingColMetadata.isSingleValue())) {
      newCompressionType = null;
    }

    return existingCompressionType != newCompressionType;
  }

  private void rewriteForwardIndexForCompressionChange(String column, SegmentDirectory.Writer segmentWriter)
      throws Exception {
    ColumnMetadata existingColMetadata = _segmentDirectory.getSegmentMetadata().getColumnMetadataFor(column);
    boolean isSingleValue = existingColMetadata.isSingleValue();
    boolean isDictionaryEncodedForwardIndex = isForwardIndexDictionaryEncoded(column);

    File indexDir = _segmentDirectory.getSegmentMetadata().getIndexDir();
    String segmentName = _segmentDirectory.getSegmentMetadata().getName();
    File inProgress = new File(indexDir, column + ".fwd.inprogress");
    String fileExtension;
    if (isSingleValue) {
      fileExtension = isDictionaryEncodedForwardIndex
          ? V1Constants.Indexes.UNSORTED_SV_FORWARD_INDEX_FILE_EXTENSION
          : V1Constants.Indexes.RAW_SV_FORWARD_INDEX_FILE_EXTENSION;
    } else {
      fileExtension = isDictionaryEncodedForwardIndex
          ? V1Constants.Indexes.UNSORTED_MV_FORWARD_INDEX_FILE_EXTENSION
          : V1Constants.Indexes.RAW_MV_FORWARD_INDEX_FILE_EXTENSION;
    }
    File fwdIndexFile = new File(indexDir, column + fileExtension);

    if (!inProgress.exists()) {
      // Marker file does not exist, which means last run ended normally.
      // Create a marker file.
      FileUtils.touch(inProgress);
    } else {
      // Marker file exists, which means last run was interrupted.
      // Remove forward index if exists.
      FileUtils.deleteQuietly(fwdIndexFile);
    }

    LOGGER.info("Creating new forward index for segment={} and column={}", segmentName, column);
    rewriteForwardIndexForCompressionChange(column, existingColMetadata, indexDir, segmentWriter);

    // We used the existing forward index to generate a new forward index. The existing forward index will be in V3
    // format and the new forward index will be in V1 format. Remove the existing forward index as it is not needed
    // anymore. Note that removeIndex() will only mark an index for removal and remove the in-memory state. The
    // actual cleanup from columns.psf file will happen when singleFileIndexDirectory.cleanupRemovedIndices() is
    // called during segmentWriter.close().
    segmentWriter.removeIndex(column, StandardIndexes.forward());
    LoaderUtils.writeIndexToV3Format(segmentWriter, column, fwdIndexFile, StandardIndexes.forward());

    // Delete the marker file.
    FileUtils.deleteQuietly(inProgress);

    LOGGER.info("Created forward index for segment: {}, column: {}", segmentName, column);
  }

  private void rewriteForwardIndexForCompressionChange(String column, ColumnMetadata columnMetadata, File indexDir,
      SegmentDirectory.Writer segmentWriter)
      throws Exception {
    // Get the forward index reader factory and create a reader
    IndexReaderFactory<ForwardIndexReader> readerFactory = StandardIndexes.forward().getReaderFactory();
    try (ForwardIndexReader<?> reader = readerFactory.createIndexReader(segmentWriter, _fieldIndexConfigs.get(column),
        columnMetadata)) {
      IndexCreationContext.Builder builder =
          IndexCreationContext.builder().withIndexDir(indexDir).withColumnMetadata(columnMetadata)
              .withTableNameWithType(_tableConfig.getTableName())
              .withContinueOnError(_tableConfig.getIngestionConfig() != null
                  && _tableConfig.getIngestionConfig().isContinueOnError());
      // Encoding flows through ForwardIndexConfig; for compression-change rewrite the encoding does not change so
      // the config in _fieldIndexConfigs already carries the correct encoding.
      // Set entry length info for raw index creators. No need to set this when changing dictionary id compression type.
      if (!reader.isDictionaryEncoded() && !columnMetadata.getDataType().getStoredType().isFixedWidth()) {
        int lengthOfLongestEntry = reader.getLengthOfLongestEntry();
        if (lengthOfLongestEntry < 0) {
          // When this info is not available from the reader, we need to scan the column.
          lengthOfLongestEntry = getMaxRowLength(columnMetadata, reader, null);
        }
        if (columnMetadata.isSingleValue()) {
          builder.withLengthOfLongestEntry(lengthOfLongestEntry);
        } else {
          // For VarByte MV columns like String and Bytes, the storage representation of each row contains the following
          // components:
          // 1. bytes required to store the actual elements of the MV row (A)
          // 2. bytes required to store the number of elements in the MV row (B)
          // 3. bytes required to store the length of each MV element (C)
          //
          // lengthOfLongestEntry = A + B + C
          // maxRowLengthInBytes = A
          int maxNumValuesPerEntry = columnMetadata.getMaxNumberOfMultiValues();
          int maxRowLengthInBytes =
              MultiValueVarByteRawIndexCreator.getMaxRowDataLengthInBytes(lengthOfLongestEntry, maxNumValuesPerEntry);
          builder.withMaxRowLengthInBytes(maxRowLengthInBytes);
        }
      }
      ForwardIndexConfig config = _fieldIndexConfigs.get(column).getConfig(StandardIndexes.forward());
      IndexCreationContext context = builder.build();
      try (ForwardIndexCreator creator = StandardIndexes.forward().createIndexCreator(context, config)) {
        if (!reader.getStoredType().equals(creator.getValueType())) {
          // Creator stored type should match reader stored type for raw columns. We do not support changing datatypes.
          String failureMsg =
              "Unsupported operation to change datatype for column=" + column + " from " + reader.getStoredType()
                  .toString() + " to " + creator.getValueType().toString();
          throw new UnsupportedOperationException(failureMsg);
        }

        int numDocs = columnMetadata.getTotalDocs();
        forwardIndexRewriteHelper(column, columnMetadata, reader, creator, numDocs, null, null);
      }
    }
  }

  private void forwardIndexRewriteHelper(String column, ColumnMetadata existingColumnMetadata,
      ForwardIndexReader<?> reader, ForwardIndexCreator creator, int numDocs,
      @Nullable SegmentDictionaryCreator dictionaryCreator, @Nullable Dictionary dictionaryReader) {
    if (dictionaryReader == null && dictionaryCreator == null) {
      if (reader.isDictionaryEncoded()) {
        Preconditions.checkState(creator.isDictionaryEncoded(), "Cannot change dictionary based forward index to raw "
            + "forward index without providing dictionary reader for column: %s", column);
        // Read dictionary based forward index and write dictionary based forward index.
        forwardIndexReadDictWriteDictHelper(reader, creator, numDocs);
      } else {
        Preconditions.checkState(!creator.isDictionaryEncoded(), "Cannot change raw forward index to dictionary based "
            + "forward index without providing dictionary creator for column: %s", column);
        // Read raw forward index and write raw forward index.
        forwardIndexReadRawWriteRawHelper(column, existingColumnMetadata, reader, creator, numDocs);
      }
    } else if (dictionaryCreator == null) {
      // Read dictionary based forward index and write raw forward index.
      forwardIndexReadDictWriteRawHelper(column, existingColumnMetadata, reader, creator, numDocs, dictionaryReader);
    } else if (dictionaryReader == null) {
      // Read raw forward index and write dictionary based forward index.
      forwardIndexReadRawWriteDictHelper(column, existingColumnMetadata, reader, creator, numDocs, dictionaryCreator);
    } else {
      throw new IllegalStateException("One of dictionary reader or creator should be null for column: %s" + column);
    }
  }

  private <C extends ForwardIndexReaderContext> void forwardIndexReadDictWriteDictHelper(ForwardIndexReader<C> reader,
      ForwardIndexCreator creator, int numDocs) {
    C readerContext = reader.createContext();
    if (reader.isSingleValue()) {
      for (int i = 0; i < numDocs; i++) {
        creator.putDictId(reader.getDictId(i, readerContext));
      }
    } else {
      for (int i = 0; i < numDocs; i++) {
        creator.putDictIdMV(reader.getDictIdMV(i, readerContext));
      }
    }
  }

  private <C extends ForwardIndexReaderContext> void forwardIndexReadRawWriteRawHelper(String column,
      ColumnMetadata existingColumnMetadata, ForwardIndexReader<C> reader, ForwardIndexCreator creator, int numDocs) {
    C readerContext = reader.createContext();
    boolean isSVColumn = reader.isSingleValue();

    switch (reader.getStoredType()) {
      // JSON fields are either stored as string or bytes. No special handling is needed because we make this
      // decision based on the storedType of the reader.
      case INT: {
        for (int i = 0; i < numDocs; i++) {
          if (isSVColumn) {
            int val = reader.getInt(i, readerContext);
            creator.putInt(val);
          } else {
            int[] ints = reader.getIntMV(i, readerContext);
            creator.putIntMV(ints);
          }
        }
        break;
      }
      case LONG: {
        for (int i = 0; i < numDocs; i++) {
          if (isSVColumn) {
            long val = reader.getLong(i, readerContext);
            creator.putLong(val);
          } else {
            long[] longs = reader.getLongMV(i, readerContext);
            creator.putLongMV(longs);
          }
        }
        break;
      }
      case FLOAT: {
        for (int i = 0; i < numDocs; i++) {
          if (isSVColumn) {
            float val = reader.getFloat(i, readerContext);
            creator.putFloat(val);
          } else {
            float[] floats = reader.getFloatMV(i, readerContext);
            creator.putFloatMV(floats);
          }
        }
        break;
      }
      case DOUBLE: {
        for (int i = 0; i < numDocs; i++) {
          if (isSVColumn) {
            double val = reader.getDouble(i, readerContext);
            creator.putDouble(val);
          } else {
            double[] doubles = reader.getDoubleMV(i, readerContext);
            creator.putDoubleMV(doubles);
          }
        }
        break;
      }
      case BIG_DECIMAL: {
        for (int i = 0; i < numDocs; i++) {
          if (isSVColumn) {
            BigDecimal val = reader.getBigDecimal(i, readerContext);
            creator.putBigDecimal(val);
          } else {
            BigDecimal[] bigDecimals = reader.getBigDecimalMV(i, readerContext);
            creator.putBigDecimalMV(bigDecimals);
          }
        }
        break;
      }
      case STRING: {
        for (int i = 0; i < numDocs; i++) {
          if (isSVColumn) {
            String val = reader.getString(i, readerContext);
            creator.putString(val);
          } else {
            String[] strings = reader.getStringMV(i, readerContext);
            creator.putStringMV(strings);
          }
        }
        break;
      }
      case BYTES: {
        for (int i = 0; i < numDocs; i++) {
          if (isSVColumn) {
            byte[] val = reader.getBytes(i, readerContext);
            creator.putBytes(val);
          } else {
            byte[][] bytesArray = reader.getBytesMV(i, readerContext);
            creator.putBytesMV(bytesArray);
          }
        }
        break;
      }
      case MAP: {
        for (int i = 0; i < numDocs; i++) {
          if (isSVColumn) {
            byte[] val = reader.getBytes(i, readerContext);
            creator.putBytes(val);
          } else {
            throw new IllegalStateException("Map is not supported for MV columns");
          }
        }
        break;
      }
      default:
        throw new IllegalStateException("Unsupported storedType=" + reader.getStoredType() + " for column=" + column);
    }
  }

  private <C extends ForwardIndexReaderContext> void forwardIndexReadDictWriteRawHelper(String column,
      ColumnMetadata existingColumnMetadata, ForwardIndexReader<C> reader, ForwardIndexCreator creator, int numDocs,
      Dictionary dictionaryReader) {
    C readerContext = reader.createContext();
    boolean isSVColumn = reader.isSingleValue();
    DataType storedType = dictionaryReader.getValueType().getStoredType();

    switch (storedType) {
      case INT: {
        for (int i = 0; i < numDocs; i++) {
          if (isSVColumn) {
            int dictId = reader.getDictId(i, readerContext);
            int val = dictionaryReader.getIntValue(dictId);
            creator.putInt(val);
          } else {
            int[] dictIds = reader.getDictIdMV(i, readerContext);
            int[] ints = new int[dictIds.length];
            dictionaryReader.readIntValues(dictIds, dictIds.length, ints);
            creator.putIntMV(ints);
          }
        }
        break;
      }
      case LONG: {
        for (int i = 0; i < numDocs; i++) {
          if (isSVColumn) {
            int dictId = reader.getDictId(i, readerContext);
            long val = dictionaryReader.getLongValue(dictId);
            creator.putLong(val);
          } else {
            int[] dictIds = reader.getDictIdMV(i, readerContext);
            long[] longs = new long[dictIds.length];
            dictionaryReader.readLongValues(dictIds, dictIds.length, longs);
            creator.putLongMV(longs);
          }
        }
        break;
      }
      case FLOAT: {
        for (int i = 0; i < numDocs; i++) {
          if (isSVColumn) {
            int dictId = reader.getDictId(i, readerContext);
            float val = dictionaryReader.getFloatValue(dictId);
            creator.putFloat(val);
          } else {
            int[] dictIds = reader.getDictIdMV(i, readerContext);
            float[] floats = new float[dictIds.length];
            dictionaryReader.readFloatValues(dictIds, dictIds.length, floats);
            creator.putFloatMV(floats);
          }
        }
        break;
      }
      case DOUBLE: {
        for (int i = 0; i < numDocs; i++) {
          if (isSVColumn) {
            int dictId = reader.getDictId(i, readerContext);
            double val = dictionaryReader.getDoubleValue(dictId);
            creator.putDouble(val);
          } else {
            int[] dictIds = reader.getDictIdMV(i, readerContext);
            double[] doubles = new double[dictIds.length];
            dictionaryReader.readDoubleValues(dictIds, dictIds.length, doubles);
            creator.putDoubleMV(doubles);
          }
        }
        break;
      }
      case BIG_DECIMAL: {
        for (int i = 0; i < numDocs; i++) {
          if (isSVColumn) {
            int dictId = reader.getDictId(i, readerContext);
            BigDecimal val = dictionaryReader.getBigDecimalValue(dictId);
            creator.putBigDecimal(val);
          } else {
            int[] dictIds = reader.getDictIdMV(i, readerContext);
            BigDecimal[] bigDecimals = new BigDecimal[dictIds.length];
            dictionaryReader.readBigDecimalValues(dictIds, dictIds.length, bigDecimals);
            creator.putBigDecimalMV(bigDecimals);
          }
        }
        break;
      }
      case STRING: {
        for (int i = 0; i < numDocs; i++) {
          if (isSVColumn) {
            int dictId = reader.getDictId(i, readerContext);
            String val = dictionaryReader.getStringValue(dictId);
            creator.putString(val);
          } else {
            int[] dictIds = reader.getDictIdMV(i, readerContext);
            String[] strings = new String[dictIds.length];
            dictionaryReader.readStringValues(dictIds, dictIds.length, strings);
            creator.putStringMV(strings);
          }
        }
        break;
      }
      case BYTES: {
        for (int i = 0; i < numDocs; i++) {
          if (isSVColumn) {
            int dictId = reader.getDictId(i, readerContext);
            byte[] val = dictionaryReader.getBytesValue(dictId);
            creator.putBytes(val);
          } else {
            int[] dictIds = reader.getDictIdMV(i, readerContext);
            byte[][] bytes = new byte[dictIds.length][];
            dictionaryReader.readBytesValues(dictIds, dictIds.length, bytes);
            creator.putBytesMV(bytes);
          }
        }
        break;
      }
      default:
        throw new IllegalStateException("Unsupported storedType=" + storedType + " for column=" + column);
    }
  }

  private void forwardIndexReadRawWriteDictHelper(String column, ColumnMetadata existingColumnMetadata,
      ForwardIndexReader<?> reader, ForwardIndexCreator creator, int numDocs,
      SegmentDictionaryCreator dictionaryCreator) {
    boolean isSVColumn = reader.isSingleValue();
    int maxNumValuesPerEntry = existingColumnMetadata.getMaxNumberOfMultiValues();
    PinotSegmentColumnReader columnReader = new PinotSegmentColumnReader(reader, null, null, maxNumValuesPerEntry);

    for (int i = 0; i < numDocs; i++) {
      Object obj = columnReader.getValue(i);

      if (isSVColumn) {
        int dictId = dictionaryCreator.indexOfSV(obj);
        creator.putDictId(dictId);
      } else {
        int[] dictIds = dictionaryCreator.indexOfMV(obj);
        creator.putDictIdMV(dictIds);
      }
    }
  }

  private void createDictBasedForwardIndex(String column, SegmentDirectory.Writer segmentWriter)
      throws Exception {
    SegmentMetadataImpl segmentMetadata = _segmentDirectory.getSegmentMetadata();
    File indexDir = segmentMetadata.getIndexDir();
    String segmentName = segmentMetadata.getName();
    File inProgress = new File(indexDir, column + ".dict.inprogress");
    File dictionaryFile = new File(indexDir, column + V1Constants.Dict.FILE_EXTENSION);

    ColumnMetadata existingColMetadata = segmentMetadata.getColumnMetadataFor(column);
    FieldSpec fieldSpec = existingColMetadata.getFieldSpec();
    String fwdIndexFileExtension;
    if (fieldSpec.isSingleValueField()) {
      if (existingColMetadata.isSorted()) {
        fwdIndexFileExtension = V1Constants.Indexes.SORTED_SV_FORWARD_INDEX_FILE_EXTENSION;
      } else {
        fwdIndexFileExtension = V1Constants.Indexes.UNSORTED_SV_FORWARD_INDEX_FILE_EXTENSION;
      }
    } else {
      fwdIndexFileExtension = V1Constants.Indexes.UNSORTED_MV_FORWARD_INDEX_FILE_EXTENSION;
    }
    File fwdIndexFile = new File(indexDir, column + fwdIndexFileExtension);

    if (!inProgress.exists()) {
      // Marker file does not exist, which means last run ended normally.
      // Create a marker file.
      FileUtils.touch(inProgress);
    } else {
      // Marker file exists, which means last run was interrupted.
      // Remove forward index and dictionary files if they exist.
      FileUtils.deleteQuietly(fwdIndexFile);
      FileUtils.deleteQuietly(dictionaryFile);
    }

    AbstractColumnStatisticsCollector statsCollector;
    SegmentDictionaryCreator dictionaryCreator;
    try (ForwardIndexReader<?> reader = StandardIndexes.forward()
        .getReaderFactory()
        .createIndexReader(segmentWriter, _fieldIndexConfigs.get(column), existingColMetadata)) {
      assert reader != null;

      LOGGER.info("Creating a new dictionary for segment={} and column={}", segmentName, column);
      int numDocs = existingColMetadata.getTotalDocs();
      statsCollector = getStatsCollector(column, fieldSpec.getDataType().getStoredType(), true);
      // NOTE:
      //   Special null handling is not necessary here. This is because, the existing default null value in the raw
      //   forwardIndex will be retained as such while created the dictionary and dict-based forward index. Also, null
      //   value vectors maintain a bitmap of docIds. No handling is necessary there.
      try (PinotSegmentColumnReader columnReader = new PinotSegmentColumnReader(reader, null, null,
          existingColMetadata.getMaxNumberOfMultiValues())) {
        for (int i = 0; i < numDocs; i++) {
          statsCollector.collect(columnReader.getValue(i));
        }
        statsCollector.seal();
      }
      DictionaryIndexConfig dictConf = _fieldIndexConfigs.get(column).getConfig(StandardIndexes.dictionary());
      boolean optimizeDictionaryType = _tableConfig.getIndexingConfig().isOptimizeDictionaryType();
      boolean useVarLength = dictConf.isUseVarLengthDictionary() || DictionaryIndexType.shouldUseVarLengthDictionary(
          reader.getStoredType(), statsCollector) || (optimizeDictionaryType
          && DictionaryIndexType.optimizeTypeShouldUseVarLengthDictionary(reader.getStoredType(), statsCollector));
      dictionaryCreator = new SegmentDictionaryCreator(fieldSpec, segmentMetadata.getIndexDir(), useVarLength);
      dictionaryCreator.build(statsCollector.getUniqueValuesSet());

      LOGGER.info("Built dictionary. Rewriting dictionary enabled forward index for segment={} and column={}",
          segmentName, column);
      ForwardIndexConfig config = _fieldIndexConfigs.get(column).getConfig(StandardIndexes.forward());
      IndexCreationContext context = IndexCreationContext.builder()
          .withIndexDir(indexDir)
          .withFieldSpec(fieldSpec)
          .withColumnStatistics(statsCollector)
          .withTotalDocs(numDocs)
          .withDictionary(true)
          .withTableNameWithType(_tableConfig.getTableName())
          .withContinueOnError(
              _tableConfig.getIngestionConfig() != null && _tableConfig.getIngestionConfig().isContinueOnError())
          .build();
      try (ForwardIndexCreator creator = StandardIndexes.forward().createIndexCreator(context, config)) {
        forwardIndexRewriteHelper(column, existingColMetadata, reader, creator, numDocs, dictionaryCreator, null);
      }
    }

    // We used the existing forward index to generate a new forward index. The existing forward index will be in V3
    // format and the new forward index will be in V1 format. Remove the existing forward index as it is not needed
    // anymore. Note that removeIndex() will only mark an index for removal and remove the in-memory state. The
    // actual cleanup from columns.psf file will happen when singleFileIndexDirectory.cleanupRemovedIndices() is
    // called during segmentWriter.close().
    segmentWriter.removeIndex(column, StandardIndexes.forward());
    LoaderUtils.writeIndexToV3Format(segmentWriter, column, dictionaryFile, StandardIndexes.dictionary());
    LoaderUtils.writeIndexToV3Format(segmentWriter, column, fwdIndexFile, StandardIndexes.forward());

    LOGGER.info("Created forwardIndex. Updating metadata properties for segment={} and column={}", segmentName, column);
    Map<String, String> metadataProperties = new HashMap<>();
    metadataProperties.put(getKeyFor(column, HAS_DICTIONARY), String.valueOf(true));
    metadataProperties.put(getKeyFor(column, FORWARD_INDEX_ENCODING), FieldConfig.EncodingType.DICTIONARY.name());
    metadataProperties.put(getKeyFor(column, DICTIONARY_ELEMENT_SIZE),
        String.valueOf(dictionaryCreator.getNumBytesPerEntry()));
    // If realtime segments were completed when the column was RAW, the cardinality value is populated as Integer
    // .MIN_VALUE. When dictionary is enabled for this column later, cardinality value should be rightly populated so
    // that the dictionary can be loaded.
    int cardinality = statsCollector.getCardinality();
    metadataProperties.put(getKeyFor(column, CARDINALITY), String.valueOf(cardinality));
    metadataProperties.put(getKeyFor(column, BITS_PER_ELEMENT),
        String.valueOf(PinotDataBitSet.getNumBitsPerValue(cardinality - 1)));
    SegmentMetadataUtils.updateMetadataProperties(_segmentDirectory, metadataProperties);

    // We remove indexes that have to be rewritten when a dictEnabled is toggled. Note that the respective index
    // handler will take care of recreating the index.
    removeDictRelatedIndexes(column, segmentWriter);

    // Delete the marker file.
    FileUtils.deleteQuietly(inProgress);

    LOGGER.info("Created dictionary based forward index for segment: {}, column: {}", segmentName, column);
  }

  private void createDictionaryForRawForwardIndex(String column, SegmentDirectory.Writer segmentWriter)
      throws Exception {
    SegmentMetadataImpl segmentMetadata = _segmentDirectory.getSegmentMetadata();
    File indexDir = segmentMetadata.getIndexDir();
    String segmentName = segmentMetadata.getName();
    File inProgress = new File(indexDir, column + ".rawdict.inprogress");
    File dictionaryFile = new File(indexDir, column + V1Constants.Dict.FILE_EXTENSION);

    if (!inProgress.exists()) {
      FileUtils.touch(inProgress);
    } else {
      FileUtils.deleteQuietly(dictionaryFile);
    }

    ColumnMetadata existingColMetadata = segmentMetadata.getColumnMetadataFor(column);
    FieldSpec fieldSpec = existingColMetadata.getFieldSpec();
    int dictionaryCardinality;
    int dictionaryElementSize;
    try (ForwardIndexReader<?> reader = StandardIndexes.forward().getReaderFactory()
        .createIndexReader(segmentWriter, _fieldIndexConfigs.get(column), existingColMetadata)) {
      AbstractColumnStatisticsCollector statsCollector =
          getStatsCollector(column, fieldSpec.getDataType().getStoredType(), true);
      try (PinotSegmentColumnReader columnReader = new PinotSegmentColumnReader(reader, null, null,
          existingColMetadata.getMaxNumberOfMultiValues())) {
        for (int i = 0; i < existingColMetadata.getTotalDocs(); i++) {
          statsCollector.collect(columnReader.getValue(i));
        }
        statsCollector.seal();
      }
      DictionaryIndexConfig dictConf = _fieldIndexConfigs.get(column).getConfig(StandardIndexes.dictionary());
      boolean optimizeDictionaryType = _tableConfig.getIndexingConfig().isOptimizeDictionaryType();
      boolean useVarLength = dictConf.isUseVarLengthDictionary()
          || DictionaryIndexType.shouldUseVarLengthDictionary(reader.getStoredType(), statsCollector)
          || (optimizeDictionaryType
          && DictionaryIndexType.optimizeTypeShouldUseVarLengthDictionary(reader.getStoredType(), statsCollector));
      try (SegmentDictionaryCreator dictionaryCreator =
          new SegmentDictionaryCreator(fieldSpec, indexDir, useVarLength)) {
        // Capture cardinality from the unique-values set passed to build() rather than from the stats collector,
        // because some collectors over-count by tracking null/duplicate occurrences separately. The dictionary on
        // disk has exactly java.lang.reflect.Array.getLength(uniqueValues) entries.
        Object uniqueValues = statsCollector.getUniqueValuesSet();
        dictionaryCardinality = java.lang.reflect.Array.getLength(uniqueValues);
        dictionaryCreator.build(uniqueValues);
        dictionaryElementSize = dictionaryCreator.getNumBytesPerEntry();
      }
    }

    LoaderUtils.writeIndexToV3Format(segmentWriter, column, dictionaryFile, StandardIndexes.dictionary());

    Preconditions.checkState(dictionaryCardinality > 0,
        "Dictionary cardinality for column %s must be > 0 to compute BITS_PER_ELEMENT", column);
    Map<String, String> metadataProperties = new HashMap<>();
    metadataProperties.put(getKeyFor(column, HAS_DICTIONARY), String.valueOf(true));
    metadataProperties.put(getKeyFor(column, FORWARD_INDEX_ENCODING), FieldConfig.EncodingType.RAW.name());
    metadataProperties.put(getKeyFor(column, DICTIONARY_ELEMENT_SIZE), String.valueOf(dictionaryElementSize));
    metadataProperties.put(getKeyFor(column, CARDINALITY), String.valueOf(dictionaryCardinality));
    metadataProperties.put(getKeyFor(column, BITS_PER_ELEMENT),
        String.valueOf(PinotDataBitSet.getNumBitsPerValue(dictionaryCardinality - 1)));
    SegmentMetadataUtils.updateMetadataProperties(_segmentDirectory, metadataProperties);

    // Secondary indexes that require dictionary semantics should be rebuilt against the new shared dictionary.
    removeDictRelatedIndexes(column, segmentWriter);

    // Delete the inprogress marker only after the cleanup is complete. If the JVM dies between the metadata write
    // and the secondary-index removal, the marker still exists; on next preprocess `inProgress.exists()` deletes
    // the partial dictionary file and the rebuild starts fresh.
    FileUtils.deleteQuietly(inProgress);

    LOGGER.info("Created dictionary for raw forward index segment={} column={}", segmentName, column);
  }

  static void removeDictRelatedIndexes(String column, SegmentDirectory.Writer segmentWriter) {
    // TODO: Move this logic as a static function in each index creator.

    // Remove all dictionary related indexes. They will be recreated if necessary by the respective handlers. Note that
    // the remove index call will be a no-op if the index doesn't exist.
    DICTIONARY_BASED_INDEXES_TO_REWRITE.forEach((index) -> segmentWriter.removeIndex(column, index));
  }

  private void disableDictionaryAndCreateRawForwardIndex(String column, SegmentDirectory.Writer segmentWriter)
      throws Exception {
    ColumnMetadata existingColMetadata = _segmentDirectory.getSegmentMetadata().getColumnMetadataFor(column);
    boolean isSingleValue = existingColMetadata.isSingleValue();

    File indexDir = _segmentDirectory.getSegmentMetadata().getIndexDir();
    String segmentName = _segmentDirectory.getSegmentMetadata().getName();
    File inProgress = new File(indexDir, column + ".fwd.inprogress");
    String fileExtension = isSingleValue ? V1Constants.Indexes.RAW_SV_FORWARD_INDEX_FILE_EXTENSION
        : V1Constants.Indexes.RAW_MV_FORWARD_INDEX_FILE_EXTENSION;
    File fwdIndexFile = new File(indexDir, column + fileExtension);

    if (!inProgress.exists()) {
      // Marker file does not exist, which means last run ended normally.
      // Create a marker file.
      FileUtils.touch(inProgress);
    } else {
      // Marker file exists, which means last run was interrupted.
      // Remove forward index if exists.
      FileUtils.deleteQuietly(fwdIndexFile);
    }

    LOGGER.info("Creating raw forward index for segment={} and column={}", segmentName, column);
    rewriteDictToRawForwardIndex(existingColMetadata, segmentWriter, indexDir);

    // Remove dictionary and forward index
    segmentWriter.removeIndex(column, StandardIndexes.forward());
    segmentWriter.removeIndex(column, StandardIndexes.dictionary());
    LoaderUtils.writeIndexToV3Format(segmentWriter, column, fwdIndexFile, StandardIndexes.forward());

    LOGGER.info("Created raw forwardIndex. Updating metadata properties for segment={} and column={}", segmentName,
        column);
    Map<String, String> metadataProperties = new HashMap<>();
    metadataProperties.put(getKeyFor(column, HAS_DICTIONARY), String.valueOf(false));
    metadataProperties.put(getKeyFor(column, FORWARD_INDEX_ENCODING), FieldConfig.EncodingType.RAW.name());
    metadataProperties.put(getKeyFor(column, DICTIONARY_ELEMENT_SIZE), String.valueOf(0));
    // TODO: See https://github.com/apache/pinot/pull/16921 for details
    // metadataProperties.put(getKeyFor(column, BITS_PER_ELEMENT), String.valueOf(-1));
    SegmentMetadataUtils.updateMetadataProperties(_segmentDirectory, metadataProperties);

    // Remove range index, inverted index and FST index.
    removeDictRelatedIndexes(column, segmentWriter);

    // Delete marker file.
    FileUtils.deleteQuietly(inProgress);

    LOGGER.info("Created raw based forward index for segment: {}, column: {}", segmentName, column);
  }

  /// Flip a dict-encoded forward index to RAW while keeping the dictionary intact. Used when an enabled
  /// secondary index (e.g. inverted, FST, IFST) requires the dictionary to remain after the encoding flip —
  /// the secondary index continues to operate against the same dict ids, so it does not need to be removed
  /// or rebuilt. Compare with {@link #disableDictionaryAndCreateRawForwardIndex}, which performs the same
  /// rewrite and additionally drops the dictionary plus all dict-dependent indexes.
  private void convertDictForwardToRawKeepingDictionary(String column, SegmentDirectory.Writer segmentWriter)
      throws Exception {
    ColumnMetadata existingColMetadata = _segmentDirectory.getSegmentMetadata().getColumnMetadataFor(column);
    boolean isSingleValue = existingColMetadata.isSingleValue();

    File indexDir = _segmentDirectory.getSegmentMetadata().getIndexDir();
    String segmentName = _segmentDirectory.getSegmentMetadata().getName();
    File inProgress = new File(indexDir, column + ".fwdraw.inprogress");
    String fileExtension = isSingleValue ? V1Constants.Indexes.RAW_SV_FORWARD_INDEX_FILE_EXTENSION
        : V1Constants.Indexes.RAW_MV_FORWARD_INDEX_FILE_EXTENSION;
    File fwdIndexFile = new File(indexDir, column + fileExtension);

    if (!inProgress.exists()) {
      FileUtils.touch(inProgress);
    } else {
      FileUtils.deleteQuietly(fwdIndexFile);
    }

    LOGGER.info("Converting dict-encoded forward index to raw (keeping dictionary) for segment={} column={}",
        segmentName, column);
    rewriteDictToRawForwardIndex(existingColMetadata, segmentWriter, indexDir);

    // Remove the old dict-encoded forward index file and write the new raw forward index in its place.
    // Crucially we do NOT remove the dictionary or any dict-dependent secondary indexes — the dictionary stays
    // and those indexes remain valid against unchanged dict ids.
    segmentWriter.removeIndex(column, StandardIndexes.forward());
    LoaderUtils.writeIndexToV3Format(segmentWriter, column, fwdIndexFile, StandardIndexes.forward());

    Map<String, String> metadataProperties = new HashMap<>();
    metadataProperties.put(getKeyFor(column, FORWARD_INDEX_ENCODING), FieldConfig.EncodingType.RAW.name());
    SegmentMetadataUtils.updateMetadataProperties(_segmentDirectory, metadataProperties);

    FileUtils.deleteQuietly(inProgress);
    LOGGER.info("Converted forward index to raw (dictionary kept) for segment: {}, column: {}", segmentName, column);
  }

  private void rewriteDictToRawForwardIndex(ColumnMetadata columnMetadata, SegmentDirectory.Writer segmentWriter,
      File indexDir)
      throws Exception {
    String column = columnMetadata.getColumnName();
    // Get the forward index reader factory and create a reader
    IndexReaderFactory<ForwardIndexReader> readerFactory = StandardIndexes.forward().getReaderFactory();
    try (ForwardIndexReader<?> reader = readerFactory.createIndexReader(segmentWriter, _fieldIndexConfigs.get(column),
        columnMetadata)) {
      Dictionary dictionary = DictionaryIndexType.read(segmentWriter, columnMetadata);
      IndexCreationContext.Builder builder =
          IndexCreationContext.builder().withIndexDir(indexDir).withColumnMetadata(columnMetadata)
              .withTableNameWithType(_tableConfig.getTableName())
              .withContinueOnError(_tableConfig.getIngestionConfig() != null
                  && _tableConfig.getIngestionConfig().isContinueOnError());
      builder.withDictionary(false);
      // Encoding flows through ForwardIndexConfig set below.
      if (!columnMetadata.getDataType().getStoredType().isFixedWidth()) {
        if (columnMetadata.isSingleValue()) {
          // lengthOfLongestEntry is available for dict columns from metadata.
          builder.withLengthOfLongestEntry(columnMetadata.getLengthOfLongestElement());
        } else {
          // maxRowLength can only be determined by scanning the column.
          builder.withMaxRowLengthInBytes(getMaxRowLength(columnMetadata, reader, dictionary));
        }
      }
      ForwardIndexConfig config = _fieldIndexConfigs.get(column).getConfig(StandardIndexes.forward());
      IndexCreationContext context = builder.build();
      try (ForwardIndexCreator creator = StandardIndexes.forward().createIndexCreator(context, config)) {
        forwardIndexRewriteHelper(column, columnMetadata, reader, creator, columnMetadata.getTotalDocs(), null,
            dictionary);
      }
    }
  }

  /**
   * Returns the max row length for a column.
   * - For SV column, this is the length of the longest value.
   * - For MV column, this is the length of the longest MV entry (sum of lengths of all elements).
   */
  private int getMaxRowLength(ColumnMetadata columnMetadata, ForwardIndexReader<?> forwardIndex,
      @Nullable Dictionary dictionary)
      throws IOException {
    String column = columnMetadata.getColumnName();
    DataType storedType = columnMetadata.getDataType().getStoredType();
    assert !storedType.isFixedWidth();

    try (PinotSegmentColumnReader columnReader = new PinotSegmentColumnReader(forwardIndex, dictionary, null,
        columnMetadata.getMaxNumberOfMultiValues())) {
      AbstractColumnStatisticsCollector statsCollector = getStatsCollector(column, storedType);
      int numDocs = columnMetadata.getTotalDocs();
      for (int i = 0; i < numDocs; i++) {
        statsCollector.collect(columnReader.getValue(i));
      }
      // NOTE: No need to seal the stats collector because value length is updated while collecting stats.
      return columnMetadata.isSingleValue() ? statsCollector.getLengthOfLongestElement()
          : statsCollector.getMaxRowLengthInBytes();
    }
  }

  private AbstractColumnStatisticsCollector getStatsCollector(String column, DataType storedType) {
    return getStatsCollector(column, storedType, false);
  }

  /// `requireUniqueValues=true` forces a per-type collector even when the no-dict optimization would apply.
  /// Callers building a dictionary out of the raw values must opt in — `_fieldIndexConfigs` may still report
  /// `dictionary=disabled` when the dictionary requirement was derived from a secondary-index need rather
  /// than user config (e.g. legacy `invertedIndexColumns` triggering ENABLE_DICTIONARY in computeOperations),
  /// in which case the optimized no-dict collector would be picked and `getUniqueValuesSet()` returns null.
  private AbstractColumnStatisticsCollector getStatsCollector(String column, DataType storedType,
      boolean requireUniqueValues) {
    StatsCollectorConfig statsCollectorConfig = new StatsCollectorConfig(_tableConfig, _schema, null);
    boolean dictionaryEnabled = hasIndex(column, StandardIndexes.dictionary());
    // MAP collector is optimised for no-dictionary collection.
    if (!requireUniqueValues && !dictionaryEnabled && storedType != DataType.MAP) {
      if (ClusterConfigForTable.useOptimizedNoDictCollector(_tableConfig)) {
        return new NoDictColumnStatisticsCollector(column, statsCollectorConfig);
      }
    }
    switch (storedType) {
      case INT:
        return new IntColumnPreIndexStatsCollector(column, statsCollectorConfig);
      case LONG:
        return new LongColumnPreIndexStatsCollector(column, statsCollectorConfig);
      case FLOAT:
        return new FloatColumnPreIndexStatsCollector(column, statsCollectorConfig);
      case DOUBLE:
        return new DoubleColumnPreIndexStatsCollector(column, statsCollectorConfig);
      case BIG_DECIMAL:
        return new BigDecimalColumnPreIndexStatsCollector(column, statsCollectorConfig);
      case STRING:
        return new StringColumnPreIndexStatsCollector(column, statsCollectorConfig);
      case BYTES:
        return new BytesColumnPreIndexStatsCollector(column, statsCollectorConfig);
      case MAP:
        return new MapColumnPreIndexStatsCollector(column, statsCollectorConfig);
      default:
        throw new IllegalStateException("Unsupported storedType=" + storedType + " for column=" + column);
    }
  }

  private boolean hasIndex(String column, IndexType<?, ?, ?> indexType) {
    return _fieldIndexConfigs.get(column).getConfig(indexType).isEnabled();
  }
}
