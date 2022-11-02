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
package org.apache.pinot.segment.local.indexsegment.mutable;

import com.google.common.base.Preconditions;
import it.unimi.dsi.fastutil.ints.IntArrays;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import javax.annotation.Nullable;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.common.metrics.ServerMeter;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.request.context.FunctionContext;
import org.apache.pinot.common.request.context.RequestContextUtils;
import org.apache.pinot.segment.local.aggregator.ValueAggregator;
import org.apache.pinot.segment.local.aggregator.ValueAggregatorFactory;
import org.apache.pinot.segment.local.dedup.PartitionDedupMetadataManager;
import org.apache.pinot.segment.local.realtime.impl.RealtimeSegmentConfig;
import org.apache.pinot.segment.local.realtime.impl.RealtimeSegmentStatsHistory;
import org.apache.pinot.segment.local.realtime.impl.dictionary.BaseOffHeapMutableDictionary;
import org.apache.pinot.segment.local.realtime.impl.geospatial.MutableH3Index;
import org.apache.pinot.segment.local.realtime.impl.invertedindex.NativeMutableFSTIndex;
import org.apache.pinot.segment.local.realtime.impl.invertedindex.NativeMutableTextIndex;
import org.apache.pinot.segment.local.realtime.impl.invertedindex.RealtimeLuceneIndexRefreshState;
import org.apache.pinot.segment.local.realtime.impl.invertedindex.RealtimeLuceneTextIndex;
import org.apache.pinot.segment.local.realtime.impl.nullvalue.MutableNullValueVector;
import org.apache.pinot.segment.local.segment.index.datasource.ImmutableDataSource;
import org.apache.pinot.segment.local.segment.index.datasource.MutableDataSource;
import org.apache.pinot.segment.local.segment.readers.PinotSegmentColumnReader;
import org.apache.pinot.segment.local.segment.readers.PinotSegmentRecordReader;
import org.apache.pinot.segment.local.segment.store.TextIndexUtils;
import org.apache.pinot.segment.local.segment.virtualcolumn.VirtualColumnContext;
import org.apache.pinot.segment.local.segment.virtualcolumn.VirtualColumnProvider;
import org.apache.pinot.segment.local.segment.virtualcolumn.VirtualColumnProviderFactory;
import org.apache.pinot.segment.local.upsert.PartitionUpsertMetadataManager;
import org.apache.pinot.segment.local.upsert.RecordInfo;
import org.apache.pinot.segment.local.utils.FixedIntArrayOffHeapIdMap;
import org.apache.pinot.segment.local.utils.GeometrySerializer;
import org.apache.pinot.segment.local.utils.IdMap;
import org.apache.pinot.segment.local.utils.IngestionUtils;
import org.apache.pinot.segment.local.utils.TableConfigUtils;
import org.apache.pinot.segment.spi.AggregationFunctionType;
import org.apache.pinot.segment.spi.MutableSegment;
import org.apache.pinot.segment.spi.SegmentMetadata;
import org.apache.pinot.segment.spi.datasource.DataSource;
import org.apache.pinot.segment.spi.index.IndexingOverrides;
import org.apache.pinot.segment.spi.index.creator.H3IndexConfig;
import org.apache.pinot.segment.spi.index.metadata.SegmentMetadataImpl;
import org.apache.pinot.segment.spi.index.mutable.MutableDictionary;
import org.apache.pinot.segment.spi.index.mutable.MutableForwardIndex;
import org.apache.pinot.segment.spi.index.mutable.MutableInvertedIndex;
import org.apache.pinot.segment.spi.index.mutable.MutableJsonIndex;
import org.apache.pinot.segment.spi.index.mutable.MutableTextIndex;
import org.apache.pinot.segment.spi.index.mutable.ThreadSafeMutableRoaringBitmap;
import org.apache.pinot.segment.spi.index.mutable.provider.MutableIndexContext;
import org.apache.pinot.segment.spi.index.mutable.provider.MutableIndexProvider;
import org.apache.pinot.segment.spi.index.reader.BloomFilterReader;
import org.apache.pinot.segment.spi.index.reader.RangeIndexReader;
import org.apache.pinot.segment.spi.index.startree.StarTreeV2;
import org.apache.pinot.segment.spi.memory.PinotDataBufferMemoryManager;
import org.apache.pinot.segment.spi.partition.PartitionFunction;
import org.apache.pinot.spi.config.table.ColumnPartitionConfig;
import org.apache.pinot.spi.config.table.FieldConfig;
import org.apache.pinot.spi.config.table.JsonIndexConfig;
import org.apache.pinot.spi.config.table.SegmentPartitionConfig;
import org.apache.pinot.spi.config.table.UpsertConfig;
import org.apache.pinot.spi.config.table.ingestion.AggregationConfig;
import org.apache.pinot.spi.data.DimensionFieldSpec;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.MetricFieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.data.readers.PrimaryKey;
import org.apache.pinot.spi.stream.RowMetadata;
import org.apache.pinot.spi.utils.ByteArray;
import org.apache.pinot.spi.utils.FixedIntArray;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.roaringbitmap.BatchIterator;
import org.roaringbitmap.buffer.MutableRoaringBitmap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.pinot.spi.data.FieldSpec.DataType.BYTES;
import static org.apache.pinot.spi.data.FieldSpec.DataType.STRING;


@SuppressWarnings({"rawtypes", "unchecked"})
public class MutableSegmentImpl implements MutableSegment {

  private static final String RECORD_ID_MAP = "__recordIdMap__";
  private static final int EXPECTED_COMPRESSION = 1000;
  private static final int MIN_ROWS_TO_INDEX = 1000_000; // Min size of recordIdMap for updatable metrics.
  private static final int MIN_RECORD_ID_MAP_CACHE_SIZE = 10000; // Min overflow map size for updatable metrics.

  private final Logger _logger;
  private final long _startTimeMillis = System.currentTimeMillis();
  private final ServerMetrics _serverMetrics;

  private final String _realtimeTableName;
  private final String _segmentName;
  private final Schema _schema;
  private final String _timeColumnName;
  private final int _capacity;
  private final SegmentMetadata _segmentMetadata;
  private final boolean _offHeap;
  private final PinotDataBufferMemoryManager _memoryManager;
  private final RealtimeSegmentStatsHistory _statsHistory;
  private final String _partitionColumn;
  private final PartitionFunction _partitionFunction;
  private final boolean _nullHandlingEnabled;

  private final Map<String, IndexContainer> _indexContainerMap = new HashMap<>();

  private final IdMap<FixedIntArray> _recordIdMap;

  private volatile int _numDocsIndexed = 0;
  private final int _numKeyColumns;

  // Cache the physical (non-virtual) field specs
  private final Collection<FieldSpec> _physicalFieldSpecs;
  private final Collection<DimensionFieldSpec> _physicalDimensionFieldSpecs;
  private final Collection<MetricFieldSpec> _physicalMetricFieldSpecs;
  private final Collection<String> _physicalTimeColumnNames;

  private final List<FieldConfig> _fieldConfigList;

  // default message metadata
  private volatile long _lastIndexedTimeMs = Long.MIN_VALUE;
  private volatile long _latestIngestionTimeMs = Long.MIN_VALUE;

  private RealtimeLuceneIndexRefreshState.RealtimeLuceneReaders _realtimeLuceneReaders;

  private final UpsertConfig.Mode _upsertMode;
  private final String _upsertComparisonColumn;
  private final PartitionUpsertMetadataManager _partitionUpsertMetadataManager;
  private final PartitionDedupMetadataManager _partitionDedupMetadataManager;
  // The valid doc ids are maintained locally instead of in the upsert metadata manager because:
  // 1. There is only one consuming segment per partition, the committed segments do not need to modify the valid doc
  //    ids for the consuming segment.
  // 2. During the segment commitment, when loading the immutable version of this segment, in order to keep the result
  //    correct, the valid doc ids should not be changed, only the record location should be changed.
  // FIXME: There is a corner case for this approach which could cause inconsistency. When there is segment load during
  //        consumption with newer timestamp (late event in consuming segment), the record location will be updated, but
  //        the valid doc ids won't be updated.
  private final ThreadSafeMutableRoaringBitmap _validDocIds;

  public MutableSegmentImpl(RealtimeSegmentConfig config, @Nullable ServerMetrics serverMetrics) {
    _serverMetrics = serverMetrics;
    _realtimeTableName = config.getTableNameWithType();
    _segmentName = config.getSegmentName();
    _schema = config.getSchema();
    _timeColumnName = config.getTimeColumnName();
    _fieldConfigList = config.getFieldConfigList();
    _capacity = config.getCapacity();
    SegmentZKMetadata segmentZKMetadata = config.getSegmentZKMetadata();
    _segmentMetadata = new SegmentMetadataImpl(TableNameBuilder.extractRawTableName(_realtimeTableName),
        segmentZKMetadata.getSegmentName(), _schema, segmentZKMetadata.getCreationTime()) {
      @Override
      public int getTotalDocs() {
        return _numDocsIndexed;
      }

      @Override
      public long getLastIndexedTimestamp() {
        return _lastIndexedTimeMs;
      }

      @Override
      public long getLatestIngestionTimestamp() {
        return _latestIngestionTimeMs;
      }
    };

    _offHeap = config.isOffHeap();
    _memoryManager = config.getMemoryManager();
    _statsHistory = config.getStatsHistory();
    _partitionColumn = config.getPartitionColumn();
    _partitionFunction = config.getPartitionFunction();
    _nullHandlingEnabled = config.isNullHandlingEnabled();

    Collection<FieldSpec> allFieldSpecs = _schema.getAllFieldSpecs();
    List<FieldSpec> physicalFieldSpecs = new ArrayList<>(allFieldSpecs.size());
    List<DimensionFieldSpec> physicalDimensionFieldSpecs = new ArrayList<>(_schema.getDimensionNames().size());
    List<MetricFieldSpec> physicalMetricFieldSpecs = new ArrayList<>(_schema.getMetricNames().size());
    List<String> physicalTimeColumnNames = new ArrayList<>();

    for (FieldSpec fieldSpec : allFieldSpecs) {
      if (!fieldSpec.isVirtualColumn()) {
        physicalFieldSpecs.add(fieldSpec);
        FieldSpec.FieldType fieldType = fieldSpec.getFieldType();
        if (fieldType == FieldSpec.FieldType.DIMENSION) {
          physicalDimensionFieldSpecs.add((DimensionFieldSpec) fieldSpec);
        } else if (fieldType == FieldSpec.FieldType.METRIC) {
          physicalMetricFieldSpecs.add((MetricFieldSpec) fieldSpec);
        } else if (fieldType == FieldSpec.FieldType.DATE_TIME || fieldType == FieldSpec.FieldType.TIME) {
          physicalTimeColumnNames.add(fieldSpec.getName());
        }
      }
    }
    _physicalFieldSpecs = Collections.unmodifiableCollection(physicalFieldSpecs);
    _physicalDimensionFieldSpecs = Collections.unmodifiableCollection(physicalDimensionFieldSpecs);
    _physicalMetricFieldSpecs = Collections.unmodifiableCollection(physicalMetricFieldSpecs);
    _physicalTimeColumnNames = Collections.unmodifiableCollection(physicalTimeColumnNames);

    _numKeyColumns = _physicalDimensionFieldSpecs.size() + _physicalTimeColumnNames.size();

    _logger =
        LoggerFactory.getLogger(MutableSegmentImpl.class.getName() + "_" + _segmentName + "_" + config.getStreamName());

    Set<String> noDictionaryColumns = config.getNoDictionaryColumns();
    Set<String> invertedIndexColumns = config.getInvertedIndexColumns();
    Set<String> textIndexColumns = config.getTextIndexColumns();
    Set<String> fstIndexColumns = config.getFSTIndexColumns();
    Map<String, JsonIndexConfig> jsonIndexConfigs = config.getJsonIndexConfigs();
    Map<String, H3IndexConfig> h3IndexConfigs = config.getH3IndexConfigs();

    int avgNumMultiValues = config.getAvgNumMultiValues();

    // Metric aggregation can be enabled only if config is specified, and all dimensions have dictionary,
    // and no metrics have dictionary. If not enabled, the map returned is null.
    _recordIdMap = enableMetricsAggregationIfPossible(config, noDictionaryColumns);

    Map<String, Pair<String, ValueAggregator>> metricsAggregators = Collections.emptyMap();
    if (_recordIdMap != null) {
      metricsAggregators = getMetricsAggregators(config);
    }

    // Initialize for each column
    for (FieldSpec fieldSpec : _physicalFieldSpecs) {
      String column = fieldSpec.getName();
      boolean isDictionary = !isNoDictionaryColumn(noDictionaryColumns, invertedIndexColumns, fieldSpec, column);
      MutableIndexContext.Common context =
          MutableIndexContext.builder().withFieldSpec(fieldSpec).withMemoryManager(_memoryManager)
              .withDictionary(isDictionary).withCapacity(_capacity).offHeap(_offHeap).withSegmentName(_segmentName)
              .build();

      // Partition info
      PartitionFunction partitionFunction = null;
      Set<Integer> partitions = null;
      if (column.equals(_partitionColumn)) {
        partitionFunction = _partitionFunction;

        // NOTE: Use a concurrent set because the partitions can be updated when the partition of the ingested record
        //       does not match the stream partition. This could happen when stream partition changes, or the records
        //       are not properly partitioned from the stream. Log an warning and emit a metric if it happens, then add
        //       the new partition into this set.
        partitions = ConcurrentHashMap.newKeySet();
        partitions.add(config.getPartitionId());
      }

      // Check whether to generate raw index for the column while consuming
      // Only support generating raw index on single-value columns that do not have inverted index while
      // consuming. After consumption completes and the segment is built, all single-value columns can have raw index
      DataType storedType = fieldSpec.getDataType().getStoredType();
      boolean isFixedWidthColumn = storedType.isFixedWidth();
      MutableIndexProvider indexProvider = IndexingOverrides.getMutableIndexProvider();
      MutableForwardIndex forwardIndex = indexProvider.newForwardIndex(context.forForwardIndex(avgNumMultiValues));

      // Dictionary-encoded column
      MutableDictionary dictionary = null;
      if (isDictionary) {
        int dictionaryColumnSize =
            isFixedWidthColumn ? storedType.size() : _statsHistory.getEstimatedAvgColSize(column);
        // NOTE: preserve 10% buffer for cardinality to reduce the chance of re-sizing the dictionary
        int estimatedCardinality = (int) (_statsHistory.getEstimatedCardinality(column) * 1.1);
        dictionary = indexProvider.newDictionary(context.forDictionary(dictionaryColumnSize, estimatedCardinality));
        // Even though the column is defined as 'no-dictionary' in the config, we did create dictionary for consuming
        // segment.
        noDictionaryColumns.remove(column);
      }

      // Inverted index
      MutableInvertedIndex invertedIndexReader =
          invertedIndexColumns.contains(column) ? indexProvider.newInvertedIndex(context.forInvertedIndex()) : null;

      MutableTextIndex fstIndex = null;
      // FST Index
      if (_fieldConfigList != null && fstIndexColumns.contains(column)) {
        for (FieldConfig fieldConfig : _fieldConfigList) {
          if (fieldConfig.getName().equals(column)) {
            Map<String, String> properties = fieldConfig.getProperties();
            if (TextIndexUtils.isFstTypeNative(properties)) {
              fstIndex = new NativeMutableFSTIndex(column);
            }
          }
        }
      }

      // Text index
      MutableTextIndex textIndex;
      List<String> stopWordsInclude = null;
      List<String> stopWordsExclude = null;
      if (textIndexColumns.contains(column)) {
        boolean useNativeTextIndex = false;
        if (_fieldConfigList != null) {
          for (FieldConfig fieldConfig : _fieldConfigList) {
            if (fieldConfig.getName().equals(column)) {
              Map<String, String> properties = fieldConfig.getProperties();
              stopWordsInclude = TextIndexUtils.extractStopWordsInclude(properties);
              stopWordsExclude = TextIndexUtils.extractStopWordsExclude(properties);
              if (TextIndexUtils.isFstTypeNative(properties)) {
                useNativeTextIndex = true;
              }
            }
          }
        }

        if (useNativeTextIndex) {
          textIndex = new NativeMutableTextIndex(column);
        } else {

          // TODO - this logic is in the wrong place and belongs in a Lucene-specific submodule,
          //  it is beyond the scope of realtime index pluggability to do this refactoring, so realtime
          //  text indexes remain statically defined. Revisit this after this refactoring has been done.
          RealtimeLuceneTextIndex luceneTextIndex =
              new RealtimeLuceneTextIndex(column, new File(config.getConsumerDir()), _segmentName,
                  stopWordsInclude, stopWordsExclude);
          if (_realtimeLuceneReaders == null) {
            _realtimeLuceneReaders = new RealtimeLuceneIndexRefreshState.RealtimeLuceneReaders(_segmentName);
          }
          _realtimeLuceneReaders.addReader(luceneTextIndex);
          textIndex = luceneTextIndex;
        }
      } else {
        textIndex = null;
      }

      // Json index
      JsonIndexConfig jsonIndexConfig = jsonIndexConfigs.get(column);
      MutableJsonIndex jsonIndex =
          jsonIndexConfig != null ? indexProvider.newJsonIndex(context.forJsonIndex(jsonIndexConfig)) : null;

      // H3 index
      // TODO consider making this overridable
      MutableH3Index h3Index;
      try {
        H3IndexConfig h3IndexConfig = h3IndexConfigs.get(column);
        h3Index = h3IndexConfig != null ? new MutableH3Index(h3IndexConfig.getResolution()) : null;
      } catch (IOException e) {
        throw new RuntimeException(String.format("Failed to initiate H3 index for column: %s", column), e);
      }

      // Null value vector
      MutableNullValueVector nullValueVector = _nullHandlingEnabled ? new MutableNullValueVector() : null;

      Pair<String, ValueAggregator> columnAggregatorPair =
          metricsAggregators.getOrDefault(column, Pair.of(column, null));
      String sourceColumn = columnAggregatorPair.getLeft();
      ValueAggregator valueAggregator = columnAggregatorPair.getRight();

      // TODO: Support range index and bloom filter for mutable segment
      _indexContainerMap.put(column,
          new IndexContainer(fieldSpec, partitionFunction, partitions, new NumValuesInfo(), forwardIndex, dictionary,
              invertedIndexReader, null, textIndex, fstIndex, jsonIndex, h3Index, null, nullValueVector, sourceColumn,
              valueAggregator));
    }

    // TODO separate concerns: this logic does not belong here
    if (_realtimeLuceneReaders != null) {
      // add the realtime lucene index readers to the global queue for refresh task to pick up
      RealtimeLuceneIndexRefreshState realtimeLuceneIndexRefreshState = RealtimeLuceneIndexRefreshState.getInstance();
      realtimeLuceneIndexRefreshState.addRealtimeReadersToQueue(_realtimeLuceneReaders);
    }

    // init upsert-related data structure
    _upsertMode = config.getUpsertMode();
    _partitionDedupMetadataManager = config.getPartitionDedupMetadataManager();

    if (isUpsertEnabled()) {
      Preconditions.checkState(!isAggregateMetricsEnabled(),
          "Metrics aggregation and upsert cannot be enabled together");
      _partitionUpsertMetadataManager = config.getPartitionUpsertMetadataManager();
      _validDocIds = new ThreadSafeMutableRoaringBitmap();
      String upsertComparisonColumn = config.getUpsertComparisonColumn();
      _upsertComparisonColumn = upsertComparisonColumn != null ? upsertComparisonColumn : _timeColumnName;
    } else {
      _partitionUpsertMetadataManager = null;
      _validDocIds = null;
      _upsertComparisonColumn = null;
    }
  }

  /**
   * Decide whether a given column should be dictionary encoded or not
   * @param noDictionaryColumns no dictionary column set
   * @param invertedIndexColumns inverted index column set
   * @param fieldSpec field spec of column
   * @param column column name
   * @return true if column is no-dictionary, false if dictionary encoded
   */
  private boolean isNoDictionaryColumn(Set<String> noDictionaryColumns, Set<String> invertedIndexColumns,
      FieldSpec fieldSpec, String column) {
    DataType dataType = fieldSpec.getDataType();
    if (noDictionaryColumns.contains(column)) {
      // Earlier we didn't support noDict in consuming segments for STRING and BYTES columns.
      // So even if the user had the column in noDictionaryColumns set in table config, we still
      // created dictionary in consuming segments.
      // Later on we added this support. There is a particular impact of this change on the use cases
      // that have set noDict on their STRING dimension columns for other performance
      // reasons and also want metricsAggregation. These use cases don't get to
      // aggregateMetrics because the new implementation is able to honor their table config setting
      // of noDict on STRING/BYTES. Without metrics aggregation, memory pressure increases.
      // So to continue aggregating metrics for such cases, we will create dictionary even
      // if the column is part of noDictionary set from table config
      if (fieldSpec instanceof DimensionFieldSpec && isAggregateMetricsEnabled() && (dataType == STRING
          || dataType == BYTES)) {
        _logger.info(
            "Aggregate metrics is enabled. Will create dictionary in consuming segment for column {} of type {}",
            column, dataType);
        return false;
      }
      // So don't create dictionary if the column (1) is member of noDictionary, and (2) is single-value or multi-value
      // with a fixed-width field, and (3) doesn't have an inverted index
      return (fieldSpec.isSingleValueField() || fieldSpec.getDataType().isFixedWidth())
          && !invertedIndexColumns.contains(column);
    }
    // column is not a part of noDictionary set, so create dictionary
    return false;
  }

  public SegmentPartitionConfig getSegmentPartitionConfig() {
    if (_partitionColumn != null) {
      return new SegmentPartitionConfig(Collections.singletonMap(_partitionColumn,
          new ColumnPartitionConfig(_partitionFunction.getName(), _partitionFunction.getNumPartitions())));
    } else {
      return null;
    }
  }

  /**
   * Get min time from the segment, based on the time column, only used by Kafka HLC.
   */
  @Deprecated
  public long getMinTime() {
    Long minTime = IngestionUtils.extractTimeValue(_indexContainerMap.get(_timeColumnName)._minValue);
    if (minTime != null) {
      return minTime;
    }
    return Long.MAX_VALUE;
  }

  /**
   * Get max time from the segment, based on the time column, only used by Kafka HLC.
   */
  @Deprecated
  public long getMaxTime() {
    Long maxTime = IngestionUtils.extractTimeValue(_indexContainerMap.get(_timeColumnName)._maxValue);
    if (maxTime != null) {
      return maxTime;
    }
    return Long.MIN_VALUE;
  }

  @Override
  public boolean index(GenericRow row, @Nullable RowMetadata rowMetadata)
      throws IOException {
    boolean canTakeMore;
    int numDocsIndexed = _numDocsIndexed;

    RecordInfo recordInfo = null;

    if (isDedupEnabled() || isUpsertEnabled()) {
      recordInfo = getRecordInfo(row, numDocsIndexed);
    }

    if (isDedupEnabled() && _partitionDedupMetadataManager.checkRecordPresentOrUpdate(recordInfo.getPrimaryKey(),
        this)) {
      if (_serverMetrics != null) {
        _serverMetrics.addMeteredTableValue(_realtimeTableName, ServerMeter.REALTIME_DEDUP_DROPPED, 1);
      }
      return true;
    }

    if (isUpsertEnabled()) {
      GenericRow updatedRow = _partitionUpsertMetadataManager.updateRecord(row, recordInfo);
      updateDictionary(updatedRow);
      addNewRow(numDocsIndexed, updatedRow);
      // Update number of documents indexed before handling the upsert metadata so that the record becomes queryable
      // once validated
      canTakeMore = numDocsIndexed++ < _capacity;
      _partitionUpsertMetadataManager.addRecord(this, recordInfo);
    } else {
      // Update dictionary first
      updateDictionary(row);

      // If metrics aggregation is enabled and if the dimension values were already seen, this will return existing
      // docId, else this will return a new docId.
      int docId = getOrCreateDocId();

      if (docId == numDocsIndexed) {
        // New row
        addNewRow(numDocsIndexed, row);
        // Update number of documents indexed at last to make the latest row queryable
        canTakeMore = numDocsIndexed++ < _capacity;
      } else {
        assert isAggregateMetricsEnabled();
        aggregateMetrics(row, docId);
        canTakeMore = true;
      }
    }
    _numDocsIndexed = numDocsIndexed;

    // Update last indexed time and latest ingestion time
    _lastIndexedTimeMs = System.currentTimeMillis();
    if (rowMetadata != null) {
      _latestIngestionTimeMs = Math.max(_latestIngestionTimeMs, rowMetadata.getRecordIngestionTimeMs());
    }

    return canTakeMore;
  }

  private boolean isUpsertEnabled() {
    return _upsertMode != UpsertConfig.Mode.NONE;
  }

  private boolean isDedupEnabled() {
    return _partitionDedupMetadataManager != null;
  }

  private RecordInfo getRecordInfo(GenericRow row, int docId) {
    PrimaryKey primaryKey = row.getPrimaryKey(_schema.getPrimaryKeyColumns());

    if (isUpsertEnabled()) {
      Object upsertComparisonValue = row.getValue(_upsertComparisonColumn);
      Preconditions.checkState(upsertComparisonValue instanceof Comparable,
          "Upsert comparison column: %s must be comparable", _upsertComparisonColumn);
      return new RecordInfo(primaryKey, docId, (Comparable) upsertComparisonValue);
    }

    return new RecordInfo(primaryKey, docId, null);
  }

  private void updateDictionary(GenericRow row) {
    for (Map.Entry<String, IndexContainer> entry : _indexContainerMap.entrySet()) {
      IndexContainer indexContainer = entry.getValue();
      MutableDictionary dictionary = indexContainer._dictionary;
      if (dictionary == null) {
        continue;
      }

      Object value = row.getValue(entry.getKey());
      if (value == null) {
        recordIndexingError("DICTIONARY");
      } else {
        if (indexContainer._fieldSpec.isSingleValueField()) {
          indexContainer._dictId = dictionary.index(value);
        } else {
          indexContainer._dictIds = dictionary.index((Object[]) value);
        }

        // Update min/max value from dictionary
        indexContainer._minValue = dictionary.getMinVal();
        indexContainer._maxValue = dictionary.getMaxVal();
      }
    }
  }

  private void addNewRow(int docId, GenericRow row) {
    for (Map.Entry<String, IndexContainer> entry : _indexContainerMap.entrySet()) {
      String column = entry.getKey();
      IndexContainer indexContainer = entry.getValue();

      // aggregate metrics is enabled.
      if (indexContainer._valueAggregator != null) {
        Object value = row.getValue(indexContainer._sourceColumn);

        // Update numValues info
        indexContainer._numValuesInfo.updateSVEntry();

        MutableForwardIndex forwardIndex = indexContainer._forwardIndex;
        FieldSpec fieldSpec = indexContainer._fieldSpec;

        DataType dataType = fieldSpec.getDataType();
        value = indexContainer._valueAggregator.getInitialAggregatedValue(value);
        switch (dataType.getStoredType()) {
          case INT:
            forwardIndex.setInt(docId, ((Number) value).intValue());
            break;
          case LONG:
            forwardIndex.setLong(docId, ((Number) value).longValue());
            break;
          case FLOAT:
            forwardIndex.setFloat(docId, ((Number) value).floatValue());
            break;
          case DOUBLE:
            forwardIndex.setDouble(docId, ((Number) value).doubleValue());
            break;
          default:
            throw new UnsupportedOperationException(
                "Unsupported data type: " + dataType + " for aggregation: " + column);
        }
        continue;
      }

      // Update the null value vector even if a null value is somehow produced
      if (_nullHandlingEnabled && row.isNullValue(column)) {
        indexContainer._nullValueVector.setNull(docId);
      }

      Object value = row.getValue(column);
      if (value == null) {
        // the value should not be null unless something is broken upstream but this will lead to inappropriate reuse
        // of the dictionary id if this somehow happens. An NPE here can corrupt indexes leading to incorrect query
        // results, hence the extra care. A metric will already have been emitted when trying to update the dictionary.
        continue;
      }
      FieldSpec fieldSpec = indexContainer._fieldSpec;
      if (fieldSpec.isSingleValueField()) {
        // Single-value column

        // Check partitions
        if (column.equals(_partitionColumn)) {
          int partition = _partitionFunction.getPartition(value);
          if (indexContainer._partitions.add(partition)) {
            _logger.warn("Found new partition: {} from partition column: {}, value: {}", partition, column, value);
            if (_serverMetrics != null) {
              _serverMetrics.addMeteredTableValue(_realtimeTableName, ServerMeter.REALTIME_PARTITION_MISMATCH, 1);
            }
          }
        }

        // Update numValues info
        indexContainer._numValuesInfo.updateSVEntry();

        // Update indexes
        MutableForwardIndex forwardIndex = indexContainer._forwardIndex;
        int dictId = indexContainer._dictId;
        if (dictId >= 0) {
          // Dictionary-encoded single-value column

          // Update forward index
          forwardIndex.setDictId(docId, dictId);

          // Update inverted index
          MutableInvertedIndex invertedIndex = indexContainer._invertedIndex;
          if (invertedIndex != null) {
            try {
              invertedIndex.add(dictId, docId);
            } catch (Exception e) {
              recordIndexingError(FieldConfig.IndexType.INVERTED, e);
            }
          }
        } else {
          // Single-value column with raw index

          // Update forward index
          DataType dataType = fieldSpec.getDataType();
          switch (dataType.getStoredType()) {
            case INT:
              forwardIndex.setInt(docId, (Integer) value);
              break;
            case LONG:
              forwardIndex.setLong(docId, (Long) value);
              break;
            case FLOAT:
              forwardIndex.setFloat(docId, (Float) value);
              break;
            case DOUBLE:
              forwardIndex.setDouble(docId, (Double) value);
              break;
            case BIG_DECIMAL:
              forwardIndex.setBigDecimal(docId, (BigDecimal) value);
              break;
            case STRING:
              forwardIndex.setString(docId, (String) value);
              break;
            case BYTES:
              forwardIndex.setBytes(docId, (byte[]) value);
              break;
            default:
              throw new UnsupportedOperationException(
                  "Unsupported data type: " + dataType + " for no-dictionary column: " + column);
          }

          // Update min/max value from raw value
          // NOTE: Skip updating min/max value for aggregated metrics because the value will change over time.
          if (!isAggregateMetricsEnabled() || fieldSpec.getFieldType() != FieldSpec.FieldType.METRIC) {
            Comparable comparable;
            if (dataType == BYTES) {
              comparable = new ByteArray((byte[]) value);
            } else {
              comparable = (Comparable) value;
            }
            if (indexContainer._minValue == null) {
              indexContainer._minValue = comparable;
              indexContainer._maxValue = comparable;
            } else {
              if (comparable.compareTo(indexContainer._minValue) < 0) {
                indexContainer._minValue = comparable;
              }
              if (comparable.compareTo(indexContainer._maxValue) > 0) {
                indexContainer._maxValue = comparable;
              }
            }
          }
        }

        // Update text index
        MutableTextIndex textIndex = indexContainer._textIndex;
        if (textIndex != null) {
          try {
            textIndex.add((String) value);
          } catch (Exception e) {
            recordIndexingError(FieldConfig.IndexType.TEXT, e);
          }
        }

        // Update json index
        MutableJsonIndex jsonIndex = indexContainer._jsonIndex;
        if (jsonIndex != null) {
          try {
            jsonIndex.add((String) value);
          } catch (Exception e) {
            recordIndexingError(FieldConfig.IndexType.JSON, e);
          }
        }

        // Update H3 index
        MutableH3Index h3Index = indexContainer._h3Index;
        if (h3Index != null) {
          try {
            h3Index.add(GeometrySerializer.deserialize((byte[]) value));
          } catch (Exception e) {
            recordIndexingError(FieldConfig.IndexType.H3, e);
          }
        }
      } else {
        // Multi-value column

        int[] dictIds = indexContainer._dictIds;

        if (dictIds != null) {
          // Dictionary encoded
          // Update numValues info
          indexContainer._numValuesInfo.updateMVEntry(dictIds.length);

          // Update forward index
          indexContainer._forwardIndex.setDictIdMV(docId, dictIds);

          // Update inverted index
          MutableInvertedIndex invertedIndex = indexContainer._invertedIndex;
          if (invertedIndex != null) {
            for (int dictId : dictIds) {
              try {
                invertedIndex.add(dictId, docId);
              } catch (Exception e) {
                recordIndexingError(FieldConfig.IndexType.INVERTED, e);
              }
            }
          }
        } else {
          // Raw MV columns

          // Update forward index and numValues info
          DataType dataType = fieldSpec.getDataType();
          switch (dataType.getStoredType()) {
            case INT:
              Object[] values = (Object[]) value;
              int[] intValues = new int[values.length];
              for (int i = 0; i < values.length; i++) {
                intValues[i] = (Integer) values[i];
              }
              indexContainer._forwardIndex.setIntMV(docId, intValues);
              indexContainer._numValuesInfo.updateMVEntry(intValues.length);
              break;
            case LONG:
              values = (Object[]) value;
              long[] longValues = new long[values.length];
              for (int i = 0; i < values.length; i++) {
                longValues[i] = (Long) values[i];
              }
              indexContainer._forwardIndex.setLongMV(docId, longValues);
              indexContainer._numValuesInfo.updateMVEntry(longValues.length);
              break;
            case FLOAT:
              values = (Object[]) value;
              float[] floatValues = new float[values.length];
              for (int i = 0; i < values.length; i++) {
                floatValues[i] = (Float) values[i];
              }
              indexContainer._forwardIndex.setFloatMV(docId, floatValues);
              indexContainer._numValuesInfo.updateMVEntry(floatValues.length);
              break;
            case DOUBLE:
              values = (Object[]) value;
              double[] doubleValues = new double[values.length];
              for (int i = 0; i < values.length; i++) {
                doubleValues[i] = (Double) values[i];
              }
              indexContainer._forwardIndex.setDoubleMV(docId, doubleValues);
              indexContainer._numValuesInfo.updateMVEntry(doubleValues.length);
              break;
            default:
              throw new UnsupportedOperationException(
                  "Unsupported data type: " + dataType + " for MV no-dictionary column: " + column);
          }
        }
      }
    }
  }

  private void recordIndexingError(FieldConfig.IndexType indexType, Exception exception) {
    _logger.error("failed to index value with {}", indexType, exception);
    if (_serverMetrics != null) {
      String metricKeyName = _realtimeTableName + "-" + indexType + "-indexingError";
      _serverMetrics.addMeteredTableValue(metricKeyName, ServerMeter.INDEXING_FAILURES, 1);
    }
  }

  private void recordIndexingError(String indexType) {
    _logger.error("failed to index value with {}", indexType);
    if (_serverMetrics != null) {
      String metricKeyName = _realtimeTableName + "-" + indexType + "-indexingError";
      _serverMetrics.addMeteredTableValue(metricKeyName, ServerMeter.INDEXING_FAILURES, 1);
    }
  }

  private void aggregateMetrics(GenericRow row, int docId) {
    for (MetricFieldSpec metricFieldSpec : _physicalMetricFieldSpecs) {
      IndexContainer indexContainer = _indexContainerMap.get(metricFieldSpec.getName());
      Object value = row.getValue(indexContainer._sourceColumn);
      MutableForwardIndex forwardIndex = indexContainer._forwardIndex;
      DataType dataType = metricFieldSpec.getDataType();

      Double oldDoubleValue;
      Double newDoubleValue;
      Long oldLongValue;
      Long newLongValue;
      ValueAggregator valueAggregator = indexContainer._valueAggregator;
      switch (valueAggregator.getAggregatedValueType()) {
        case DOUBLE:
          switch (dataType) {
            case INT:
              oldDoubleValue = ((Integer) forwardIndex.getInt(docId)).doubleValue();
              newDoubleValue = (Double) valueAggregator.applyRawValue(oldDoubleValue, value);
              forwardIndex.setInt(docId, newDoubleValue.intValue());
              break;
            case LONG:
              oldDoubleValue = ((Long) forwardIndex.getLong(docId)).doubleValue();
              newDoubleValue = (Double) valueAggregator.applyRawValue(oldDoubleValue, value);
              forwardIndex.setLong(docId, newDoubleValue.longValue());
              break;
            case FLOAT:
              oldDoubleValue = ((Float) forwardIndex.getFloat(docId)).doubleValue();
              newDoubleValue = (Double) valueAggregator.applyRawValue(oldDoubleValue, value);
              forwardIndex.setFloat(docId, newDoubleValue.floatValue());
              break;
            case DOUBLE:
              oldDoubleValue = forwardIndex.getDouble(docId);
              newDoubleValue = (Double) valueAggregator.applyRawValue(oldDoubleValue, value);
              forwardIndex.setDouble(docId, newDoubleValue);
              break;
            default:
              throw new UnsupportedOperationException(String.format("Aggregation type %s of %s not supported for %s",
                  valueAggregator.getAggregatedValueType(), valueAggregator.getAggregationType(), dataType));
          }
          break;
        case LONG:
          switch (dataType) {
            case INT:
              oldLongValue = ((Integer) forwardIndex.getInt(docId)).longValue();
              newLongValue = (Long) valueAggregator.applyRawValue(oldLongValue, value);
              forwardIndex.setInt(docId, newLongValue.intValue());
              break;
            case LONG:
              oldLongValue = forwardIndex.getLong(docId);
              newLongValue = (Long) valueAggregator.applyRawValue(oldLongValue, value);
              forwardIndex.setLong(docId, newLongValue);
              break;
            case FLOAT:
              oldLongValue = ((Float) forwardIndex.getFloat(docId)).longValue();
              newLongValue = (Long) valueAggregator.applyRawValue(oldLongValue, value);
              forwardIndex.setFloat(docId, newLongValue.floatValue());
              break;
            case DOUBLE:
              oldLongValue = ((Double) forwardIndex.getDouble(docId)).longValue();
              newLongValue = (Long) valueAggregator.applyRawValue(oldLongValue, value);
              forwardIndex.setDouble(docId, newLongValue.doubleValue());
              break;
            default:
              throw new UnsupportedOperationException(String.format("Aggregation type %s of %s not supported for %s",
                  valueAggregator.getAggregatedValueType(), valueAggregator.getAggregationType(), dataType));
          }
          break;
        default:
          throw new UnsupportedOperationException(
              String.format("Aggregation type %s of %s not supported for %s", valueAggregator.getAggregatedValueType(),
                  valueAggregator.getAggregationType(), dataType));
      }
    }
  }

  @Override
  public int getNumDocsIndexed() {
    return _numDocsIndexed;
  }

  @Override
  public String getSegmentName() {
    return _segmentName;
  }

  @Override
  public SegmentMetadata getSegmentMetadata() {
    return _segmentMetadata;
  }

  @Override
  public Set<String> getColumnNames() {
    return _schema.getColumnNames();
  }

  @Override
  public Set<String> getPhysicalColumnNames() {
    HashSet<String> physicalColumnNames = new HashSet<>();
    for (FieldSpec fieldSpec : _physicalFieldSpecs) {
      physicalColumnNames.add(fieldSpec.getName());
    }
    return physicalColumnNames;
  }

  @Override
  public DataSource getDataSource(String column) {
    IndexContainer indexContainer = _indexContainerMap.get(column);
    if (indexContainer != null) {
      // Physical column
      return indexContainer.toDataSource();
    } else {
      // Virtual column
      FieldSpec fieldSpec = _schema.getFieldSpecFor(column);
      Preconditions.checkState(fieldSpec != null && fieldSpec.isVirtualColumn(), "Failed to find column: %s", column);
      // TODO: Refactor virtual column provider to directly generate data source
      VirtualColumnContext virtualColumnContext = new VirtualColumnContext(fieldSpec, _numDocsIndexed);
      VirtualColumnProvider virtualColumnProvider = VirtualColumnProviderFactory.buildProvider(virtualColumnContext);
      return new ImmutableDataSource(virtualColumnProvider.buildMetadata(virtualColumnContext),
          virtualColumnProvider.buildColumnIndexContainer(virtualColumnContext));
    }
  }

  @Override
  public List<StarTreeV2> getStarTrees() {
    return null;
  }

  @Nullable
  @Override
  public ThreadSafeMutableRoaringBitmap getValidDocIds() {
    return _validDocIds;
  }

  @Override
  public GenericRow getRecord(int docId, GenericRow reuse) {
    try (PinotSegmentRecordReader recordReader = new PinotSegmentRecordReader()) {
      recordReader.init(this);
      recordReader.getRecord(docId, reuse);
      return reuse;
    } catch (Exception e) {
      throw new RuntimeException("Caught exception while reading record for docId: " + docId, e);
    }
  }

  @Override
  public Object getValue(int docId, String column) {
    try (PinotSegmentColumnReader columnReader = new PinotSegmentColumnReader(this, column)) {
      return columnReader.getValue(docId);
    } catch (Exception e) {
      throw new RuntimeException(
          String.format("Caught exception while reading value for docId: %d, column: %s", docId, column), e);
    }
  }

  @Override
  public void destroy() {
    _logger.info("Trying to close RealtimeSegmentImpl : {}", _segmentName);

    // Remove the upsert and dedup metadata before closing the readers
    if (_partitionUpsertMetadataManager != null) {
      _partitionUpsertMetadataManager.removeSegment(this);
    }

    if (_partitionDedupMetadataManager != null) {
      _partitionDedupMetadataManager.removeSegment(this);
    }

    // Gather statistics for off-heap mode
    if (_offHeap) {
      if (_numDocsIndexed > 0) {
        int numSeconds = (int) ((System.currentTimeMillis() - _startTimeMillis) / 1000);
        long totalMemBytes = _memoryManager.getTotalAllocatedBytes();
        _logger.info("Segment used {} bytes of memory for {} rows consumed in {} seconds", totalMemBytes,
            _numDocsIndexed, numSeconds);

        RealtimeSegmentStatsHistory.SegmentStats segmentStats = new RealtimeSegmentStatsHistory.SegmentStats();
        for (Map.Entry<String, IndexContainer> entry : _indexContainerMap.entrySet()) {
          String column = entry.getKey();
          BaseOffHeapMutableDictionary dictionary = (BaseOffHeapMutableDictionary) entry.getValue()._dictionary;
          if (dictionary != null) {
            RealtimeSegmentStatsHistory.ColumnStats columnStats = new RealtimeSegmentStatsHistory.ColumnStats();
            columnStats.setCardinality(dictionary.length());
            columnStats.setAvgColumnSize(dictionary.getAvgValueSize());
            segmentStats.setColumnStats(column, columnStats);
          }
        }
        segmentStats.setNumRowsConsumed(_numDocsIndexed);
        segmentStats.setNumRowsIndexed(_numDocsIndexed);
        segmentStats.setMemUsedBytes(totalMemBytes);
        segmentStats.setNumSeconds(numSeconds);
        _statsHistory.addSegmentStats(segmentStats);
      }
    }

    // Stop the text index refresh before closing the indexes
    if (_realtimeLuceneReaders != null) {
      // set this to true as a way of signalling the refresh task thread to
      // not attempt refresh on this segment here onwards
      _realtimeLuceneReaders.getLock().lock();
      try {
        _realtimeLuceneReaders.setSegmentDestroyed();
        _realtimeLuceneReaders.clearRealtimeReaderList();
      } finally {
        _realtimeLuceneReaders.getLock().unlock();
      }
    }

    // Close the indexes
    for (IndexContainer indexContainer : _indexContainerMap.values()) {
      indexContainer.close();
    }

    if (_recordIdMap != null) {
      try {
        _recordIdMap.close();
      } catch (IOException e) {
        _logger.error("Failed to close the record id map. Continuing with error.", e);
      }
    }

    // NOTE: Close the memory manager as the last step. It will release all the PinotDataBuffers allocated.
    try {
      _memoryManager.close();
    } catch (IOException e) {
      _logger.error("Failed to close the memory manager", e);
    }
  }

  /**
   * Returns the docIds to use for iteration when the data is sorted by the given column.
   * <p>Called only by realtime record reader.
   *
   * @param column The column to use for sorting
   * @return The docIds to use for iteration
   */
  public int[] getSortedDocIdIterationOrderWithSortedColumn(String column) {
    IndexContainer indexContainer = _indexContainerMap.get(column);
    MutableDictionary dictionary = indexContainer._dictionary;
    int numDocsIndexed = _numDocsIndexed;
    // Sort all values in the dictionary
    int numValues = dictionary.length();
    int[] dictIds = new int[numValues];
    for (int i = 0; i < numValues; i++) {
      dictIds[i] = i;
    }
    IntArrays.quickSort(dictIds, dictionary::compare);

    // Re-order documents using the inverted index
    MutableInvertedIndex invertedIndex = indexContainer._invertedIndex;
    int[] docIds = new int[numDocsIndexed];
    int[] batch = new int[256];
    int docIdIndex = 0;
    for (int dictId : dictIds) {
      MutableRoaringBitmap bitmap = invertedIndex.getDocIds(dictId);
      BatchIterator iterator = bitmap.getBatchIterator();
      while (iterator.hasNext()) {
        int limit = iterator.nextBatch(batch);
        System.arraycopy(batch, 0, docIds, docIdIndex, limit);
        docIdIndex += limit;
      }
    }

    // Sanity check
    Preconditions.checkState(numDocsIndexed == docIdIndex,
        "The number of documents indexed: %s is not equal to the number of sorted documents: %s", numDocsIndexed,
        docIdIndex);

    return docIds;
  }

  /**
   * Helper function that returns docId, depends on the following scenarios.
   * <ul>
   *   <li> If metrics aggregation is enabled and if the dimension values were already seen, return existing docIds
   *   </li>
   *   <li> Else, this function will create and return a new docId. </li>
   * </ul>
   *
   * */
  private int getOrCreateDocId() {
    if (!isAggregateMetricsEnabled()) {
      return _numDocsIndexed;
    }

    int i = 0;
    int[] dictIds = new int[_numKeyColumns]; // dimensions + date time columns + time column.

    // FIXME: this for loop breaks for multi value dimensions. https://github.com/apache/pinot/issues/3867
    for (FieldSpec fieldSpec : _physicalDimensionFieldSpecs) {
      dictIds[i++] = _indexContainerMap.get(fieldSpec.getName())._dictId;
    }
    for (String timeColumnName : _physicalTimeColumnNames) {
      dictIds[i++] = _indexContainerMap.get(timeColumnName)._dictId;
    }
    return _recordIdMap.put(new FixedIntArray(dictIds));
  }

  /**
   * Helper method to enable/initialize aggregation of metrics, based on following conditions:
   * <ul>
   *   <li> Config to enable aggregation of metrics is specified. </li>
   *   <li> All dimensions and time are dictionary encoded. This is because an integer array containing dictionary id's
   *        is used as key for dimensions to record Id map. </li>
   *   <li> None of the metrics are dictionary encoded. </li>
   *   <li> All columns should be single-valued (see https://github.com/apache/pinot/issues/3867)</li>
   * </ul>
   *
   * TODO: Eliminate the requirement on dictionary encoding for dimension and metric columns.
   *
   * @param config Segment config.
   * @param noDictionaryColumns Set of no dictionary columns.
   *
   * @return Map from dictionary id array to doc id, null if metrics aggregation cannot be enabled.
   */
  private IdMap<FixedIntArray> enableMetricsAggregationIfPossible(RealtimeSegmentConfig config,
      Set<String> noDictionaryColumns) {
    if (!config.aggregateMetrics() && CollectionUtils.isEmpty(config.getIngestionAggregationConfigs())) {
      _logger.info("Metrics aggregation is disabled.");
      return null;
    }

    // All metric columns should have no-dictionary index.
    // All metric columns must be single value
    for (FieldSpec fieldSpec : _physicalMetricFieldSpecs) {
      String metric = fieldSpec.getName();
      if (!noDictionaryColumns.contains(metric)) {
        _logger.warn("Metrics aggregation cannot be turned ON in presence of dictionary encoded metrics, eg: {}",
            metric);
        return null;
      }

      if (!fieldSpec.isSingleValueField()) {
        _logger.warn("Metrics aggregation cannot be turned ON in presence of multi-value metric columns, eg: {}",
            metric);
        return null;
      }
    }

    // All dimension columns should be dictionary encoded.
    // All dimension columns must be single value
    for (FieldSpec fieldSpec : _physicalDimensionFieldSpecs) {
      String dimension = fieldSpec.getName();
      if (noDictionaryColumns.contains(dimension)) {
        _logger.warn("Metrics aggregation cannot be turned ON in presence of no-dictionary dimensions, eg: {}",
            dimension);
        return null;
      }

      if (!fieldSpec.isSingleValueField()) {
        _logger.warn("Metrics aggregation cannot be turned ON in presence of multi-value dimension columns, eg: {}",
            dimension);
        return null;
      }
    }

    // Time columns should be dictionary encoded.
    for (String timeColumnName : _physicalTimeColumnNames) {
      if (noDictionaryColumns.contains(timeColumnName)) {
        _logger.warn(
            "Metrics aggregation cannot be turned ON in presence of no-dictionary datetime/time columns, eg: {}",
            timeColumnName);
        return null;
      }
    }

    int estimatedRowsToIndex;
    if (_statsHistory.isEmpty()) {
      // Choose estimated rows to index as maxNumRowsPerSegment / EXPECTED_COMPRESSION (1000, to be conservative in
      // size).
      // These are just heuristics at the moment, and can be refined based on experimental results.
      estimatedRowsToIndex = Math.max(config.getCapacity() / EXPECTED_COMPRESSION, MIN_ROWS_TO_INDEX);
    } else {
      estimatedRowsToIndex = Math.max(_statsHistory.getEstimatedRowsToIndex(), MIN_ROWS_TO_INDEX);
    }

    // Compute size of overflow map.
    int maxOverFlowHashSize = Math.max(estimatedRowsToIndex / 1000, MIN_RECORD_ID_MAP_CACHE_SIZE);

    _logger.info("Initializing metrics update: estimatedRowsToIndex:{}, cacheSize:{}", estimatedRowsToIndex,
        maxOverFlowHashSize);
    return new FixedIntArrayOffHeapIdMap(estimatedRowsToIndex, maxOverFlowHashSize, _numKeyColumns, _memoryManager,
        RECORD_ID_MAP);
  }

  private boolean isAggregateMetricsEnabled() {
    return _recordIdMap != null;
  }

  // NOTE: Okay for single-writer
  @SuppressWarnings("NonAtomicOperationOnVolatileField")
  private static class NumValuesInfo {
    volatile int _numValues = 0;
    volatile int _maxNumValuesPerMVEntry = -1;

    void updateSVEntry() {
      _numValues++;
    }

    void updateMVEntry(int numValuesInMVEntry) {
      _numValues += numValuesInMVEntry;
      _maxNumValuesPerMVEntry = Math.max(_maxNumValuesPerMVEntry, numValuesInMVEntry);
    }
  }

  private static Map<String, Pair<String, ValueAggregator>> getMetricsAggregators(RealtimeSegmentConfig segmentConfig) {
    if (segmentConfig.aggregateMetrics()) {
      return fromAggregateMetrics(segmentConfig);
    } else if (!CollectionUtils.isEmpty(segmentConfig.getIngestionAggregationConfigs())) {
      return fromAggregationConfig(segmentConfig);
    } else {
      return Collections.emptyMap();
    }
  }

  private static Map<String, Pair<String, ValueAggregator>> fromAggregateMetrics(RealtimeSegmentConfig segmentConfig) {
    Preconditions.checkState(CollectionUtils.isEmpty(segmentConfig.getIngestionAggregationConfigs()),
        "aggregateMetrics cannot be enabled if AggregationConfig is set");

    Map<String, Pair<String, ValueAggregator>> columnNameToAggregator = new HashMap<>();
    for (String metricName : segmentConfig.getSchema().getMetricNames()) {
      columnNameToAggregator.put(metricName,
          Pair.of(metricName, ValueAggregatorFactory.getValueAggregator(AggregationFunctionType.SUM)));
    }
    return columnNameToAggregator;
  }

  private static Map<String, Pair<String, ValueAggregator>> fromAggregationConfig(RealtimeSegmentConfig segmentConfig) {
    Map<String, Pair<String, ValueAggregator>> columnNameToAggregator = new HashMap<>();

    Preconditions.checkState(!segmentConfig.aggregateMetrics(),
        "aggregateMetrics cannot be enabled if AggregationConfig is set");
    for (AggregationConfig config : segmentConfig.getIngestionAggregationConfigs()) {
      ExpressionContext expressionContext = RequestContextUtils.getExpression(config.getAggregationFunction());
      // validation is also done when the table is created, this is just a sanity check.
      Preconditions.checkState(expressionContext.getType() == ExpressionContext.Type.FUNCTION,
          "aggregation function must be a function: %s", config);
      FunctionContext functionContext = expressionContext.getFunction();
      TableConfigUtils.validateIngestionAggregation(functionContext.getFunctionName());
      Preconditions.checkState(functionContext.getArguments().size() == 1,
          "aggregation function can only have one argument: %s", config);
      ExpressionContext argument = functionContext.getArguments().get(0);
      Preconditions.checkState(argument.getType() == ExpressionContext.Type.IDENTIFIER,
          "aggregator function argument must be a identifier: %s", config);

      AggregationFunctionType functionType =
          AggregationFunctionType.getAggregationFunctionType(functionContext.getFunctionName());

      columnNameToAggregator.put(config.getColumnName(),
          Pair.of(argument.getIdentifier(), ValueAggregatorFactory.getValueAggregator(functionType)));
    }

    return columnNameToAggregator;
  }

  private class IndexContainer implements Closeable {
    final FieldSpec _fieldSpec;
    final PartitionFunction _partitionFunction;
    final Set<Integer> _partitions;
    final NumValuesInfo _numValuesInfo;
    final MutableForwardIndex _forwardIndex;
    final MutableDictionary _dictionary;
    final MutableInvertedIndex _invertedIndex;
    final RangeIndexReader _rangeIndex;
    final MutableH3Index _h3Index;
    final MutableTextIndex _textIndex;
    final MutableTextIndex _fstIndex;
    final MutableJsonIndex _jsonIndex;
    final BloomFilterReader _bloomFilter;
    final MutableNullValueVector _nullValueVector;
    final String _sourceColumn;
    final ValueAggregator _valueAggregator;

    volatile Comparable _minValue;
    volatile Comparable _maxValue;

    // Hold the dictionary id for the latest record
    int _dictId = Integer.MIN_VALUE;
    int[] _dictIds;

    IndexContainer(FieldSpec fieldSpec, @Nullable PartitionFunction partitionFunction,
        @Nullable Set<Integer> partitions, NumValuesInfo numValuesInfo, MutableForwardIndex forwardIndex,
        @Nullable MutableDictionary dictionary, @Nullable MutableInvertedIndex invertedIndex,
        @Nullable RangeIndexReader rangeIndex, @Nullable MutableTextIndex textIndex,
        @Nullable MutableTextIndex fstIndex, @Nullable MutableJsonIndex jsonIndex, @Nullable MutableH3Index h3Index,
        @Nullable BloomFilterReader bloomFilter, @Nullable MutableNullValueVector nullValueVector,
        @Nullable String sourceColumn, @Nullable ValueAggregator valueAggregator) {
      _fieldSpec = fieldSpec;
      _partitionFunction = partitionFunction;
      _partitions = partitions;
      _numValuesInfo = numValuesInfo;
      _forwardIndex = forwardIndex;
      _dictionary = dictionary;
      _invertedIndex = invertedIndex;
      _rangeIndex = rangeIndex;
      _h3Index = h3Index;

      _textIndex = textIndex;
      _fstIndex = fstIndex;
      _jsonIndex = jsonIndex;
      _bloomFilter = bloomFilter;
      _nullValueVector = nullValueVector;
      _sourceColumn = sourceColumn;
      _valueAggregator = valueAggregator;
    }

    DataSource toDataSource() {
      return new MutableDataSource(_fieldSpec, _numDocsIndexed, _numValuesInfo._numValues,
          _numValuesInfo._maxNumValuesPerMVEntry, _dictionary == null ? -1 : _dictionary.length(), _partitionFunction,
          _partitions, _minValue, _maxValue, _forwardIndex, _dictionary, _invertedIndex, _rangeIndex, _textIndex,
          _fstIndex, _jsonIndex, _h3Index, _bloomFilter, _nullValueVector);
    }

    @Override
    public void close() {
      String column = _fieldSpec.getName();
      try {
        _forwardIndex.close();
      } catch (Exception e) {
        _logger.error("Caught exception while closing forward index for column: {}, continuing with error", column, e);
      }
      if (_dictionary != null) {
        try {
          _dictionary.close();
        } catch (Exception e) {
          _logger.error("Caught exception while closing dictionary for column: {}, continuing with error", column, e);
        }
      }
      if (_invertedIndex != null) {
        try {
          _invertedIndex.close();
        } catch (Exception e) {
          _logger.error("Caught exception while closing inverted index for column: {}, continuing with error", column,
              e);
        }
      }
      if (_rangeIndex != null) {
        try {
          _rangeIndex.close();
        } catch (Exception e) {
          _logger.error("Caught exception while closing range index for column: {}, continuing with error", column, e);
        }
      }
      if (_textIndex != null) {
        try {
          _textIndex.close();
        } catch (Exception e) {
          _logger.error("Caught exception while closing text index for column: {}, continuing with error", column, e);
        }
      }
      if (_fstIndex != null) {
        try {
          _fstIndex.close();
        } catch (Exception e) {
          _logger.error("Caught exception while closing fst index for column: {}, continuing with error", column, e);
        }
      }
      if (_jsonIndex != null) {
        try {
          _jsonIndex.close();
        } catch (Exception e) {
          _logger.error("Caught exception while closing json index for column: {}, continuing with error", column, e);
        }
      }
      if (_h3Index != null) {
        try {
          _h3Index.close();
        } catch (Exception e) {
          _logger.error("Caught exception while closing H3 index for column: {}, continuing with error", column, e);
        }
      }
      if (_bloomFilter != null) {
        try {
          _bloomFilter.close();
        } catch (Exception e) {
          _logger.error("Caught exception while closing bloom filter for column: {}, continuing with error", column, e);
        }
      }
    }
  }
}
