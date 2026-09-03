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
import com.google.common.base.Utf8;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import it.unimi.dsi.fastutil.booleans.BooleanArrayList;
import it.unimi.dsi.fastutil.booleans.BooleanList;
import it.unimi.dsi.fastutil.ints.IntArrays;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiConsumer;
import javax.annotation.Nullable;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.common.metrics.ServerMeter;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.request.context.FunctionContext;
import org.apache.pinot.common.request.context.RequestContextUtils;
import org.apache.pinot.segment.local.aggregator.ValueAggregator;
import org.apache.pinot.segment.local.aggregator.ValueAggregatorFactory;
import org.apache.pinot.segment.local.dedup.DedupRecordInfo;
import org.apache.pinot.segment.local.dedup.PartitionDedupMetadataManager;
import org.apache.pinot.segment.local.indexsegment.IndexSegmentUtils;
import org.apache.pinot.segment.local.realtime.impl.RealtimeSegmentConfig;
import org.apache.pinot.segment.local.realtime.impl.RealtimeSegmentStatsHistory;
import org.apache.pinot.segment.local.realtime.impl.dictionary.BaseOffHeapMutableDictionary;
import org.apache.pinot.segment.local.realtime.impl.dictionary.SameValueMutableDictionary;
import org.apache.pinot.segment.local.realtime.impl.forward.FixedByteMVMutableForwardIndex;
import org.apache.pinot.segment.local.realtime.impl.forward.SameValueMutableForwardIndex;
import org.apache.pinot.segment.local.realtime.impl.invertedindex.MultiColumnRealtimeLuceneTextIndex;
import org.apache.pinot.segment.local.realtime.impl.nullvalue.MutableNullValueVector;
import org.apache.pinot.segment.local.segment.index.datasource.MutableDataSource;
import org.apache.pinot.segment.local.segment.index.dictionary.DictionaryIndexType;
import org.apache.pinot.segment.local.segment.index.map.MutableMapDataSource;
import org.apache.pinot.segment.local.segment.index.openstruct.MutableOpenStructDataSource;
import org.apache.pinot.segment.local.segment.index.openstruct.MutableOpenStructIndex;
import org.apache.pinot.segment.local.segment.readers.PinotSegmentColumnReader;
import org.apache.pinot.segment.local.segment.readers.PinotSegmentRecordReader;
import org.apache.pinot.segment.local.segment.virtualcolumn.VirtualColumnContext;
import org.apache.pinot.segment.local.segment.virtualcolumn.VirtualColumnProviderFactory;
import org.apache.pinot.segment.local.upsert.ComparisonColumns;
import org.apache.pinot.segment.local.upsert.PartitionUpsertMetadataManager;
import org.apache.pinot.segment.local.upsert.RecordInfo;
import org.apache.pinot.segment.local.upsert.UpsertContext;
import org.apache.pinot.segment.local.upsert.UpsertUtils;
import org.apache.pinot.segment.local.upsert.UpsertViewManager;
import org.apache.pinot.segment.local.utils.FixedIntArrayOffHeapIdMap;
import org.apache.pinot.segment.local.utils.IdMap;
import org.apache.pinot.segment.spi.AggregationFunctionType;
import org.apache.pinot.segment.spi.MutableSegment;
import org.apache.pinot.segment.spi.SegmentMetadata;
import org.apache.pinot.segment.spi.datasource.DataSource;
import org.apache.pinot.segment.spi.index.DictionaryIndexConfig;
import org.apache.pinot.segment.spi.index.FieldIndexConfigs;
import org.apache.pinot.segment.spi.index.FieldIndexConfigsUtil;
import org.apache.pinot.segment.spi.index.IndexService;
import org.apache.pinot.segment.spi.index.IndexType;
import org.apache.pinot.segment.spi.index.StandardIndexes;
import org.apache.pinot.segment.spi.index.VectorIndexConfigProvider;
import org.apache.pinot.segment.spi.index.creator.VectorIndexConfig;
import org.apache.pinot.segment.spi.index.metadata.SegmentMetadataImpl;
import org.apache.pinot.segment.spi.index.multicolumntext.MultiColumnTextMetadata;
import org.apache.pinot.segment.spi.index.mutable.MutableDictionary;
import org.apache.pinot.segment.spi.index.mutable.MutableForwardIndex;
import org.apache.pinot.segment.spi.index.mutable.MutableIndex;
import org.apache.pinot.segment.spi.index.mutable.MutableInvertedIndex;
import org.apache.pinot.segment.spi.index.mutable.ThreadSafeMutableRoaringBitmap;
import org.apache.pinot.segment.spi.index.mutable.provider.MutableIndexContext;
import org.apache.pinot.segment.spi.index.reader.MultiColumnTextIndexReader;
import org.apache.pinot.segment.spi.index.reader.TextIndexReader;
import org.apache.pinot.segment.spi.index.startree.AggregationFunctionColumnPair;
import org.apache.pinot.segment.spi.index.startree.StarTreeV2;
import org.apache.pinot.segment.spi.memory.PinotDataBufferMemoryManager;
import org.apache.pinot.segment.spi.partition.PartitionFunction;
import org.apache.pinot.spi.config.table.ColumnPartitionConfig;
import org.apache.pinot.spi.config.table.IndexConfig;
import org.apache.pinot.spi.config.table.MultiColumnTextIndexConfig;
import org.apache.pinot.spi.config.table.OpenStructIndexConfig;
import org.apache.pinot.spi.config.table.SegmentPartitionConfig;
import org.apache.pinot.spi.config.table.UpsertConfig;
import org.apache.pinot.spi.config.table.ingestion.AggregationConfig;
import org.apache.pinot.spi.data.ComplexFieldSpec;
import org.apache.pinot.spi.data.DimensionFieldSpec;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.MetricFieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.data.readers.PrimaryKey;
import org.apache.pinot.spi.stream.StreamMessageMetadata;
import org.apache.pinot.spi.utils.BooleanUtils;
import org.apache.pinot.spi.utils.ByteArray;
import org.apache.pinot.spi.utils.FixedIntArray;
import org.apache.pinot.spi.utils.MapUtils;
import org.apache.pinot.spi.utils.UuidUtils;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.roaringbitmap.BatchIterator;
import org.roaringbitmap.buffer.MutableRoaringBitmap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.pinot.spi.data.FieldSpec.DataType.BYTES;
import static org.apache.pinot.spi.data.FieldSpec.DataType.MAP;
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
  private final int _capacity;
  private final SegmentMetadata _segmentMetadata;
  private final boolean _offHeap;
  private final PinotDataBufferMemoryManager _memoryManager;
  private final RealtimeSegmentStatsHistory _statsHistory;
  private final String _partitionColumn;
  private final PartitionFunction _partitionFunction;
  private final int _mainPartitionId; // partition id designated for this consuming segment
  private final boolean _dropRecordOnPartitionMismatch;
  private final boolean _defaultNullHandlingEnabled;
  /// Honors `IngestionConfig.continueOnError`. When false, exceptions from dictionary / row / index writes
  /// propagate instead of being fail-soft substituted.
  private final boolean _continueOnError;
  /// First addNewRow indexing exception stashed so the row can be published before a strict rethrow.
  /// Only accessed on the consuming thread that calls [#index].
  @Nullable
  private Exception _pendingRowIndexingException;
  private final File _consumerDir;

  private final Map<String, IndexContainer> _indexContainerMap = new HashMap<>();
  private final IdMap<FixedIntArray> _recordIdMap;
  private final int _numKeyColumns;
  // Cache the physical (non-virtual) field specs
  private final Collection<FieldSpec> _physicalFieldSpecs;
  private final Collection<DimensionFieldSpec> _physicalDimensionFieldSpecs;
  private final Collection<MetricFieldSpec> _physicalMetricFieldSpecs;
  private final Collection<String> _physicalTimeColumnNames;
  private final Collection<ComplexFieldSpec> _physicalComplexFieldSpecs;
  private final PartitionDedupMetadataManager _partitionDedupMetadataManager;
  private final String _dedupTimeColumn;
  private final PartitionUpsertMetadataManager _partitionUpsertMetadataManager;
  private final List<String> _upsertComparisonColumns;
  private final String _deleteRecordColumn;
  private final boolean _upsertDropOutOfOrderRecord;
  private final String _upsertOutOfOrderRecordColumn;
  private final UpsertConfig.ConsistencyMode _upsertConsistencyMode;

  // The valid doc ids are maintained locally on the segment (not in a global store) because:
  // 1. Upsert metadata (PK → segment/docId location) is global; bitmaps stay segment-local for query filters.
  // 2. On seal/reload, some replace paths pass a snapshot copy of the old bitmap so live query bitmaps are not mutated
  //    mid-seal; the location map is always updated under ConcurrentHashMap.compute. When a newer late event lands on
  //    the consuming segment, doAddRecord/replaceDocId keep location and both bitmaps aligned (see
  //    ConcurrentMapPartitionUpsertMetadataManager). Committed segments can clear consuming bits via replaceDocId when
  //    they win a PK.
  // Regression coverage for late-event interleaved with replaceSegment lives in
  // ConcurrentMapPartitionUpsertMetadataManagerTest (issue #18217).
  private final ThreadSafeMutableRoaringBitmap _validDocIds;
  private final ThreadSafeMutableRoaringBitmap _queryableDocIds;
  private boolean _indexCapacityThresholdBreached;
  private volatile int _numDocsIndexed = 0;
  // default message metadata
  private volatile long _lastIndexedTimeMs = Long.MIN_VALUE;
  private volatile long _latestIngestionTimeMs = Long.MIN_VALUE;
  private volatile long _minimumIngestionLagMs = Long.MAX_VALUE;

  private final boolean _hasColumnWithReuseMutableTextIndex;

  // multi-column text index fields
  private final MultiColumnRealtimeLuceneTextIndex _multiColumnTextIndex;
  private final Object2IntOpenHashMap _multiColumnPos;
  private final List<Object> _multiColumnValues;
  private final MultiColumnTextMetadata _multiColumnTextMetadata;

  public MutableSegmentImpl(RealtimeSegmentConfig config, @Nullable ServerMetrics serverMetrics) {
    _serverMetrics = serverMetrics;
    _realtimeTableName = config.getTableNameWithType();
    _segmentName = config.getSegmentName();
    _schema = config.getSchema();
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

      @Override
      public long getMinimumIngestionLagMs() {
        return _minimumIngestionLagMs;
      }

      @Override
      public boolean isMutableSegment() {
        return true;
      }

      @Nullable
      @Override
      public MultiColumnTextMetadata getMultiColumnTextMetadata() {
        return _multiColumnTextMetadata;
      }
    };

    _offHeap = config.isOffHeap();
    _memoryManager = config.getMemoryManager();
    _statsHistory = config.getStatsHistory();
    _partitionColumn = config.getPartitionColumn();
    _partitionFunction = config.getPartitionFunction();
    _mainPartitionId = config.getPartitionId();
    _dropRecordOnPartitionMismatch = config.isDropRecordOnPartitionMismatch();
    _defaultNullHandlingEnabled = config.isNullHandlingEnabled();
    _continueOnError = config.isContinueOnError();
    _consumerDir = new File(config.getConsumerDir());

    Collection<FieldSpec> allFieldSpecs = _schema.getAllFieldSpecs();
    List<FieldSpec> physicalFieldSpecs = new ArrayList<>(allFieldSpecs.size());
    List<DimensionFieldSpec> physicalDimensionFieldSpecs = new ArrayList<>(_schema.getDimensionNames().size());
    List<MetricFieldSpec> physicalMetricFieldSpecs = new ArrayList<>(_schema.getMetricNames().size());
    List<String> physicalTimeColumnNames = new ArrayList<>();
    List<ComplexFieldSpec> physicalComplexFieldSpecs = new ArrayList<>();

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
        } else if (fieldType == FieldSpec.FieldType.COMPLEX) {
          physicalComplexFieldSpecs.add((ComplexFieldSpec) fieldSpec);
        }
      }
    }
    _physicalFieldSpecs = Collections.unmodifiableCollection(physicalFieldSpecs);
    _physicalDimensionFieldSpecs = Collections.unmodifiableCollection(physicalDimensionFieldSpecs);
    _physicalMetricFieldSpecs = Collections.unmodifiableCollection(physicalMetricFieldSpecs);
    _physicalTimeColumnNames = Collections.unmodifiableCollection(physicalTimeColumnNames);
    _physicalComplexFieldSpecs = Collections.unmodifiableCollection(physicalComplexFieldSpecs);

    _numKeyColumns = _physicalDimensionFieldSpecs.size() + _physicalTimeColumnNames.size();

    _logger =
        LoggerFactory.getLogger(MutableSegmentImpl.class.getName() + "_" + _segmentName + "_" + config.getStreamName());

    // Metric aggregation can be enabled only if config is specified, and all dimensions have dictionary,
    // and no metrics have dictionary. If not enabled, the map returned is null.
    _recordIdMap = enableMetricsAggregationIfPossible(config);

    Map<String, Pair<String, ValueAggregator>> metricsAggregators = Map.of();
    if (_recordIdMap != null) {
      metricsAggregators = getMetricsAggregators(config);
    }

    Set<IndexType> specialIndexes =
        Sets.newHashSet(StandardIndexes.dictionary(), // dictionary implements other contract
            StandardIndexes.nullValueVector(), // null value vector implements other contract
            StandardIndexes.openStruct()); // open-struct is constructed out-of-band below

    // Initialize for each column
    boolean hasColumnWithReuseMutableTextIndex = false;
    for (FieldSpec fieldSpec : _physicalFieldSpecs) {
      String column = fieldSpec.getName();

      int fixedByteSize = -1;
      DataType dataType = fieldSpec.getDataType();
      DataType storedType = dataType.getStoredType();
      if (!storedType.isFixedWidth()) {
        // For aggregated metrics, we need to store values with fixed byte size so that in-place replacement is possible
        Pair<String, ValueAggregator> aggregatorPair = metricsAggregators.get(column);
        if (aggregatorPair != null) {
          fixedByteSize = aggregatorPair.getRight().getMaxAggregatedValueByteSize();
        }
      }

      FieldIndexConfigs indexConfigs =
          Optional.ofNullable(config.getIndexConfigByCol().get(column)).orElse(FieldIndexConfigs.EMPTY);
      boolean isDictionary = !isNoDictionaryColumn(indexConfigs, fieldSpec, column);
      MutableIndexContext context =
          MutableIndexContext.builder()
              .withFieldSpec(fieldSpec)
              .withMemoryManager(_memoryManager)
              .withDictionary(isDictionary)
              .withCapacity(_capacity)
              .offHeap(_offHeap)
              .withSegmentName(_segmentName)
              .withEstimatedCardinality(_statsHistory.getEstimatedCardinality(column))
              .withEstimatedColSize(_statsHistory.getEstimatedAvgColSize(column))
              .withAvgNumMultiValues(_statsHistory.getEstimatedAvgColSize(column))
              .withConsumerDir(_consumerDir)
              .withFixedLengthBytes(fixedByteSize).build();

      // Partition info
      PartitionFunction partitionFunction = null;
      Set<Integer> partitions = null;
      if (column.equals(_partitionColumn)) {
        partitionFunction = _partitionFunction;

        // NOTE: Use a concurrent set because the partitions can be updated when the partition of the ingested record
        //       does not match the stream partition. This could happen when stream partition changes, or the records
        //       are not properly partitioned from the stream. Log a warning and emit a metric if it happens, then add
        //       the new partition into this set.
        partitions = ConcurrentHashMap.newKeySet();
        partitions.add(_mainPartitionId);
      }

      // TODO (mutable-index-spi): The comment above was here, but no check was done.
      //  It seems the code that apply that check was removed around 2020. Should we remove the comment?
      // Check whether to generate raw index for the column while consuming
      // Only support generating raw index on single-value columns that do not have inverted index while
      // consuming. After consumption completes and the segment is built, all single-value columns can have raw index

      // Dictionary-encoded column
      MutableDictionary dictionary;
      if (isDictionary) {
        DictionaryIndexConfig dictionaryIndexConfig = indexConfigs.getConfig(StandardIndexes.dictionary());
        if (dictionaryIndexConfig.isDisabled()) {
          // Even if dictionary is disabled in the config, isNoDictionaryColumn(...) returned false, so
          // we are going to create a dictionary.
          // This may happen for several reasons. For example, when there is a inverted index on the column.
          // See isNoDictionaryColumn to have more context.
          dictionaryIndexConfig = DictionaryIndexConfig.DEFAULT;
        }
        dictionary = DictionaryIndexType.createMutableDictionary(context, dictionaryIndexConfig);
      } else {
        dictionary = null;
        if (!fieldSpec.isSingleValueField()) {
          if (!dataType.isFixedWidth()) {
            throw new UnsupportedOperationException(
                "Unsupported data type: " + dataType + " for MV no-dictionary column: " + column);
          }
        }
      }

      // Null value vector
      MutableNullValueVector nullValueVector;
      if (isNullable(fieldSpec)) {
        _logger.info("Column: {} is nullable", column);
        nullValueVector = new MutableNullValueVector();
      } else {
        _logger.info("Column: {} is not nullable", column);
        nullValueVector = null;
      }

      Map<IndexType, MutableIndex> mutableIndexes =
          new MutableIndexes(indexConfigs.getConfig(StandardIndexes.vector()));
      for (IndexType<?, ?, ?> indexType : IndexService.getInstance().getAllIndexes()) {
        if (!specialIndexes.contains(indexType)) {
          addMutableIndex(mutableIndexes, indexType, context, indexConfigs);
        }
      }

      Pair<String, ValueAggregator> columnAggregatorPair =
          metricsAggregators.getOrDefault(column, Pair.of(column, null));
      String sourceColumn = columnAggregatorPair.getLeft();
      ValueAggregator valueAggregator = columnAggregatorPair.getRight();

      // TODO this can be removed after forward index contents no longer depends on text index configs
      // If the raw value is provided, use it for the forward/dictionary index of this column by wrapping the
      // already created MutableIndex with a SameValue implementation. This optimization can only be done when
      // the mutable index is being reused
      boolean reuseMutableIndex = indexConfigs.getConfig(StandardIndexes.text()).isReuseMutableIndex();
      if (reuseMutableIndex) {
        hasColumnWithReuseMutableTextIndex = true;
        Object rawValueForTextIndex = indexConfigs.getConfig(StandardIndexes.text()).getRawValueForTextIndex();
        if (rawValueForTextIndex != null) {
          if (dictionary == null) {
            MutableIndex forwardIndex = mutableIndexes.get(StandardIndexes.forward());
            mutableIndexes.put(StandardIndexes.forward(),
                new SameValueMutableForwardIndex(rawValueForTextIndex, (MutableForwardIndex) forwardIndex));
          } else {
            dictionary = new SameValueMutableDictionary(rawValueForTextIndex, dictionary);
          }
        }
      }

      if (dataType == DataType.OPEN_STRUCT && fieldSpec instanceof ComplexFieldSpec) {
        IndexConfig openStructConfig = indexConfigs.getConfig(StandardIndexes.openStruct());
        if (openStructConfig instanceof OpenStructIndexConfig && openStructConfig.isEnabled()) {
          MutableOpenStructIndex openStructIndex = new MutableOpenStructIndex(column, (ComplexFieldSpec) fieldSpec,
              (OpenStructIndexConfig) openStructConfig, _memoryManager, _capacity);
          mutableIndexes.put(StandardIndexes.openStruct(), openStructIndex);
        }
      }

      _indexContainerMap.put(column,
          new IndexContainer(fieldSpec, partitionFunction, partitions, new ValuesInfo(), mutableIndexes, dictionary,
              nullValueVector, sourceColumn, valueAggregator));
    }
    _hasColumnWithReuseMutableTextIndex = hasColumnWithReuseMutableTextIndex;

    _partitionDedupMetadataManager = config.getPartitionDedupMetadataManager();
    _dedupTimeColumn =
        _partitionDedupMetadataManager != null ? _partitionDedupMetadataManager.getContext().getDedupTimeColumn()
            : null;

    _partitionUpsertMetadataManager = config.getPartitionUpsertMetadataManager();
    if (_partitionUpsertMetadataManager != null) {
      Preconditions.checkState(!isAggregateMetricsEnabled(),
          "Metrics aggregation and upsert cannot be enabled together");
      UpsertContext upsertContext = _partitionUpsertMetadataManager.getContext();
      _upsertComparisonColumns = upsertContext.getComparisonColumns();
      _deleteRecordColumn = upsertContext.getDeleteRecordColumn();
      _upsertDropOutOfOrderRecord = upsertContext.isDropOutOfOrderRecord();
      _upsertOutOfOrderRecordColumn = upsertContext.getOutOfOrderRecordColumn();
      _upsertConsistencyMode = upsertContext.getConsistencyMode();
      _validDocIds = new ThreadSafeMutableRoaringBitmap();
      if (_deleteRecordColumn != null) {
        _queryableDocIds = new ThreadSafeMutableRoaringBitmap();
      } else {
        _queryableDocIds = null;
      }
    } else {
      _upsertComparisonColumns = null;
      _deleteRecordColumn = null;
      _upsertDropOutOfOrderRecord = false;
      _upsertOutOfOrderRecordColumn = null;
      _upsertConsistencyMode = null;
      _validDocIds = null;
      _queryableDocIds = null;
    }

    MultiColumnTextIndexConfig textConfig = config.getMultiColIndexConfig();
    if (textConfig != null) {
      List<String> textColumns = textConfig.getColumns();
      BooleanList columnsSV = new BooleanArrayList(textColumns.size());
      Schema schema = config.getSchema();
      for (String column : textColumns) {
        DataType dataType = schema.getFieldSpecFor(column).getDataType();
        if (dataType.getStoredType() != FieldSpec.DataType.STRING) {
          throw new IllegalStateException(
              "Multi-column text index is currently only supported on STRING type columns! Found column: " + column
                  + " of type: " + dataType);
        }
        columnsSV.add(schema.getFieldSpecFor(column).isSingleValueField());
      }
      _multiColumnTextIndex =
          new MultiColumnRealtimeLuceneTextIndex(textColumns, columnsSV, _consumerDir, config.getSegmentName(),
              textConfig);
      _multiColumnPos = _multiColumnTextIndex.getMapping();
      _multiColumnValues = new ArrayList<>(_multiColumnPos.size());
      for (int i = 0; i < _multiColumnPos.size(); i++) {
        _multiColumnValues.add(null);
      }
      _multiColumnTextMetadata = new MultiColumnTextMetadata(MultiColumnTextMetadata.VERSION_1, textConfig.getColumns(),
          textConfig.getProperties(), textConfig.getPerColumnProperties());
    } else {
      _multiColumnTextIndex = null;
      _multiColumnPos = null;
      _multiColumnValues = null;
      _multiColumnTextMetadata = null;
    }
  }

  private static Map<String, Pair<String, ValueAggregator>> getMetricsAggregators(RealtimeSegmentConfig segmentConfig) {
    if (segmentConfig.aggregateMetrics()) {
      return fromAggregateMetrics(segmentConfig);
    } else if (CollectionUtils.isNotEmpty(segmentConfig.getIngestionAggregationConfigs())) {
      return fromAggregationConfig(segmentConfig);
    } else {
      return Map.of();
    }
  }

  private static Map<String, Pair<String, ValueAggregator>> fromAggregateMetrics(RealtimeSegmentConfig segmentConfig) {
    Preconditions.checkState(CollectionUtils.isEmpty(segmentConfig.getIngestionAggregationConfigs()),
        "aggregateMetrics cannot be enabled if AggregationConfig is set");

    List<String> metricNames = segmentConfig.getSchema().getMetricNames();
    Map<String, Pair<String, ValueAggregator>> columnNameToAggregator =
        Maps.newHashMapWithExpectedSize(metricNames.size());
    for (String metricName : metricNames) {
      columnNameToAggregator.put(metricName, Pair.of(metricName,
          ValueAggregatorFactory.getValueAggregator(AggregationFunctionType.SUM, List.of())));
    }
    return columnNameToAggregator;
  }

  private static Map<String, Pair<String, ValueAggregator>> fromAggregationConfig(RealtimeSegmentConfig segmentConfig) {
    List<AggregationConfig> aggregationConfigs = segmentConfig.getIngestionAggregationConfigs();
    assert !segmentConfig.aggregateMetrics() && CollectionUtils.isNotEmpty(aggregationConfigs);
    Map<String, Pair<String, ValueAggregator>> columnNameToAggregator =
        Maps.newHashMapWithExpectedSize(aggregationConfigs.size());
    for (AggregationConfig config : aggregationConfigs) {
      ExpressionContext expressionContext = RequestContextUtils.getExpression(config.getAggregationFunction());
      // validation is also done when the table is created, this is just a sanity check.
      Preconditions.checkState(expressionContext.getType() == ExpressionContext.Type.FUNCTION,
          "aggregation function must be a function: %s", config);
      FunctionContext functionContext = expressionContext.getFunction();
      AggregationFunctionType functionType =
          AggregationFunctionType.getAggregationFunctionType(functionContext.getFunctionName());
      List<ExpressionContext> arguments = functionContext.getArguments();
      ExpressionContext argument = arguments.get(0);
      Preconditions.checkState(argument.getType() == ExpressionContext.Type.IDENTIFIER,
          "aggregator function argument must be a identifier: %s", config);
      ValueAggregator valueAggregator =
          ValueAggregatorFactory.getValueAggregator(functionType, arguments.subList(1, arguments.size()));
      Preconditions.checkState(valueAggregator.isAggregatedValueFixedSize(),
          "aggregator function must have fixed size aggregated value: %s", config);

      columnNameToAggregator.put(config.getColumnName(), Pair.of(argument.getIdentifier(), valueAggregator));
    }

    return columnNameToAggregator;
  }

  private boolean isNullable(FieldSpec fieldSpec) {
    return _schema.isEnableColumnBasedNullHandling() ? fieldSpec.isNullable() : _defaultNullHandlingEnabled;
  }

  private <C extends IndexConfig> void addMutableIndex(Map<IndexType, MutableIndex> mutableIndexes,
      IndexType<C, ?, ?> indexType, MutableIndexContext context, FieldIndexConfigs indexConfigs) {
    MutableIndex mutableIndex = indexType.createMutableIndex(context, indexConfigs.getConfig(indexType));
    if (mutableIndex != null) {
      mutableIndexes.put(indexType, mutableIndex);
    }
  }

  /// Decide whether a given column should be dictionary encoded or not
  /// @param fieldSpec field spec of column
  /// @param column column name
  /// @return true if column is no-dictionary, false if dictionary encoded
  private boolean isNoDictionaryColumn(FieldIndexConfigs indexConfigs, FieldSpec fieldSpec, String column) {
    DataType dataType = fieldSpec.getDataType();
    if (dataType == DataType.MAP || dataType == DataType.OPEN_STRUCT) {
      return true;
    }
    if (indexConfigs == null) {
      return false;
    }
    if (indexConfigs.getConfig(StandardIndexes.dictionary()).isEnabled()) {
      return false;
    }
    // Metrics aggregation keys each row on the dictionary ids of the dimension and time columns (see
    // getOrCreateDocId), so those columns must be dictionary encoded in the consuming segment even when the table
    // config marks them as no-dictionary. The consuming-segment dictionary is a transient structure that only exists
    // to drive the in-memory rollup; the committed segment is rebuilt from the table config (see
    // RealtimeSegmentConverter), so the no-dictionary setting is still honored there. Metric columns are excluded:
    // aggregated values are mutated in place in the raw forward index and must stay no-dictionary.
    FieldSpec.FieldType fieldType = fieldSpec.getFieldType();
    if (isAggregateMetricsEnabled() && (fieldType == FieldSpec.FieldType.DIMENSION
        || fieldType == FieldSpec.FieldType.DATE_TIME || fieldType == FieldSpec.FieldType.TIME)) {
      _logger.info("Metrics aggregation is enabled. Will create dictionary in consuming segment for key column: {} of "
          + "type: {}", column, dataType);
      return false;
    }
    // So don't create dictionary if the column (1) is member of noDictionary, and (2) is single-value or multi-value
    // with a fixed-width field, and (3) doesn't have an inverted index
    return (fieldSpec.isSingleValueField() || fieldSpec.getDataType().isFixedWidth()) && indexConfigs.getConfig(
        StandardIndexes.inverted()).isDisabled();
  }

  public SegmentPartitionConfig getSegmentPartitionConfig() {
    if (_partitionColumn != null) {
      return new SegmentPartitionConfig(Map.of(_partitionColumn,
          new ColumnPartitionConfig(_partitionFunction.getName(), _partitionFunction.getNumPartitions(),
              _partitionFunction.getFunctionConfig())));
    } else {
      return null;
    }
  }

  @Override
  public boolean index(GenericRow row, @Nullable StreamMessageMetadata metadata)
      throws IOException {
    _pendingRowIndexingException = null;
    if (_partitionColumn != null) {
      Object value = row.getValue(_partitionColumn);
      Preconditions.checkState(value != null, "Failed to find value for partition column: %s", _partitionColumn);
      IndexContainer indexContainer = _indexContainerMap.get(_partitionColumn);
      String stringValue = indexContainer._fieldSpec.getDataType().toString(value);
      int partition = _partitionFunction.getPartition(stringValue);
      if (partition != _mainPartitionId) {
        if (_serverMetrics != null) {
          _serverMetrics.addMeteredTableValue(_realtimeTableName, ServerMeter.REALTIME_PARTITION_MISMATCH, 1);
        }
        if (_dropRecordOnPartitionMismatch) {
          updateIndexedAndIngestionTime(metadata);
          return true;
        }
        if (indexContainer._partitions.add(partition)) {
          // for every partition other than mainPartitionId, log a warning once
          _logger.warn("Found new partition: {} from partition column: {}, value: {}", partition, _partitionColumn,
              stringValue);
        }
      }
    }

    if (isDedupEnabled()) {
      DedupRecordInfo dedupRecordInfo = getDedupRecordInfo(row);
      if (_partitionDedupMetadataManager.checkRecordPresentOrUpdate(dedupRecordInfo, this)) {
        if (_serverMetrics != null) {
          _serverMetrics.addMeteredTableValue(_realtimeTableName, ServerMeter.REALTIME_DEDUP_DROPPED, 1);
        }
        updateIndexedAndIngestionTime(metadata);
        return true;
      }
    }

    // Validate the length of each multi-value to ensure it can be properly stored in the underlying forward index.
    // If the length of any MV column exceeds the capacity of a chunk in the forward index, an exception is thrown.
    // If an exception is not thrown, it leads to a mismatch in the number of values in the MV column compared to
    // other columns when sealing the segment (due to the overflow), causing the sealing process to fail.
    // NOTE: We must do this before we index a single column to avoid partially indexing the row
    validateLengthOfMVColumns(row);

    boolean canTakeMore;
    int numDocsIndexed = _numDocsIndexed;
    if (isUpsertEnabled()) {
      RecordInfo recordInfo = getRecordInfo(row, numDocsIndexed);
      GenericRow updatedRow = _partitionUpsertMetadataManager.updateRecord(row, recordInfo);
      // NOTE: out-of-order records can not be dropped or marked when consistent upsert view is enabled.
      // Since Indexing the record and updation of _numDocsIndexed counter happens before updating the upsert
      // metadata, we wouldn't be able to actually drop or mark those records as dropped. This order is important for
      // consistent upsert view, otherwise the latest doc can be missed by query due to 'docId < _numDocs' check
      // in query filter operators. Here the record becomes queryable before validDocIds bitmaps are updated.
      if (_upsertConsistencyMode != UpsertConfig.ConsistencyMode.NONE) {
        // Fail-soft physical index (issue #16316): complete the row before advancing doc count / upsert metadata.
        indexPhysicalRow(numDocsIndexed, updatedRow);
        numDocsIndexed++;
        canTakeMore = numDocsIndexed < _capacity;
        _numDocsIndexed = numDocsIndexed;
        // Index the record and update _numDocsIndexed counter before updating the upsert metadata so that the record
        // becomes queryable before validDocIds bitmaps are updated. This order is important for consistent upsert view,
        // otherwise the latest doc can be missed by query due to 'docId < _numDocs' check in query filter operators.
        // NOTE: out-of-order records can not be dropped or marked when consistent upsert view is enabled.
        _partitionUpsertMetadataManager.addRecord(this, recordInfo);
        throwPendingRowIndexingExceptionIfStrict();
      } else {
        // if record doesn't need to be dropped, then persist in segment and update metadata hashmap
        // we are doing metadata update first followed by segment data update here, there can be a scenario where
        // segment indexing or addNewRow call errors out in those scenario, there can be metadata inconsistency where
        // a key is pointing to some other key's docID
        // TODO fix this metadata mismatch scenario (ConsistencyMode.NONE ordering) — follow-up to #16316
        boolean isOutOfOrderRecord = !_partitionUpsertMetadataManager.addRecord(this, recordInfo);
        if (_upsertOutOfOrderRecordColumn != null) {
          updatedRow.putValue(_upsertOutOfOrderRecordColumn, BooleanUtils.toInt(isOutOfOrderRecord));
        }
        if (!isOutOfOrderRecord || !_upsertDropOutOfOrderRecord) {
          // Fail-soft: physical write always completes the row so metadata docId is never a hole.
          indexPhysicalRow(numDocsIndexed, updatedRow);
          // Update number of documents indexed before handling the upsert metadata so that the record becomes queryable
          // once validated
          numDocsIndexed++;
        }
        canTakeMore = numDocsIndexed < _capacity;
        _numDocsIndexed = numDocsIndexed;
        throwPendingRowIndexingExceptionIfStrict();
      }
    } else {
      // If metrics aggregation is enabled and if the dimension values were already seen, this will return existing
      // docId, else this will return a new docId. Dictionary must be updated before the rollup key is computed.
      boolean dictHadError = updateDictionary(row);
      int docId = getOrCreateDocId();

      if (docId == numDocsIndexed) {
        // New row — fail-soft complete-the-row so _numDocsIndexed and per-column lengths stay aligned (#16316).
        boolean rowHadError = addNewRow(numDocsIndexed, row);
        if (dictHadError || rowHadError) {
          recordIncompleteRow();
        }
        // Update number of documents indexed at last to make the latest row queryable
        canTakeMore = numDocsIndexed++ < _capacity;
      } else {
        assert isAggregateMetricsEnabled();
        try {
          aggregateMetrics(row, docId);
          if (dictHadError) {
            recordIncompleteRow();
          }
        } catch (Exception e) {
          // In-place rollup already has a complete prior row. When continueOnError is disabled, rethrow so
          // strict ingestion fails the index call. Already-written metrics on this docId are not rolled back
          // (apply-then-write is per-metric); a two-phase rewrite is left as a follow-up.
          recordOrThrowIndexingError("AGGREGATE_METRICS", e);
          recordIncompleteRow();
        }
        canTakeMore = true;
      }
      _numDocsIndexed = numDocsIndexed;
      throwPendingRowIndexingExceptionIfStrict();
    }

    updateIndexedAndIngestionTime(metadata);
    return canTakeMore;
  }

  private void updateIndexedAndIngestionTime(@Nullable StreamMessageMetadata metadata) {
    _lastIndexedTimeMs = System.currentTimeMillis();
    if (metadata != null) {
      updateIngestionTimestamp(metadata.getRecordIngestionTimeMs());
    }
  }

  /// Updates ingestion timestamp metadata. This is a public function to allow
  /// external components to update the ingestion timestamp metadata without indexing a row.
  public void updateIngestionTimestamp(long recordIngestionTimeMs) {
    long now = System.currentTimeMillis();
    _latestIngestionTimeMs = Math.max(_latestIngestionTimeMs, recordIngestionTimeMs);
    long ingestionLagMs = Math.max(0, now - _latestIngestionTimeMs);
    _minimumIngestionLagMs = Math.min(_minimumIngestionLagMs, ingestionLagMs);
  }

  private boolean isUpsertEnabled() {
    return _partitionUpsertMetadataManager != null;
  }

  private boolean isDedupEnabled() {
    return _partitionDedupMetadataManager != null;
  }

  private DedupRecordInfo getDedupRecordInfo(GenericRow row) {
    PrimaryKey primaryKey = row.getPrimaryKey(_schema.getPrimaryKeyColumns());
    // it is okay not having dedup time column if metadata ttl is not enabled
    if (_dedupTimeColumn == null) {
      return new DedupRecordInfo(primaryKey);
    }
    double dedupTime = ((Number) row.getValue(_dedupTimeColumn)).doubleValue();
    return new DedupRecordInfo(primaryKey, dedupTime);
  }

  private RecordInfo getRecordInfo(GenericRow row, int docId) {
    PrimaryKey primaryKey = row.getPrimaryKey(_schema.getPrimaryKeyColumns());
    Comparable comparisonValue = getComparisonValue(row);
    boolean deleteRecord = _deleteRecordColumn != null && BooleanUtils.toBoolean(row.getValue(_deleteRecordColumn));
    return new RecordInfo(primaryKey, docId, comparisonValue, deleteRecord);
  }

  private Comparable getComparisonValue(GenericRow row) {
    int numComparisonColumns = _upsertComparisonColumns.size();
    if (numComparisonColumns == 1) {
      String comparisonColumn = _upsertComparisonColumns.get(0);
      return toComparable(row.getValue(comparisonColumn));
    }

    Comparable[] comparisonValues = new Comparable[numComparisonColumns];
    int comparableIndex = -1;
    for (int i = 0; i < numComparisonColumns; i++) {
      String columnName = _upsertComparisonColumns.get(i);

      if (!row.isNullValue(columnName)) {
        // Inbound records may only have exactly 1 non-null value in one of the comparison column i.e. comparison
        // columns are mutually exclusive. If comparableIndex has already been modified from its initialized value,
        // that means there must have already been a non-null value processed and therefore processing an additional
        // non-null value would be an error.
        Preconditions.checkState(comparableIndex == -1,
            "Documents must have exactly 1 non-null comparison column value");

        comparableIndex = i;
        comparisonValues[i] = toComparable(row.getValue(columnName));
      }
    }
    Preconditions.checkState(comparableIndex != -1, "Documents must have exactly 1 non-null comparison column value");
    return new ComparisonColumns(comparisonValues, comparableIndex);
  }

  /// @param row
  /// @throws UnsupportedOperationException if the length of an MV column would exceed the
  /// capacity of a chunk in the ForwardIndex
  private void validateLengthOfMVColumns(GenericRow row)
      throws UnsupportedOperationException {
    for (Map.Entry<String, IndexContainer> entry : _indexContainerMap.entrySet()) {
      IndexContainer indexContainer = entry.getValue();
      FieldSpec fieldSpec = indexContainer._fieldSpec;
      MutableIndex forwardIndex = indexContainer._mutableIndexes.get(StandardIndexes.forward());
      if (fieldSpec.isSingleValueField() || !(forwardIndex instanceof FixedByteMVMutableForwardIndex)) {
        continue;
      }

      Object[] values = (Object[]) row.getValue(entry.getKey());
      // Note that max chunk capacity is derived from "FixedByteMVMutableForwardIndex._maxNumberOfMultiValuesPerRow"
      // which is set to "1000" in "ForwardIndexType.MAX_MULTI_VALUES_PER_ROW". If the number of values in the
      // multi-value entry that we are attempting to ingest is greater than the maximum accepted value, we throw an
      // UnsupportedOperationException.
      int maxChunkCapacity = ((FixedByteMVMutableForwardIndex) forwardIndex).getMaxChunkCapacity();
      if (values.length > maxChunkCapacity) {
        throw new UnsupportedOperationException(
            "Length of MV column " + entry.getKey() + " is longer than ForwardIndex's capacity per chunk.");
      }
    }
  }

  /// Runs dictionary + forward/secondary indexing for a new docId and meters an incomplete row when either step had
  /// to fall back to defaults (issue #16316).
  private void indexPhysicalRow(int docId, GenericRow row) {
    boolean dictHadError = updateDictionary(row);
    boolean rowHadError = addNewRow(docId, row);
    if (dictHadError || rowHadError) {
      recordIncompleteRow();
    }
  }

  /// @return {@code true} if any column required a default/fallback while updating dictionaries
  /// When `continueOnError` is false, dictionary failures are stashed and rethrown after the row is published.
  private boolean updateDictionary(GenericRow row) {
    boolean hadError = false;
    for (Map.Entry<String, IndexContainer> entry : _indexContainerMap.entrySet()) {
      IndexContainer indexContainer = entry.getValue();
      MutableDictionary dictionary = indexContainer._dictionary;
      if (dictionary == null) {
        continue;
      }
      String column = entry.getKey();
      Object value = row.getValue(column);
      try {
        if (value == null) {
          // Prefer default-null dict entry so addNewRow can still complete the forward index for this docId
          // (fail-soft; issue #16316). Meter and fall back to field-spec default.
          recordIndexingError("DICTIONARY");
          hadError = true;
          value = getDefaultNullValueForIndexing(indexContainer._fieldSpec);
          row.putDefaultNullValue(column, value);
        }
        if (indexContainer._fieldSpec.isSingleValueField()) {
          indexContainer._dictId = dictionary.index(value);
        } else {
          indexContainer._dictIds = dictionary.index((Object[]) value);
        }
        // Update min/max value from dictionary
        indexContainer._minValue = dictionary.getMinVal();
        indexContainer._maxValue = dictionary.getMaxVal();
      } catch (Exception e) {
        // Do not abort the row mid-dictionary: remaining columns still get a chance, and addNewRow will fill
        // defaults for this column if dict ids are unset (Integer.MIN_VALUE / null). When continueOnError is
        // false the exception is stashed and rethrown after the caller publishes the docId so upsert/dedup
        // metadata never points at an unpublished row.
        recordOrDeferIndexingError("DICTIONARY", e);
        hadError = true;
        // Error sentinels, kept only if even the default cannot be indexed below. For MV columns the sentinel is a
        // null array, which is distinct from an empty array (a row that legitimately carries no values).
        indexContainer._dictId = Integer.MIN_VALUE;
        indexContainer._dictIds = null;
        try {
          // Index the field default instead of leaving the sentinel: with metrics aggregation the rollup key is built
          // straight from the dict ids (see getOrCreateDocId), and Integer.MIN_VALUE is not a real dict id, so the
          // sentinel key never matches the default value that addNewRow actually stores for this column. Two failed
          // rows now deliberately share the default-value key, which is better than colliding on a raw sentinel
          // shared with any other failing column combination.
          Object defaultValue = getDefaultNullValueForIndexing(indexContainer._fieldSpec);
          if (indexContainer._fieldSpec.isSingleValueField()) {
            indexContainer._dictId = dictionary.index(defaultValue);
          } else {
            indexContainer._dictIds = dictionary.index((Object[]) defaultValue);
          }
          // Keep the row consistent with the dict id: addNewRow writes this default to the forward and secondary
          // indexes and marks the value null for null-aware queries.
          row.putDefaultNullValue(column, defaultValue);
          indexContainer._minValue = dictionary.getMinVal();
          indexContainer._maxValue = dictionary.getMaxVal();
        } catch (Exception fallbackError) {
          _logger.error("Failed to index default null value into dictionary for column: {}", column, fallbackError);
        }
      }
      updateIndexCapacityThresholdBreached(dictionary, column);
    }
    return hadError;
  }

  /// Indexes a new physical row. Always completes the row so seal/query lengths stay aligned with [_numDocsIndexed]
  /// (issue #16316). On forward-index failure the column is completed with the field default/null. Secondary index
  /// and aggregation-path failures are metered and, when `continueOnError` is false, stashed for
  /// [#throwPendingRowIndexingExceptionIfStrict] after the caller publishes the docId.
  ///
  /// @return {@code true} if any column required a default/fallback while indexing
  private boolean addNewRow(int docId, GenericRow row) {
    boolean rowHadError = false;
    for (Map.Entry<String, IndexContainer> entry : _indexContainerMap.entrySet()) {
      String column = entry.getKey();
      IndexContainer indexContainer = entry.getValue();
      try {
        if (indexContainer._valueAggregator != null) {
          if (!addAggregatedColumn(docId, row, column, indexContainer)) {
            rowHadError = true;
          }
        } else if (!addPhysicalColumn(docId, row, column, indexContainer)) {
          rowHadError = true;
        }
      } catch (Exception e) {
        // Last-resort complete-the-row so a single bad column cannot leave a half-written docId.
        recordOrDeferIndexingError("ROW", e);
        rowHadError = true;
        try {
          indexDefaultNullColumn(docId, indexContainer);
        } catch (Exception fallbackError) {
          _logger.error("Failed to index default null for column: {} at docId: {}", column, docId, fallbackError);
        }
      }
    }

    if (_multiColumnValues != null) {
      try {
        _multiColumnTextIndex.add(_multiColumnValues);
      } catch (Exception e) {
        recordOrDeferIndexingError("MULTI_COLUMN_TEXT", e);
        rowHadError = true;
      } finally {
        Collections.fill(_multiColumnValues, null);
      }
    }
    return rowHadError;
  }

  /// Returns {@code true} when the aggregated column was written without error.
  private boolean addAggregatedColumn(int docId, GenericRow row, String column, IndexContainer indexContainer) {
    ValueAggregator valueAggregator = indexContainer._valueAggregator;
    String sourceColumn = indexContainer._sourceColumn;
    // NOTE: value can be null if the column is not specified in the schema.
    Object value = row.getValue(sourceColumn);
    // Handle COUNT(*)
    if (value == null && sourceColumn.equals(AggregationFunctionColumnPair.STAR)) {
      assert valueAggregator.getAggregationType() == AggregationFunctionType.COUNT;
      value = 1;
    }

    MutableIndex forwardIndex = indexContainer._mutableIndexes.get(StandardIndexes.forward());
    FieldSpec fieldSpec = indexContainer._fieldSpec;
    DataType dataType = fieldSpec.getDataType();
    try {
      value = valueAggregator.getInitialAggregatedValue(value);
      // BIG_DECIMAL is actually stored as byte[] and hence can be supported here.
      switch (dataType.getStoredType()) {
        case INT:
          forwardIndex.add(((Number) value).intValue(), -1, docId);
          break;
        case LONG:
          forwardIndex.add(((Number) value).longValue(), -1, docId);
          break;
        case FLOAT:
          forwardIndex.add(((Number) value).floatValue(), -1, docId);
          break;
        case DOUBLE:
          forwardIndex.add(((Number) value).doubleValue(), -1, docId);
          break;
        case BIG_DECIMAL:
        case BYTES:
          forwardIndex.add(valueAggregator.serializeAggregatedValue(value), -1, docId);
          break;
        default:
          throw new UnsupportedOperationException(
              "Unsupported data type: " + dataType + " for aggregation: " + column);
      }
      indexContainer._valuesInfo.updateSVNumValues();
      return true;
    } catch (Exception e) {
      recordOrDeferIndexingError(StandardIndexes.forward(), e);
      indexDefaultAggregatedValue(docId, indexContainer);
      return false;
    }
  }

  /// Returns {@code true} when the physical column was written from the row value without error and without falling
  /// back to the field default.
  private boolean addPhysicalColumn(int docId, GenericRow row, String column, IndexContainer indexContainer) {
    FieldSpec fieldSpec = indexContainer._fieldSpec;
    DataType dataType = fieldSpec.getDataType();
    boolean isNull = row.isNullValue(column);
    Object value = row.getValue(column);
    // Folded into every return path so addNewRow meters the row incomplete once, even when several columns fall back.
    boolean defaultSubstituted = false;
    if (value == null) {
      // Should not happen after NullValueTransformer, but complete the row with defaults rather than leaving a hole.
      recordIndexingError("NULL_VALUE");
      value = getDefaultNullValueForIndexing(fieldSpec);
      isNull = true;
      defaultSubstituted = true;
    }
    if (indexContainer._nullValueVector != null && isNull) {
      indexContainer._nullValueVector.setNull(docId);
    }

    if (fieldSpec.isSingleValueField()) {
      // Route OPEN_STRUCT values to the dedicated mutable index. OPEN_STRUCT has no forward
      // index / dictionary / min-max, so the standard per-IndexType loop and the comparable
      // tracking below would be no-ops at best and crash at worst (Map is not Comparable).
      if (dataType == DataType.OPEN_STRUCT) {
        MutableIndex openStructIndex = indexContainer._mutableIndexes.get(StandardIndexes.openStruct());
        if (openStructIndex != null) {
          try {
            openStructIndex.add(value, -1, docId);
          } catch (Exception e) {
            recordOrDeferIndexingError(StandardIndexes.openStruct(), e);
            return false;
          }
        }
        indexContainer._valuesInfo.updateSVNumValues();
        return !defaultSubstituted;
      }

      int dictId = indexContainer._dictId;
      if (indexContainer._dictionary != null && dictId == Integer.MIN_VALUE) {
        // Dictionary indexing failed earlier; index the default so forward index can still be written.
        try {
          Object defaultValue = getDefaultNullValueForIndexing(fieldSpec);
          dictId = indexContainer._dictionary.index(defaultValue);
          indexContainer._dictId = dictId;
          value = defaultValue;
          if (indexContainer._nullValueVector != null) {
            indexContainer._nullValueVector.setNull(docId);
          }
        } catch (Exception e) {
          recordOrDeferIndexingError("DICTIONARY", e);
          return false;
        }
      }

      boolean forwardWritten = false;
      boolean hadError = false;
      for (Map.Entry<IndexType, MutableIndex> indexEntry : indexContainer._mutableIndexes.entrySet()) {
        IndexType indexType = indexEntry.getKey();
        try {
          MutableIndex mutableIndex = indexEntry.getValue();
          mutableIndex.add(value, dictId, docId);
          updateIndexCapacityThresholdBreached(mutableIndex, indexType, column);
          if (indexType.equals(StandardIndexes.forward())) {
            forwardWritten = true;
          }
        } catch (Exception e) {
          recordOrDeferIndexingError(indexType, e);
          hadError = true;
          // Forward-index failure is a row-level integrity issue: complete with default rather than skip.
          if (indexType.equals(StandardIndexes.forward())) {
            try {
              Object defaultValue = getDefaultNullValueForIndexing(fieldSpec);
              int defaultDictId = dictId;
              if (indexContainer._dictionary != null) {
                defaultDictId = indexContainer._dictionary.index(defaultValue);
                indexContainer._dictId = defaultDictId;
              }
              indexEntry.getValue().add(defaultValue, defaultDictId, docId);
              forwardWritten = true;
              if (indexContainer._nullValueVector != null) {
                indexContainer._nullValueVector.setNull(docId);
              }
              value = defaultValue;
              dictId = defaultDictId;
            } catch (Exception fallbackError) {
              _logger.error("Failed to write default forward index for column: {} at docId: {}", column, docId,
                  fallbackError);
            }
          }
        }
      }
      if (!forwardWritten && indexContainer._mutableIndexes.containsKey(StandardIndexes.forward())) {
        // Should be unreachable if the fallback above worked; still try once more.
        indexDefaultNullColumn(docId, indexContainer);
        hadError = true;
      } else {
        indexContainer._valuesInfo.updateSVNumValues();
      }

      if (dictId < 0) {
        // Update min/max value from raw value
        // NOTE: Skip updating min/max value for aggregated metrics because the value will change over time.
        if (!isAggregateMetricsEnabled() || fieldSpec.getFieldType() != FieldSpec.FieldType.METRIC) {
          Comparable comparable = toComparableValue(value, dataType, column);
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

      if (_multiColumnValues != null) {
        int pos = _multiColumnPos.getInt(column);
        if (pos > -1) {
          _multiColumnValues.set(pos, value);
        }
      }
      return !hadError && !defaultSubstituted;
    } else {
      // Multi-value column
      Object[] values = value instanceof Object[] ? (Object[]) value
          : new Object[]{value};
      int[] dictIds = indexContainer._dictIds;
      if (indexContainer._dictionary != null && dictIds == null) {
        try {
          Object[] defaultValues = (Object[]) getDefaultNullValueForIndexing(fieldSpec);
          dictIds = indexContainer._dictionary.index(defaultValues);
          indexContainer._dictIds = dictIds;
          values = defaultValues;
          if (indexContainer._nullValueVector != null) {
            indexContainer._nullValueVector.setNull(docId);
          }
        } catch (Exception e) {
          recordOrDeferIndexingError("DICTIONARY", e);
          return false;
        }
      }

      boolean forwardWritten = false;
      boolean hadError = false;
      indexContainer._valuesInfo.updateVarByteMVMaxRowLengthInBytes(values, dataType.getStoredType());
      for (Map.Entry<IndexType, MutableIndex> indexEntry : indexContainer._mutableIndexes.entrySet()) {
        IndexType indexType = indexEntry.getKey();
        try {
          MutableIndex mutableIndex = indexEntry.getValue();
          mutableIndex.add(values, dictIds, docId);
          updateIndexCapacityThresholdBreached(mutableIndex, indexType, column);
          if (indexType.equals(StandardIndexes.forward())) {
            forwardWritten = true;
          }
        } catch (Exception e) {
          recordOrDeferIndexingError(indexType, e);
          hadError = true;
          if (indexType.equals(StandardIndexes.forward())) {
            try {
              Object[] defaultValues = (Object[]) getDefaultNullValueForIndexing(fieldSpec);
              int[] defaultDictIds = dictIds;
              if (indexContainer._dictionary != null) {
                defaultDictIds = indexContainer._dictionary.index(defaultValues);
                indexContainer._dictIds = defaultDictIds;
              }
              indexEntry.getValue().add(defaultValues, defaultDictIds, docId);
              forwardWritten = true;
              values = defaultValues;
              if (indexContainer._nullValueVector != null) {
                indexContainer._nullValueVector.setNull(docId);
              }
            } catch (Exception fallbackError) {
              _logger.error("Failed to write default MV forward index for column: {} at docId: {}", column, docId,
                  fallbackError);
            }
          }
        }
      }
      if (!forwardWritten && indexContainer._mutableIndexes.containsKey(StandardIndexes.forward())) {
        indexDefaultNullColumn(docId, indexContainer);
        hadError = true;
      } else {
        indexContainer._valuesInfo.updateMVNumValues(values.length);
      }

      if (_multiColumnValues != null) {
        int pos = _multiColumnPos.getInt(column);
        if (pos > -1) {
          _multiColumnValues.set(pos, values);
        }
      }
      return !hadError && !defaultSubstituted;
    }
  }

  private static Object getDefaultNullValueForIndexing(FieldSpec fieldSpec) {
    Object defaultNullValue = fieldSpec.getDefaultNullValue();
    if (fieldSpec.isSingleValueField()) {
      return defaultNullValue;
    }
    return new Object[]{defaultNullValue};
  }

  private void indexDefaultNullColumn(int docId, IndexContainer indexContainer) {
    FieldSpec fieldSpec = indexContainer._fieldSpec;
    Object defaultValue = getDefaultNullValueForIndexing(fieldSpec);
    if (indexContainer._nullValueVector != null) {
      indexContainer._nullValueVector.setNull(docId);
    }
    if (fieldSpec.getDataType() == DataType.OPEN_STRUCT) {
      MutableIndex openStructIndex = indexContainer._mutableIndexes.get(StandardIndexes.openStruct());
      if (openStructIndex != null) {
        openStructIndex.add(defaultValue, -1, docId);
      }
      indexContainer._valuesInfo.updateSVNumValues();
      return;
    }
    MutableIndex forwardIndex = indexContainer._mutableIndexes.get(StandardIndexes.forward());
    if (forwardIndex == null) {
      return;
    }
    if (fieldSpec.isSingleValueField()) {
      int dictId = -1;
      if (indexContainer._dictionary != null) {
        dictId = indexContainer._dictionary.index(defaultValue);
        indexContainer._dictId = dictId;
      }
      forwardIndex.add(defaultValue, dictId, docId);
      indexContainer._valuesInfo.updateSVNumValues();
    } else {
      Object[] defaultValues = (Object[]) defaultValue;
      int[] dictIds = null;
      if (indexContainer._dictionary != null) {
        dictIds = indexContainer._dictionary.index(defaultValues);
        indexContainer._dictIds = dictIds;
      }
      forwardIndex.add(defaultValues, dictIds, docId);
      indexContainer._valuesInfo.updateMVNumValues(defaultValues.length);
    }
  }

  private void indexDefaultAggregatedValue(int docId, IndexContainer indexContainer) {
    ValueAggregator valueAggregator = indexContainer._valueAggregator;
    MutableIndex forwardIndex = indexContainer._mutableIndexes.get(StandardIndexes.forward());
    DataType dataType = indexContainer._fieldSpec.getDataType();
    Object value = valueAggregator.getInitialAggregatedValue(null);
    switch (dataType.getStoredType()) {
      case INT:
        forwardIndex.add(((Number) value).intValue(), -1, docId);
        break;
      case LONG:
        forwardIndex.add(((Number) value).longValue(), -1, docId);
        break;
      case FLOAT:
        forwardIndex.add(((Number) value).floatValue(), -1, docId);
        break;
      case DOUBLE:
        forwardIndex.add(((Number) value).doubleValue(), -1, docId);
        break;
      case BIG_DECIMAL:
      case BYTES:
        forwardIndex.add(valueAggregator.serializeAggregatedValue(value), -1, docId);
        break;
      default:
        throw new UnsupportedOperationException(
            "Unsupported data type: " + dataType + " for aggregation default at docId: " + docId);
    }
    indexContainer._valuesInfo.updateSVNumValues();
  }

  /// Wraps a raw comparison-column value as a Comparable without a per-row schema lookup: a byte[] (a BYTES or UUID
  /// comparison column) becomes a ByteArray; every other type is already Comparable. Mirrors
  /// UpsertUtils.SingleComparisonColumnReader so the write and read paths agree.
  private static Comparable toComparable(Object value) {
    if (value instanceof byte[]) {
      return new ByteArray((byte[]) value);
    }
    Preconditions.checkState(value instanceof Comparable, "Upsert comparison column value must be comparable: %s",
        value);
    return (Comparable) value;
  }

  private Comparable toComparableValue(Object value, DataType dataType, @Nullable String columnName) {
    if (dataType == MAP) {
      return new ByteArray(MapUtils.serializeMap((Map) value));
    }
    if (dataType.getStoredType() == BYTES) {
      return new ByteArray((byte[]) value);
    }
    Preconditions.checkState(value instanceof Comparable, "Column: %s must be comparable", columnName);
    return (Comparable) value;
  }

  private void updateIndexCapacityThresholdBreached(MutableIndex mutableIndex, IndexType indexType, String column) {
    // Few of the Immutable version of the mutable index are bounded by size like
    // {@link VarByteChunkForwardIndexWriterV4#putBytes(byte[])} and {@link FixedBitMVForwardIndex}
    // If num of values or size is above limit, A mutable index is unable to convert to an immutable index and segment
    // build fails causing the realtime consumption to stop. Hence, The below check is a temporary measure to avoid
    // such scenarios until immutable index implementations are changed.
    if (!_indexCapacityThresholdBreached && !mutableIndex.canAddMore()) {
      _logger.info(
          "Index: {} for column: {} cannot consume more rows, marking _indexCapacityThresholdBreached as true",
          indexType, column
      );
      _indexCapacityThresholdBreached = true;
    }
  }

  private void updateIndexCapacityThresholdBreached(MutableDictionary dictionary, String column) {
    // If optimizeDictionary is enabled, Immutable version of the mutable dictionary may become raw forward index.
    // Some of them may be bounded by size like
    // {@link VarByteChunkForwardIndexWriterV4#putBytes(byte[])} and {@link FixedBitMVForwardIndex}
    // If num of values or size is above limit, A mutable index is unable to convert to an immutable index and segment
    // build fails causing the realtime consumption to stop. Hence, The below check is a temporary measure to avoid
    // such scenarios until immutable index implementations are changed.
    if (!_indexCapacityThresholdBreached && !dictionary.canAddMore()) {
      _logger.info(
          "Dictionary for column: {} cannot consume more rows, marking _indexCapacityThresholdBreached as true", column
      );
      _indexCapacityThresholdBreached = true;
    }
  }

  /// When [#_continueOnError] is false, rethrows so strict ingestion fails the index operation. When true,
  /// records the error metric and returns so the caller can complete the row with defaults.
  private void recordOrThrowIndexingError(String indexType, Exception exception) {
    if (!_continueOnError) {
      throw wrapIndexingException(indexType, exception);
    }
    recordIndexingError(indexType, exception);
  }

  /// Records an addNewRow indexing error. When [#_continueOnError] is false the exception is stashed so
  /// [#index] can publish the completed row (avoiding inverted-index holes) and then rethrow.
  private void recordOrDeferIndexingError(IndexType<?, ?, ?> indexType, Exception exception) {
    recordIndexingError(indexType, exception);
    if (!_continueOnError && _pendingRowIndexingException == null) {
      _pendingRowIndexingException = exception;
    }
  }

  /// Records an addNewRow indexing error. When [#_continueOnError] is false the exception is stashed so
  /// [#index] can publish the completed row (avoiding inverted-index holes) and then rethrow.
  private void recordOrDeferIndexingError(String indexType, Exception exception) {
    recordIndexingError(indexType, exception);
    if (!_continueOnError && _pendingRowIndexingException == null) {
      _pendingRowIndexingException = exception;
    }
  }

  private void throwPendingRowIndexingExceptionIfStrict() {
    Exception pending = _pendingRowIndexingException;
    _pendingRowIndexingException = null;
    if (pending != null) {
      throw wrapIndexingException("ROW", pending);
    }
  }

  private static RuntimeException wrapIndexingException(String indexType, Exception exception) {
    if (exception instanceof RuntimeException runtimeException) {
      return runtimeException;
    }
    return new RuntimeException("Failed to index value with " + indexType, exception);
  }

  private void recordIndexingError(IndexType<?, ?, ?> indexType, Exception exception) {
    _logger.error("failed to index value with {}", indexType, exception);
    if (_serverMetrics != null) {
      String indexMetricName = indexType.getPrettyName().toUpperCase(Locale.US);
      String metricKeyName = _realtimeTableName + "-" + indexMetricName + "-indexingError";
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

  private void recordIndexingError(String indexType, Exception exception) {
    _logger.error("failed to index value with {}", indexType, exception);
    if (_serverMetrics != null) {
      String metricKeyName = _realtimeTableName + "-" + indexType + "-indexingError";
      _serverMetrics.addMeteredTableValue(metricKeyName, ServerMeter.INDEXING_FAILURES, 1);
    }
  }

  private void recordIncompleteRow() {
    if (_serverMetrics != null) {
      _serverMetrics.addMeteredTableValue(_realtimeTableName, ServerMeter.INCOMPLETE_REALTIME_ROWS_CONSUMED, 1);
    }
  }

  private void aggregateMetrics(GenericRow row, int docId) {
    for (MetricFieldSpec metricFieldSpec : _physicalMetricFieldSpecs) {
      IndexContainer indexContainer = _indexContainerMap.get(metricFieldSpec.getName());
      ValueAggregator valueAggregator = indexContainer._valueAggregator;
      String sourceColumn = indexContainer._sourceColumn;
      // NOTE: value can be null if the column is not specified in the schema.
      Object value = row.getValue(sourceColumn);
      // Skip aggregation if the input value is null.
      if (value == null) {
        // Handle COUNT(*)
        if (sourceColumn.equals(AggregationFunctionColumnPair.STAR)) {
          assert valueAggregator.getAggregationType() == AggregationFunctionType.COUNT;
          value = 1;
        } else {
          continue;
        }
      }
      MutableForwardIndex forwardIndex =
          (MutableForwardIndex) indexContainer._mutableIndexes.get(StandardIndexes.forward());
      DataType dataType = metricFieldSpec.getDataType();

      switch (valueAggregator.getAggregatedValueType()) {
        case DOUBLE:
          double oldDoubleValue;
          double newDoubleValue;
          switch (dataType) {
            case INT:
              oldDoubleValue = forwardIndex.getInt(docId);
              newDoubleValue = (double) valueAggregator.applyRawValue(oldDoubleValue, value);
              forwardIndex.setInt(docId, (int) newDoubleValue);
              break;
            case LONG:
              oldDoubleValue = forwardIndex.getLong(docId);
              newDoubleValue = (double) valueAggregator.applyRawValue(oldDoubleValue, value);
              forwardIndex.setLong(docId, (long) newDoubleValue);
              break;
            case FLOAT:
              oldDoubleValue = forwardIndex.getFloat(docId);
              newDoubleValue = (double) valueAggregator.applyRawValue(oldDoubleValue, value);
              forwardIndex.setFloat(docId, (float) newDoubleValue);
              break;
            case DOUBLE:
              oldDoubleValue = forwardIndex.getDouble(docId);
              newDoubleValue = (double) valueAggregator.applyRawValue(oldDoubleValue, value);
              forwardIndex.setDouble(docId, newDoubleValue);
              break;
            default:
              throw new UnsupportedOperationException(String.format("Aggregation type %s of %s not supported for %s",
                  valueAggregator.getAggregatedValueType(), valueAggregator.getAggregationType(), dataType));
          }
          break;
        case LONG:
          long oldLongValue;
          long newLongValue;
          switch (dataType) {
            case INT:
              oldLongValue = forwardIndex.getInt(docId);
              newLongValue = (long) valueAggregator.applyRawValue(oldLongValue, value);
              forwardIndex.setInt(docId, (int) newLongValue);
              break;
            case LONG:
              oldLongValue = forwardIndex.getLong(docId);
              newLongValue = (long) valueAggregator.applyRawValue(oldLongValue, value);
              forwardIndex.setLong(docId, newLongValue);
              break;
            case FLOAT:
              oldLongValue = (long) forwardIndex.getFloat(docId);
              newLongValue = (long) valueAggregator.applyRawValue(oldLongValue, value);
              forwardIndex.setFloat(docId, (float) newLongValue);
              break;
            case DOUBLE:
              oldLongValue = (long) forwardIndex.getDouble(docId);
              newLongValue = (long) valueAggregator.applyRawValue(oldLongValue, value);
              forwardIndex.setDouble(docId, (double) newLongValue);
              break;
            default:
              throw new UnsupportedOperationException(String.format("Aggregation type %s of %s not supported for %s",
                  valueAggregator.getAggregatedValueType(), valueAggregator.getAggregationType(), dataType));
          }
          break;
        case BYTES:
          Object oldValue = valueAggregator.deserializeAggregatedValue(forwardIndex.getBytes(docId));
          Object newValue = valueAggregator.applyRawValue(oldValue, value);
          forwardIndex.setBytes(docId, valueAggregator.serializeAggregatedValue(newValue));
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
  public File getConsumerDir() {
    return _consumerDir;
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

  @Nullable
  @Override
  public DataSource getDataSourceNullable(String column) {
    IndexContainer indexContainer = _indexContainerMap.get(column);
    if (indexContainer != null) {
      // Physical column
      return indexContainer.toDataSource();
    }
    FieldSpec fieldSpec = _schema.getFieldSpecFor(column);
    if (fieldSpec != null && fieldSpec.isVirtualColumn()) {
      // Virtual column
      VirtualColumnContext virtualColumnContext =
          new VirtualColumnContext(fieldSpec, _numDocsIndexed, _segmentMetadata);
      return VirtualColumnProviderFactory.buildProvider(virtualColumnContext).buildDataSource(virtualColumnContext);
    }
    return null;
  }

  @Override
  public DataSource getDataSource(String column, Schema schema) {
    DataSource dataSource = getDataSourceNullable(column);
    if (dataSource != null) {
      return dataSource;
    }
    FieldSpec fieldSpec = schema.getFieldSpecFor(column);
    Preconditions.checkState(fieldSpec != null, "Failed to find column: %s in schema: %s", column,
        schema.getSchemaName());
    return IndexSegmentUtils.createVirtualDataSource(
        new VirtualColumnContext(fieldSpec, _numDocsIndexed, _segmentMetadata));
  }

  @Nullable
  @Override
  public List<StarTreeV2> getStarTrees() {
    return null;
  }

  @Nullable
  @Override
  public TextIndexReader getMultiColumnTextIndex() {
    return _multiColumnTextIndex;
  }

  @Nullable
  @Override
  public ThreadSafeMutableRoaringBitmap getValidDocIds() {
    return _validDocIds;
  }

  @Nullable
  public String getDeleteRecordColumn() {
    return _deleteRecordColumn;
  }

  @Nullable
  @Override
  public ThreadSafeMutableRoaringBitmap getQueryableDocIds() {
    return _queryableDocIds;
  }

  @Override
  public boolean hasNoQueryableDocs() {
    if (_partitionUpsertMetadataManager == null) {
      return false;
    }
    UpsertViewManager viewManager = _partitionUpsertMetadataManager.getUpsertViewManager();
    if (viewManager != null) {
      MutableRoaringBitmap queryableDocIdsSnapshot = viewManager.getQueryableDocIdsSnapshot(this);
      if (queryableDocIdsSnapshot != null) {
        return queryableDocIdsSnapshot.isEmpty();
      }
      return false;
    }
    ThreadSafeMutableRoaringBitmap queryableDocIds = getQueryableDocIds();
    if (queryableDocIds != null) {
      return queryableDocIds.isEmpty();
    }
    ThreadSafeMutableRoaringBitmap validDocIds = getValidDocIds();
    return validDocIds != null && validDocIds.isEmpty();
  }

  @Override
  public boolean hasNoValidDocs() {
    return UpsertUtils.hasNoValidDocs(_partitionUpsertMetadataManager, this);
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

  /// Calls commit() on all mutable indexes. This is used in preparation for realtime segment conversion.
  /// .commit() can be implemented per index to perform any required actions before using mutable segment
  /// artifacts to optimize immutable segment build.
  public void commit() {
    for (IndexContainer indexContainer : _indexContainerMap.values()) {
      for (MutableIndex mutableIndex : indexContainer._mutableIndexes.values()) {
        mutableIndex.commit();
      }
    }

    if (_multiColumnTextIndex != null) {
      _multiColumnTextIndex.commit();
    }
  }

  /// Returns the per-column mutable OPEN_STRUCT index, or `null` if the column is not OPEN_STRUCT
  /// or the index has not been initialized.
  @Nullable
  public MutableOpenStructIndex getOpenStructIndex(String column) {
    IndexContainer container = _indexContainerMap.get(column);
    if (container == null) {
      return null;
    }
    MutableIndex index = container._mutableIndexes.get(StandardIndexes.openStruct());
    return index instanceof MutableOpenStructIndex ? (MutableOpenStructIndex) index : null;
  }

  @Override
  public void offload() {
    if (_partitionUpsertMetadataManager != null) {
      _partitionUpsertMetadataManager.removeSegment(this);
    }
    if (_partitionDedupMetadataManager != null) {
      _partitionDedupMetadataManager.removeSegment(this);
    }
  }

  @Override
  public void destroy() {
    _logger.info("Trying to close RealtimeSegmentImpl : {}", _segmentName);
    if (_partitionUpsertMetadataManager != null) {
      _partitionUpsertMetadataManager.untrackSegmentForUpsertView(this);
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
          // Skip stat collection for SameValueMutableDictionary
          if (entry.getValue()._dictionary instanceof BaseOffHeapMutableDictionary) {
            BaseOffHeapMutableDictionary dictionary = (BaseOffHeapMutableDictionary) entry.getValue()._dictionary;
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

    // Close the indexes
    for (IndexContainer indexContainer : _indexContainerMap.values()) {
      indexContainer.close();
    }
    _indexContainerMap.clear();

    if (_multiColumnTextIndex != null) {
      try {
        _multiColumnTextIndex.close();
      } catch (Exception e) {
        _logger.error("Caught exception while closing multi-column text index for column: {}, continuing with error",
            _multiColumnTextMetadata.getColumns(), e);
      }
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

  /// Returns the docIds to use for iteration when the data is sorted by the given column.
  /// Called only by realtime record reader.
  ///
  /// When the column has a dictionary and an inverted index (the common case for sorted columns), delegates to
  /// [#getSortedDocIdsWithInvertedIndex]. When the column is configured as no-dictionary (raw forward index),
  /// delegates to [#getSortedDocIdsWithRawForwardIndex].
  ///
  /// @param column The column to use for sorting
  /// @return The docIds to use for iteration
  public int[] getSortedDocIdIterationOrderWithSortedColumn(String column) {
    IndexContainer indexContainer = _indexContainerMap.get(column);
    if (indexContainer._dictionary != null) {
      return getSortedDocIdsWithInvertedIndex(indexContainer);
    } else {
      return getSortedDocIdsWithRawForwardIndex(column, indexContainer);
    }
  }

  /// Returns sorted docIds for a dictionary-encoded sorted column by sorting dictionary ids and re-ordering documents
  /// via the inverted index bitmaps.
  private int[] getSortedDocIdsWithInvertedIndex(IndexContainer indexContainer) {
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
    MutableInvertedIndex invertedIndex =
        ((MutableInvertedIndex) indexContainer._mutableIndexes.get(StandardIndexes.inverted()));
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

  /// Returns sorted docIds for a no-dictionary (raw) sorted column by reading raw values directly from the forward
  /// index and sorting by them.
  private int[] getSortedDocIdsWithRawForwardIndex(String column, IndexContainer indexContainer) {
    MutableForwardIndex forwardIndex =
        (MutableForwardIndex) indexContainer._mutableIndexes.get(StandardIndexes.forward());
    int numDocsIndexed = _numDocsIndexed;
    int[] docIds = new int[numDocsIndexed];
    for (int i = 0; i < numDocsIndexed; i++) {
      docIds[i] = i;
    }

    DataType dataType = indexContainer._fieldSpec.getDataType();
    DataType storedType = dataType.getStoredType();
    switch (storedType) {
      case INT:
        IntArrays.quickSort(docIds, (d1, d2) -> Integer.compare(forwardIndex.getInt(d1), forwardIndex.getInt(d2)));
        break;
      case LONG:
        IntArrays.quickSort(docIds, (d1, d2) -> Long.compare(forwardIndex.getLong(d1), forwardIndex.getLong(d2)));
        break;
      case FLOAT:
        IntArrays.quickSort(docIds, (d1, d2) -> Float.compare(forwardIndex.getFloat(d1), forwardIndex.getFloat(d2)));
        break;
      case DOUBLE:
        IntArrays.quickSort(docIds, (d1, d2) -> Double.compare(forwardIndex.getDouble(d1), forwardIndex.getDouble(d2)));
        break;
      case BIG_DECIMAL:
        IntArrays.quickSort(docIds,
            (d1, d2) -> forwardIndex.getBigDecimal(d1).compareTo(forwardIndex.getBigDecimal(d2)));
        break;
      case STRING:
        IntArrays.quickSort(docIds, (d1, d2) -> forwardIndex.getString(d1).compareTo(forwardIndex.getString(d2)));
        break;
      case BYTES:
        if (dataType == DataType.UUID) {
          IntArrays.quickSort(docIds,
              (d1, d2) -> UuidUtils.compare(forwardIndex.getBytes(d1), forwardIndex.getBytes(d2)));
        } else {
          IntArrays.quickSort(docIds,
              (d1, d2) -> ByteArray.compare(forwardIndex.getBytes(d1), forwardIndex.getBytes(d2)));
        }
        break;
      default:
        throw new UnsupportedOperationException(
            "Unsupported stored type: " + storedType + " for no-dictionary sorted column: " + column);
    }

    return docIds;
  }

  /// Helper function that returns docId, depends on the following scenarios.
  ///
  /// - If metrics aggregation is enabled and if the dimension values were already seen, return existing docIds
  /// - Else, this function will create and return a new docId.
  private int getOrCreateDocId() {
    if (!isAggregateMetricsEnabled()) {
      return _numDocsIndexed;
    }

    int i = 0;
    // Dimension and time columns form the aggregation key. They are always dictionary encoded in the consuming
    // segment (isNoDictionaryColumn forces a dictionary on them when aggregation is enabled), so the _dictId read
    // below is always valid. Keep this set of columns in sync with the field types forced there.
    // Multi-value dimensions cannot be rollup keys (#3867): enableMetricsAggregationIfPossible and
    // TableConfigUtils.validateMetricsAggregation disable/reject aggregation when any dimension is multi-value, so
    // this loop only runs with single-value dimensions (see testMultiValueDimensionDisablesAggregation).
    int[] dictIds = new int[_numKeyColumns]; // dimensions + date time columns + time column.

    for (FieldSpec fieldSpec : _physicalDimensionFieldSpecs) {
      dictIds[i++] = _indexContainerMap.get(fieldSpec.getName())._dictId;
    }
    for (String timeColumnName : _physicalTimeColumnNames) {
      dictIds[i++] = _indexContainerMap.get(timeColumnName)._dictId;
    }
    return _recordIdMap.put(new FixedIntArray(dictIds));
  }

  /// Enables and initializes metrics aggregation for the consuming segment when configured and feasible.
  ///
  /// Aggregation is enabled when all of the following hold:
  /// - The `aggregateMetrics` flag or ingestion `aggregationConfigs` is specified.
  /// - No metric column is dictionary encoded. Aggregated values are mutated in place in the raw forward index, so
  ///   metrics must stay no-dictionary.
  /// - All metric and dimension columns are single-valued (see https://github.com/apache/pinot/issues/3867).
  ///
  /// Dimension and time columns form the aggregation key via their dictionary ids (see [#getOrCreateDocId]), so they
  /// must be dictionary encoded. This is not required from the caller: [#isNoDictionaryColumn] forces a dictionary on
  /// those columns in the consuming segment whenever aggregation is enabled, even when the table config marks them as
  /// no-dictionary. The committed segment is rebuilt from the table config, so the no-dictionary setting is still
  /// honored there.
  ///
  /// Returns the map from dictionary id array to doc id, or `null` if metrics aggregation cannot be enabled.
  private IdMap<FixedIntArray> enableMetricsAggregationIfPossible(RealtimeSegmentConfig config) {
    Set<String> noDictionaryColumns =
        FieldIndexConfigsUtil.columnsWithIndexDisabled(StandardIndexes.dictionary(), config.getIndexConfigByCol());
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

    // All dimension columns must be single value. No-dictionary dimensions are supported: isNoDictionaryColumn()
    // forces a dictionary on them in the consuming segment so they can be used as the aggregation key.
    for (FieldSpec fieldSpec : _physicalDimensionFieldSpecs) {
      if (!fieldSpec.isSingleValueField()) {
        _logger.warn("Metrics aggregation cannot be turned ON in presence of multi-value dimension columns, eg: {}",
            fieldSpec.getName());
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

  public boolean canAddMore() {
    return !_indexCapacityThresholdBreached;
  }

  /// Returns `true` when any column has re-use mutable text index enabled.
  public boolean hasColumnWithReuseMutableTextIndex() {
    return _hasColumnWithReuseMutableTextIndex;
  }

  // NOTE: Okay for single-writer
  @SuppressWarnings("NonAtomicOperationOnVolatileField")
  private static class ValuesInfo {
    volatile int _numValues = 0;
    volatile int _maxNumValuesPerMVEntry = -1;
    volatile int _varByteMVMaxRowLengthInBytes = -1;

    void updateSVNumValues() {
      _numValues++;
    }

    void updateMVNumValues(int numValuesInMVEntry) {
      _numValues += numValuesInMVEntry;
      _maxNumValuesPerMVEntry = Math.max(_maxNumValuesPerMVEntry, numValuesInMVEntry);
    }

    /// When an MV VarByte column is created with noDict, the realtime segment is still created with a dictionary.
    /// When the realtime segment is converted to offline segment, the offline segment creates a noDict column.
    /// MultiValueVarByteRawIndexCreator requires the maxRowLengthInBytes. Refer to OSS issue
    /// https://github.com/apache/pinot/issues/10127 for more details.
    void updateVarByteMVMaxRowLengthInBytes(Object entry, DataType dataType) {
      // MV support for BigDecimal is not available.
      if (dataType != STRING && dataType != BYTES) {
        return;
      }

      Object[] values = (Object[]) entry;
      int rowLength = 0;

      switch (dataType) {
        case STRING: {
          for (Object value : values) {
            rowLength += Utf8.encodedLength((String) value);
          }

          _varByteMVMaxRowLengthInBytes = Math.max(_varByteMVMaxRowLengthInBytes, rowLength);
          break;
        }
        case BYTES: {
          for (Object value : values) {
            rowLength += ((byte[]) value).length;
          }

          _varByteMVMaxRowLengthInBytes = Math.max(_varByteMVMaxRowLengthInBytes, rowLength);
          break;
        }
        default:
          throw new IllegalStateException("Invalid type=" + dataType);
      }
    }
  }

  private class IndexContainer implements Closeable {
    final FieldSpec _fieldSpec;
    final PartitionFunction _partitionFunction;
    final Set<Integer> _partitions;
    final ValuesInfo _valuesInfo;
    final MutableDictionary _dictionary;
    final MutableNullValueVector _nullValueVector;
    final Map<IndexType, MutableIndex> _mutableIndexes;
    final String _sourceColumn;
    final ValueAggregator _valueAggregator;

    volatile Comparable _minValue;
    volatile Comparable _maxValue;

    /// The dictionary id for the latest single-value record.
    /// It is set on [#updateDictionary(GenericRow)] and read in [#addNewRow(int, GenericRow)]
    int _dictId = Integer.MIN_VALUE;
    /// The dictionary ids for the latest multi-value record.
    /// It is set on [#updateDictionary(GenericRow)] and read in [#addNewRow(int, GenericRow)]
    int[] _dictIds;

    IndexContainer(FieldSpec fieldSpec, @Nullable PartitionFunction partitionFunction,
        @Nullable Set<Integer> partitions, ValuesInfo valuesInfo, Map<IndexType, MutableIndex> mutableIndexes,
        @Nullable MutableDictionary dictionary, @Nullable MutableNullValueVector nullValueVector,
        @Nullable String sourceColumn, @Nullable ValueAggregator valueAggregator) {
      Preconditions.checkArgument(
          mutableIndexes.containsKey(StandardIndexes.forward())
              || mutableIndexes.containsKey(StandardIndexes.openStruct()),
          "Forward index or OPEN_STRUCT index is required");
      _fieldSpec = fieldSpec;
      _mutableIndexes = mutableIndexes;
      _dictionary = dictionary;
      _nullValueVector = nullValueVector;
      _partitionFunction = partitionFunction;
      _partitions = partitions;
      _valuesInfo = valuesInfo;
      _sourceColumn = sourceColumn;
      _valueAggregator = valueAggregator;
    }

    DataSource toDataSource() {
      if (_fieldSpec.getDataType() == DataType.OPEN_STRUCT) {
        MutableIndex idx = _mutableIndexes.get(StandardIndexes.openStruct());
        return new MutableOpenStructDataSource((ComplexFieldSpec) _fieldSpec, (MutableOpenStructIndex) idx,
            _numDocsIndexed);
      }
      if (_fieldSpec.getDataType() == MAP) {
        return new MutableMapDataSource(_fieldSpec, _numDocsIndexed, _valuesInfo._numValues,
            _valuesInfo._maxNumValuesPerMVEntry, _dictionary == null ? -1 : _dictionary.length(), _partitionFunction,
            _partitions, _minValue, _maxValue, _mutableIndexes, _dictionary, _nullValueVector,
            _valuesInfo._varByteMVMaxRowLengthInBytes);
      }
      MultiColumnTextIndexReader multiColTextReader;
      if (_multiColumnTextMetadata != null && _multiColumnTextMetadata.getColumns().contains(_fieldSpec.getName())) {
        multiColTextReader = _multiColumnTextIndex;
      } else {
        multiColTextReader = null;
      }

      return new MutableDataSource(_fieldSpec, _numDocsIndexed, _valuesInfo._numValues,
          _valuesInfo._maxNumValuesPerMVEntry, _dictionary == null ? -1 : _dictionary.length(), _partitionFunction,
          _partitions, _minValue, _maxValue, _mutableIndexes, _dictionary, _nullValueVector,
          _valuesInfo._varByteMVMaxRowLengthInBytes, multiColTextReader);
    }

    @Override
    public void close() {
      String column = _fieldSpec.getName();

      BiConsumer<IndexType<?, ?, ?>, AutoCloseable> closer = (indexType, closeable) -> {
        try {
          if (closeable != null) {
            closeable.close();
          }
        } catch (Exception e) {
          _logger.error("Caught exception while closing {} index for column: {}, continuing with error", indexType,
              column, e);
        }
      };

      _mutableIndexes.forEach(closer::accept);
      closer.accept(StandardIndexes.dictionary(), _dictionary);
      closer.accept(StandardIndexes.nullValueVector(), _nullValueVector);
    }
  }

  private static final class MutableIndexes extends HashMap<IndexType, MutableIndex>
      implements VectorIndexConfigProvider {
    @Nullable
    private final VectorIndexConfig _vectorIndexConfig;

    private MutableIndexes(@Nullable VectorIndexConfig vectorIndexConfig) {
      _vectorIndexConfig = vectorIndexConfig != null && vectorIndexConfig.isEnabled() ? vectorIndexConfig : null;
    }

    @Nullable
    @Override
    public VectorIndexConfig getVectorIndexConfig() {
      return _vectorIndexConfig;
    }
  }
}
