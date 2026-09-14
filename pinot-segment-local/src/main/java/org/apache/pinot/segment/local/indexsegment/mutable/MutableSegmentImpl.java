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
  /// propagate after the row is completed. When true, the row is repaired with defaults and consumption continues
  /// only if that repair succeeded.
  private final boolean _continueOnError;
  /// First indexing exception stashed so the row can be published before a strict rethrow.
  /// Only accessed on the consuming thread that calls [#index].
  @Nullable
  private Exception _pendingRowIndexingException;
  /// Set when a started row cannot be completed or repaired. Further [#index] calls are rejected so the hole
  /// cannot be preserved by appending at the next document ID (issue #16316).
  private volatile boolean _unrecoverableIndexingFailure;
  /// Test-only interceptor invoked immediately before each mutable-index write, including fallback writes.
  @Nullable
  IndexWriteInterceptor _indexWriteInterceptor;
  /// Reused on the single consuming thread. Reset at the start of each [#index] call.
  private final RowWriteState _rowWriteState = new RowWriteState();
  private final File _consumerDir;

  private final Map<String, IndexContainer> _indexContainerMap = new HashMap<>();
  private final MultiValueLimit[] _multiValueLimits;
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
  private final boolean _isPartialUpsert;
  private final List<String> _upsertComparisonColumns;
  private final String _deleteRecordColumn;
  private final boolean _upsertDropOutOfOrderRecord;
  private final String _upsertOutOfOrderRecordColumn;
  private final UpsertConfig.ConsistencyMode _upsertConsistencyMode;

  // The valid doc ids are maintained locally instead of in the upsert metadata manager because:
  // 1. There is only one consuming segment per partition, the committed segments do not need to modify the valid doc
  //    ids for the consuming segment.
  // 2. During the segment commitment, when loading the immutable version of this segment, in order to keep the result
  //    correct, the valid doc ids should not be changed, only the record location should be changed.
  // FIXME: There is a corner case for this approach which could cause inconsistency. When there is segment load during
  //        consumption with newer timestamp (late event in consuming segment), the record location will be updated, but
  //        the valid doc ids won't be updated.
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
    List<MultiValueLimit> multiValueLimits = new ArrayList<>();
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
      VectorIndexConfig vectorIndexConfig = indexConfigs.getConfig(StandardIndexes.vector());
      boolean isDictionary = !isNoDictionaryColumn(indexConfigs, fieldSpec, column);
      MutableIndexContext.Builder contextBuilder = MutableIndexContext.builder()
          .withFieldSpec(fieldSpec)
          .withMemoryManager(_memoryManager)
          .withDictionary(isDictionary)
          .withCapacity(_capacity)
          .offHeap(_offHeap)
          .withSegmentName(_segmentName)
          .withEstimatedCardinality(_statsHistory.getEstimatedCardinality(column))
          .withEstimatedColSize(_statsHistory.getEstimatedAvgColSize(column))
          .withAvgNumMultiValues(config.getAvgNumMultiValues())
          .withConsumerDir(_consumerDir)
          .withFixedLengthBytes(fixedByteSize);
      if (vectorIndexConfig.isEnabled()) {
        // A vector column holds one value per dimension, which may exceed the default cap
        contextBuilder.withMaxNumMultiValues(vectorIndexConfig.getVectorDimension());
      }
      MutableIndexContext context = contextBuilder.build();

      if (!fieldSpec.isSingleValueField()) {
        multiValueLimits.add(new MultiValueLimit(column, context.getMaxNumMultiValues()));
      }

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
          new MutableIndexes(vectorIndexConfig);
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
          MutableOpenStructIndex openStructIndex = new MutableOpenStructIndex(column, _realtimeTableName,
              (ComplexFieldSpec) fieldSpec,
              (OpenStructIndexConfig) openStructConfig, _memoryManager, _capacity);
          mutableIndexes.put(StandardIndexes.openStruct(), openStructIndex);
        }
      }

      _indexContainerMap.put(column,
          new IndexContainer(fieldSpec, partitionFunction, partitions, new ValuesInfo(), mutableIndexes, dictionary,
              nullValueVector, sourceColumn, valueAggregator));
    }
    _hasColumnWithReuseMutableTextIndex = hasColumnWithReuseMutableTextIndex;
    _multiValueLimits = multiValueLimits.toArray(new MultiValueLimit[0]);

    _partitionDedupMetadataManager = config.getPartitionDedupMetadataManager();
    _dedupTimeColumn =
        _partitionDedupMetadataManager != null ? _partitionDedupMetadataManager.getContext().getDedupTimeColumn()
            : null;

    _partitionUpsertMetadataManager = config.getPartitionUpsertMetadataManager();
    if (_partitionUpsertMetadataManager != null) {
      Preconditions.checkState(!isAggregateMetricsEnabled(),
          "Metrics aggregation and upsert cannot be enabled together");
      UpsertContext upsertContext = _partitionUpsertMetadataManager.getContext();
      _isPartialUpsert = upsertContext.getUpsertMode() == UpsertConfig.Mode.PARTIAL;
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
      _isPartialUpsert = false;
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
    if (_unrecoverableIndexingFailure) {
      throw new IllegalStateException(
          "Mutable segment " + _segmentName + " is terminal after an unrecoverable indexing failure");
    }
    _pendingRowIndexingException = null;
    _rowWriteState.reset();
    IndexContainer mismatchedPartitionIndexContainer = null;
    String mismatchedPartitionValue = null;
    int mismatchedPartition = -1;
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
          return canAddMore();
        }
        mismatchedPartitionIndexContainer = indexContainer;
        mismatchedPartitionValue = stringValue;
        mismatchedPartition = partition;
      }
    }

    int numDocsIndexed = _numDocsIndexed;
    if (isUpsertEnabled()) {
      // Validate the incoming row before partial-upsert strategies can copy or expand oversized MV values.
      validateNumMultiValues(row);
      RecordInfo recordInfo = getRecordInfo(row, numDocsIndexed);
      GenericRow updatedRow = _partitionUpsertMetadataManager.updateRecord(row, recordInfo);
      if (_isPartialUpsert) {
        // Strategies such as APPEND and UNION can produce a merged row that is larger than the incoming row.
        validateNumMultiValues(updatedRow);
      }
      trackMismatchedPartition(mismatchedPartitionIndexContainer, mismatchedPartition, mismatchedPartitionValue);

      boolean canTakeMore;
      // NOTE: out-of-order records can not be dropped or marked when consistent upsert view is enabled.
      // Peek first so drop/mark can skip the write without mutating validDocIds. Then finish the physical row
      // before addRecord so metadata never points at a hole (issue #16316).
      if (!_partitionUpsertMetadataManager.isAcceptingRecords()) {
        updateIndexedAndIngestionTime(metadata);
        return canAddMore();
      }
      if (_upsertConsistencyMode == UpsertConfig.ConsistencyMode.NONE) {
        boolean isOutOfOrderRecord = _partitionUpsertMetadataManager.isOutOfOrderRecord(recordInfo);
        if (_upsertOutOfOrderRecordColumn != null) {
          updatedRow.putValue(_upsertOutOfOrderRecordColumn, BooleanUtils.toInt(isOutOfOrderRecord));
        }
        if (isOutOfOrderRecord && _upsertDropOutOfOrderRecord) {
          // addRecord on an OOO key only meters UPSERT_OUT_OF_ORDER; it does not move validDocIds.
          _partitionUpsertMetadataManager.addRecord(this, recordInfo);
          updateIndexedAndIngestionTime(metadata);
          return canAddMore();
        }
      }
      indexPhysicalRow(numDocsIndexed, updatedRow);
      numDocsIndexed++;
      canTakeMore = numDocsIndexed < _capacity;
      _numDocsIndexed = numDocsIndexed;
      // Index the record and update _numDocsIndexed counter before updating the upsert metadata so that the record
      // becomes queryable before validDocIds bitmaps are updated. This order is important for consistent upsert view,
      // otherwise the latest doc can be missed by query due to 'docId < _numDocs' check in query filter operators.
      _partitionUpsertMetadataManager.addRecord(this, recordInfo);
      throwPendingRowIndexingExceptionIfStrict();
      updateIndexedAndIngestionTime(metadata);
      return canTakeMore && canAddMore();
    }

    // Validate before dedup or partition tracking so a rejected row cannot leave metadata state behind.
    validateNumMultiValues(row);
    trackMismatchedPartition(mismatchedPartitionIndexContainer, mismatchedPartition, mismatchedPartitionValue);

    DedupRecordInfo pendingDedupRecordInfo = null;
    if (isDedupEnabled()) {
      DedupRecordInfo dedupRecordInfo = getDedupRecordInfo(row);
      if (_partitionDedupMetadataManager.isRecordPresent(dedupRecordInfo)) {
        if (_serverMetrics != null) {
          _serverMetrics.addMeteredTableValue(_realtimeTableName, ServerMeter.REALTIME_DEDUP_DROPPED, 1);
        }
        updateIndexedAndIngestionTime(metadata);
        return canAddMore();
      }
      pendingDedupRecordInfo = dedupRecordInfo;
    }

    // Dictionary ids are prepared before the rollup key is computed and before any forward/secondary write.
    RowWriteState state = _rowWriteState;
    boolean dictHadError = updateDictionary(row, state);
    if (state._repairFailed) {
      markUnrecoverable(state);
      throwPendingRowIndexingExceptionIfStrict();
      throw new IllegalStateException(
          "Mutable segment " + _segmentName + " is terminal after an unrecoverable indexing failure");
    }
    int docId = getOrCreateDocId();

    boolean canTakeMore;
    if (docId == numDocsIndexed) {
      // New row: complete every column (or repair) before publishing _numDocsIndexed.
      boolean rowHadError = addNewRow(numDocsIndexed, row, state);
      if (dictHadError || rowHadError) {
        recordIncompleteRow();
      }
      if (_unrecoverableIndexingFailure) {
        throwPendingRowIndexingExceptionIfStrict();
        throw new IllegalStateException(
            "Mutable segment " + _segmentName + " is terminal after an unrecoverable indexing failure");
      }
      canTakeMore = numDocsIndexed++ < _capacity;
    } else {
      assert isAggregateMetricsEnabled();
      try {
        aggregateMetrics(row, docId);
        if (dictHadError) {
          recordIncompleteRow();
        }
      } catch (Exception e) {
        recordIncompleteRow();
        if (_unrecoverableIndexingFailure) {
          throw wrapIndexingException("AGGREGATE_METRICS", e);
        }
        recordOrThrowIndexingError("AGGREGATE_METRICS", e);
      }
      canTakeMore = true;
    }
    _numDocsIndexed = numDocsIndexed;
    if (pendingDedupRecordInfo != null) {
      // Claim the key only after the doc is published so an unpublished failure cannot drop a later retry.
      // Consume is single-writer per partition; a lost race with another consuming segment is not rolled back.
      _partitionDedupMetadataManager.checkRecordPresentOrUpdate(pendingDedupRecordInfo, this);
    }
    throwPendingRowIndexingExceptionIfStrict();

    updateIndexedAndIngestionTime(metadata);
    return canTakeMore && canAddMore();
  }

  private void trackMismatchedPartition(@Nullable IndexContainer indexContainer, int partition,
      @Nullable String partitionValue) {
    if (indexContainer != null && indexContainer._partitions.add(partition)) {
      // for every partition other than mainPartitionId, log a warning once
      _logger.warn("Found new partition: {} from partition column: {}, value: {}", partition, _partitionColumn,
          partitionValue);
    }
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

  /// Validates that no multi-value column in the row holds more values than its forward index can store in a single
  /// multi-value entry. Must run before any column of the row is indexed so that a rejected row leaves no partial
  /// state behind.
  ///
  /// @throws IllegalStateException if a multi-value column exceeds its maximum number of values
  private void validateNumMultiValues(GenericRow row) {
    for (MultiValueLimit limit : _multiValueLimits) {
      Object value = row.getValue(limit.column());
      if (value != null) {
        int numValues = ((Object[]) value).length;
        if (numValues > limit.maxNumMultiValues()) {
          throw new IllegalStateException(
              String.format("Number of values: %d in MV column: %s exceeds the maximum allowed: %d", numValues,
                  limit.column(), limit.maxNumMultiValues()));
        }
      }
    }
  }

  /// Runs dictionary + forward/secondary indexing for a new docId and meters an incomplete row when either step had
  /// to fall back to defaults (issue #16316).
  private void indexPhysicalRow(int docId, GenericRow row) {
    RowWriteState state = _rowWriteState;
    boolean dictHadError = updateDictionary(row, state);
    boolean rowHadError = addNewRow(docId, row, state);
    if (dictHadError || rowHadError) {
      recordIncompleteRow();
    }
    if (state._repairFailed) {
      markUnrecoverable(state);
      throwPendingRowIndexingExceptionIfStrict();
      throw new IllegalStateException(
          "Mutable segment " + _segmentName + " is terminal after an unrecoverable indexing failure");
    }
  }

  /// @return {@code true} if any column required a default/fallback while updating dictionaries.
  /// When `continueOnError` is false, dictionary failures are stashed and rethrown after the row is published.
  private boolean updateDictionary(GenericRow row, RowWriteState state) {
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
          recordIndexingError("DICTIONARY");
          hadError = true;
          state.markIncomplete();
          value = getDefaultNullValueForIndexing(indexContainer._fieldSpec);
          row.putDefaultNullValue(column, value);
        }
        if (indexContainer._fieldSpec.isSingleValueField()) {
          indexContainer._dictId = dictionary.index(value);
        } else {
          indexContainer._dictIds = dictionary.index((Object[]) value);
        }
        indexContainer._minValue = dictionary.getMinVal();
        indexContainer._maxValue = dictionary.getMaxVal();
      } catch (Exception e) {
        recordOrDeferIndexingError("DICTIONARY", e);
        hadError = true;
        state.noteError(e);
        indexContainer._dictId = Integer.MIN_VALUE;
        indexContainer._dictIds = null;
        try {
          // Index the field default instead of leaving the sentinel: with metrics aggregation the rollup key is built
          // from the dict ids (see getOrCreateDocId), and Integer.MIN_VALUE is not a real dict id.
          Object defaultValue = getDefaultNullValueForIndexing(indexContainer._fieldSpec);
          if (indexContainer._fieldSpec.isSingleValueField()) {
            indexContainer._dictId = dictionary.index(defaultValue);
          } else {
            indexContainer._dictIds = dictionary.index((Object[]) defaultValue);
          }
          row.putDefaultNullValue(column, defaultValue);
          indexContainer._minValue = dictionary.getMinVal();
          indexContainer._maxValue = dictionary.getMaxVal();
        } catch (Exception fallbackError) {
          _logger.error("Failed to index default null value into dictionary for column: {}", column, fallbackError);
          state.noteRepairFailure(fallbackError);
        }
      }
      updateIndexCapacityThresholdBreached(dictionary, column);
    }
    return hadError;
  }

  /// Indexes a new physical row. Always completes the row so seal/query lengths stay aligned with [_numDocsIndexed]
  /// (issue #16316). Forward indexes are written first from a finalized value; secondary indexes then consume that
  /// same value. On unrecoverable repair the segment is marked terminal.
  ///
  /// @return {@code true} if any column required a default/fallback while indexing
  private boolean addNewRow(int docId, GenericRow row, RowWriteState state) {
    boolean rowHadError = false;
    for (Map.Entry<String, IndexContainer> entry : _indexContainerMap.entrySet()) {
      String column = entry.getKey();
      IndexContainer indexContainer = entry.getValue();
      try {
        if (indexContainer._valueAggregator != null) {
          if (!addAggregatedColumn(docId, row, column, indexContainer, state)) {
            rowHadError = true;
          }
        } else if (!addPhysicalColumn(docId, row, column, indexContainer, state)) {
          rowHadError = true;
        }
      } catch (Exception e) {
        recordOrDeferIndexingError("ROW", e);
        rowHadError = true;
        state.noteError(e);
        try {
          indexDefaultNullColumn(docId, column, indexContainer, state);
        } catch (Exception fallbackError) {
          _logger.error("Failed to index default null for column: {} at docId: {}", column, docId, fallbackError);
          state.noteRepairFailure(fallbackError);
        }
      }
    }

    if (_multiColumnValues != null) {
      try {
        // Multi-column text is a row-level secondary index. It is written only after every column has a finalized
        // value in `_multiColumnValues`. There is no per-doc rollback; a failure leaves this doc absent from the
        // text index while forward indexes remain complete. Counted once as a secondary-index error.
        _multiColumnTextIndex.add(_multiColumnValues);
      } catch (Exception e) {
        recordOrDeferIndexingError("MULTI_COLUMN_TEXT", e);
        rowHadError = true;
        state.noteError(e);
      } finally {
        Collections.fill(_multiColumnValues, null);
      }
    }
    if (state._repairFailed) {
      markUnrecoverable(state);
    }
    return rowHadError;
  }

  /// Returns {@code true} when the aggregated column was written without error.
  private boolean addAggregatedColumn(int docId, GenericRow row, String column, IndexContainer indexContainer,
      RowWriteState state) {
    ValueAggregator valueAggregator = indexContainer._valueAggregator;
    String sourceColumn = indexContainer._sourceColumn;
    Object value = row.getValue(sourceColumn);
    if (value == null && sourceColumn.equals(AggregationFunctionColumnPair.STAR)) {
      assert valueAggregator.getAggregationType() == AggregationFunctionType.COUNT;
      value = 1;
    }

    MutableIndex forwardIndex = indexContainer._mutableIndexes.get(StandardIndexes.forward());
    FieldSpec fieldSpec = indexContainer._fieldSpec;
    DataType dataType = fieldSpec.getDataType();
    try {
      value = valueAggregator.getInitialAggregatedValue(value);
      addAggregatedForwardValue(forwardIndex, valueAggregator, dataType, value, docId, column);
      indexContainer._valuesInfo.updateSVNumValues();
      state._columnsWithCanonicalForward.add(column);
      return true;
    } catch (Exception e) {
      recordOrDeferIndexingError(StandardIndexes.forward(), e);
      state.noteError(e);
      try {
        indexDefaultAggregatedValue(docId, column, indexContainer, state);
      } catch (Exception fallbackError) {
        state.noteRepairFailure(fallbackError);
      }
      return false;
    }
  }

  private void addAggregatedForwardValue(MutableIndex forwardIndex, ValueAggregator valueAggregator, DataType dataType,
      Object value, int docId, String column) {
    maybeIntercept(column, StandardIndexes.forward(), value, docId);
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
        throw new UnsupportedOperationException("Unsupported data type: " + dataType + " for aggregation: " + column);
    }
  }

  /// Returns {@code true} when the physical column was written from the row value without error and without falling
  /// back to the field default.
  private boolean addPhysicalColumn(int docId, GenericRow row, String column, IndexContainer indexContainer,
      RowWriteState state) {
    FieldSpec fieldSpec = indexContainer._fieldSpec;
    DataType dataType = fieldSpec.getDataType();
    boolean isNull = row.isNullValue(column);
    Object value = row.getValue(column);
    boolean defaultSubstituted = false;
    if (value == null) {
      recordIndexingError("NULL_VALUE");
      value = getDefaultNullValueForIndexing(fieldSpec);
      isNull = true;
      defaultSubstituted = true;
      state.markIncomplete();
    }

    if (fieldSpec.isSingleValueField()) {
      if (dataType == DataType.OPEN_STRUCT) {
        return addOpenStructColumn(docId, column, indexContainer, value, defaultSubstituted, state);
      }
      int dictId = indexContainer._dictId;
      if (indexContainer._dictionary != null && dictId == Integer.MIN_VALUE) {
        try {
          Object defaultValue = getDefaultNullValueForIndexing(fieldSpec);
          dictId = indexContainer._dictionary.index(defaultValue);
          indexContainer._dictId = dictId;
          value = defaultValue;
          isNull = true;
          defaultSubstituted = true;
          state.markIncomplete();
        } catch (Exception e) {
          recordOrDeferIndexingError("DICTIONARY", e);
          state.noteError(e);
          state.noteRepairFailure(e);
          return false;
        }
      }
      boolean hadError = writeSingleValueIndexes(docId, column, indexContainer, fieldSpec, dataType, value, dictId,
          isNull, state);
      return !hadError && !defaultSubstituted;
    }

    Object[] values = value instanceof Object[] ? (Object[]) value : new Object[]{value};
    int[] dictIds = indexContainer._dictIds;
    if (indexContainer._dictionary != null && dictIds == null) {
      try {
        Object[] defaultValues = (Object[]) getDefaultNullValueForIndexing(fieldSpec);
        dictIds = indexContainer._dictionary.index(defaultValues);
        indexContainer._dictIds = dictIds;
        values = defaultValues;
        isNull = true;
        defaultSubstituted = true;
        state.markIncomplete();
      } catch (Exception e) {
        recordOrDeferIndexingError("DICTIONARY", e);
        state.noteError(e);
        state.noteRepairFailure(e);
        return false;
      }
    }
    boolean hadError = writeMultiValueIndexes(docId, column, indexContainer, dataType, values, dictIds, isNull, state);
    return !hadError && !defaultSubstituted;
  }

  private boolean addOpenStructColumn(int docId, String column, IndexContainer indexContainer, Object value,
      boolean defaultSubstituted, RowWriteState state) {
    MutableIndex openStructIndex = indexContainer._mutableIndexes.get(StandardIndexes.openStruct());
    if (openStructIndex != null) {
      try {
        maybeIntercept(column, StandardIndexes.openStruct(), value, docId);
        openStructIndex.add(value, -1, docId);
        state._columnsWithCanonicalForward.add(column);
      } catch (Exception e) {
        recordOrDeferIndexingError(StandardIndexes.openStruct(), e);
        state.noteError(e);
        try {
          Object defaultValue = getDefaultNullValueForIndexing(indexContainer._fieldSpec);
          maybeIntercept(column, StandardIndexes.openStruct(), defaultValue, docId);
          openStructIndex.add(defaultValue, -1, docId);
          state._columnsWithCanonicalForward.add(column);
        } catch (Exception fallbackError) {
          state.noteRepairFailure(fallbackError);
          return false;
        }
        defaultSubstituted = true;
      }
    } else {
      state._columnsWithCanonicalForward.add(column);
    }
    indexContainer._valuesInfo.updateSVNumValues();
    return !defaultSubstituted;
  }

  /// Writes the canonical forward index first, then every secondary index from that same finalized value.
  private boolean writeSingleValueIndexes(int docId, String column, IndexContainer indexContainer, FieldSpec fieldSpec,
      DataType dataType, Object value, int dictId, boolean isNull, RowWriteState state) {
    boolean hadError = false;
    MutableIndex forwardIndex = indexContainer._mutableIndexes.get(StandardIndexes.forward());
    if (forwardIndex != null && !state._columnsWithCanonicalForward.contains(column)) {
      try {
        maybeIntercept(column, StandardIndexes.forward(), value, docId);
        forwardIndex.add(value, dictId, docId);
        updateIndexCapacityThresholdBreached(forwardIndex, StandardIndexes.forward(), column);
        state._columnsWithCanonicalForward.add(column);
      } catch (Exception e) {
        recordOrDeferIndexingError(StandardIndexes.forward(), e);
        state.noteError(e);
        hadError = true;
        try {
          Object defaultValue = getDefaultNullValueForIndexing(fieldSpec);
          int defaultDictId = dictId;
          if (indexContainer._dictionary != null) {
            defaultDictId = indexContainer._dictionary.index(defaultValue);
            indexContainer._dictId = defaultDictId;
          }
          value = defaultValue;
          dictId = defaultDictId;
          isNull = true;
          maybeIntercept(column, StandardIndexes.forward(), value, docId);
          forwardIndex.add(value, dictId, docId);
          state._columnsWithCanonicalForward.add(column);
        } catch (Exception fallbackError) {
          _logger.error("Failed to write default forward index for column: {} at docId: {}", column, docId,
              fallbackError);
          state.noteRepairFailure(fallbackError);
          return true;
        }
      }
    }

    if (indexContainer._nullValueVector != null && isNull) {
      indexContainer._nullValueVector.setNull(docId);
    }

    hadError |= writeSecondaryIndexes(docId, column, indexContainer, value, dictId, null, null, true, state);

    if (state._columnsWithCanonicalForward.contains(column)) {
      indexContainer._valuesInfo.updateSVNumValues();
    }

    if (dictId < 0) {
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
    return hadError;
  }

  private boolean writeMultiValueIndexes(int docId, String column, IndexContainer indexContainer, DataType dataType,
      Object[] values, int[] dictIds, boolean isNull, RowWriteState state) {
    boolean hadError = false;
    indexContainer._valuesInfo.updateVarByteMVMaxRowLengthInBytes(values, dataType.getStoredType());
    MutableIndex forwardIndex = indexContainer._mutableIndexes.get(StandardIndexes.forward());
    if (forwardIndex != null && !state._columnsWithCanonicalForward.contains(column)) {
      try {
        maybeIntercept(column, StandardIndexes.forward(), values, docId);
        forwardIndex.add(values, dictIds, docId);
        updateIndexCapacityThresholdBreached(forwardIndex, StandardIndexes.forward(), column);
        state._columnsWithCanonicalForward.add(column);
      } catch (Exception e) {
        recordOrDeferIndexingError(StandardIndexes.forward(), e);
        state.noteError(e);
        hadError = true;
        try {
          Object[] defaultValues = (Object[]) getDefaultNullValueForIndexing(indexContainer._fieldSpec);
          int[] defaultDictIds = dictIds;
          if (indexContainer._dictionary != null) {
            defaultDictIds = indexContainer._dictionary.index(defaultValues);
            indexContainer._dictIds = defaultDictIds;
          }
          values = defaultValues;
          dictIds = defaultDictIds;
          isNull = true;
          maybeIntercept(column, StandardIndexes.forward(), values, docId);
          forwardIndex.add(values, dictIds, docId);
          state._columnsWithCanonicalForward.add(column);
        } catch (Exception fallbackError) {
          _logger.error("Failed to write default MV forward index for column: {} at docId: {}", column, docId,
              fallbackError);
          state.noteRepairFailure(fallbackError);
          return true;
        }
      }
    }

    if (indexContainer._nullValueVector != null && isNull) {
      indexContainer._nullValueVector.setNull(docId);
    }

    hadError |= writeSecondaryIndexes(docId, column, indexContainer, null, -1, values, dictIds, false, state);

    if (state._columnsWithCanonicalForward.contains(column)) {
      indexContainer._valuesInfo.updateMVNumValues(values.length);
    }

    if (_multiColumnValues != null) {
      int pos = _multiColumnPos.getInt(column);
      if (pos > -1) {
        _multiColumnValues.set(pos, values);
      }
    }
    return hadError;
  }

  /// Secondary writers (inverted, range, JSON, text, geospatial, vector) always see the finalized forward value.
  /// A failure here does not rewrite the canonical forward value. Repair is idempotent: already-committed secondaries
  /// are not rewritten.
  private boolean writeSecondaryIndexes(int docId, String column, IndexContainer indexContainer, Object svValue,
      int dictId, Object[] mvValues, int[] dictIds, boolean singleValue, RowWriteState state) {
    boolean hadError = false;
    for (Map.Entry<IndexType, MutableIndex> indexEntry : indexContainer._mutableIndexes.entrySet()) {
      IndexType indexType = indexEntry.getKey();
      if (indexType.equals(StandardIndexes.forward()) || indexType.equals(StandardIndexes.openStruct())) {
        continue;
      }
      String secondaryKey = column + "/" + indexType.getId();
      if (state._committedSecondaries.contains(secondaryKey)) {
        continue;
      }
      try {
        Object interceptValue = singleValue ? svValue : mvValues;
        maybeIntercept(column, indexType, interceptValue, docId);
        MutableIndex mutableIndex = indexEntry.getValue();
        if (singleValue) {
          mutableIndex.add(svValue, dictId, docId);
        } else {
          mutableIndex.add(mvValues, dictIds, docId);
        }
        updateIndexCapacityThresholdBreached(mutableIndex, indexType, column);
        state._committedSecondaries.add(secondaryKey);
      } catch (Exception e) {
        recordOrDeferIndexingError(indexType, e);
        state.noteError(e);
        hadError = true;
      }
    }
    return hadError;
  }

  private static Object getDefaultNullValueForIndexing(FieldSpec fieldSpec) {
    Object defaultNullValue = fieldSpec.getDefaultNullValue();
    if (fieldSpec.isSingleValueField()) {
      return defaultNullValue;
    }
    return new Object[]{defaultNullValue};
  }

  private void indexDefaultNullColumn(int docId, String column, IndexContainer indexContainer, RowWriteState state) {
    if (state._columnsWithCanonicalForward.contains(column)) {
      return;
    }
    FieldSpec fieldSpec = indexContainer._fieldSpec;
    Object defaultValue = getDefaultNullValueForIndexing(fieldSpec);
    if (indexContainer._nullValueVector != null) {
      indexContainer._nullValueVector.setNull(docId);
    }
    if (fieldSpec.getDataType() == DataType.OPEN_STRUCT) {
      MutableIndex openStructIndex = indexContainer._mutableIndexes.get(StandardIndexes.openStruct());
      if (openStructIndex != null) {
        maybeIntercept(column, StandardIndexes.openStruct(), defaultValue, docId);
        openStructIndex.add(defaultValue, -1, docId);
      }
      indexContainer._valuesInfo.updateSVNumValues();
      state._columnsWithCanonicalForward.add(column);
      return;
    }
    MutableIndex forwardIndex = indexContainer._mutableIndexes.get(StandardIndexes.forward());
    if (forwardIndex == null) {
      state._columnsWithCanonicalForward.add(column);
      return;
    }
    if (fieldSpec.isSingleValueField()) {
      int dictId = -1;
      if (indexContainer._dictionary != null) {
        dictId = indexContainer._dictionary.index(defaultValue);
        indexContainer._dictId = dictId;
      }
      maybeIntercept(column, StandardIndexes.forward(), defaultValue, docId);
      forwardIndex.add(defaultValue, dictId, docId);
      indexContainer._valuesInfo.updateSVNumValues();
      state._columnsWithCanonicalForward.add(column);
      writeSecondaryIndexes(docId, column, indexContainer, defaultValue, dictId, null, null, true, state);
    } else {
      Object[] defaultValues = (Object[]) defaultValue;
      int[] dictIds = null;
      if (indexContainer._dictionary != null) {
        dictIds = indexContainer._dictionary.index(defaultValues);
        indexContainer._dictIds = dictIds;
      }
      maybeIntercept(column, StandardIndexes.forward(), defaultValues, docId);
      forwardIndex.add(defaultValues, dictIds, docId);
      indexContainer._valuesInfo.updateMVNumValues(defaultValues.length);
      state._columnsWithCanonicalForward.add(column);
      writeSecondaryIndexes(docId, column, indexContainer, null, -1, defaultValues, dictIds, false, state);
    }
  }

  private void indexDefaultAggregatedValue(int docId, String column, IndexContainer indexContainer,
      RowWriteState state) {
    if (state._columnsWithCanonicalForward.contains(column)) {
      return;
    }
    ValueAggregator valueAggregator = indexContainer._valueAggregator;
    MutableIndex forwardIndex = indexContainer._mutableIndexes.get(StandardIndexes.forward());
    DataType dataType = indexContainer._fieldSpec.getDataType();
    Object value = valueAggregator.getInitialAggregatedValue(null);
    addAggregatedForwardValue(forwardIndex, valueAggregator, dataType, value, docId, column);
    indexContainer._valuesInfo.updateSVNumValues();
    state._columnsWithCanonicalForward.add(column);
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

  private void maybeIntercept(String column, IndexType<?, ?, ?> indexType, Object value, int docId) {
    IndexWriteInterceptor interceptor = _indexWriteInterceptor;
    if (interceptor != null) {
      interceptor.beforeAdd(column, indexType, value, docId);
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

  /// Records an indexing error. When [#_continueOnError] is false the exception is stashed so
  /// [#index] can publish the completed row and then rethrow.
  private void recordOrDeferIndexingError(IndexType<?, ?, ?> indexType, Exception exception) {
    recordIndexingError(indexType, exception);
    if (!_continueOnError && _pendingRowIndexingException == null) {
      _pendingRowIndexingException = exception;
    }
  }

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

  private void markUnrecoverable(RowWriteState state) {
    _unrecoverableIndexingFailure = true;
    if (state._firstError != null && state._repairError != null) {
      state._firstError.addSuppressed(state._repairError);
    }
    if (_pendingRowIndexingException == null && state._firstError != null) {
      _pendingRowIndexingException = state._firstError;
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

  /// Prepares every metric update, then commits them. A failure during prepare leaves prior metrics untouched. A
  /// failure during commit restores already-written metrics; if restore also fails the segment is marked terminal.
  private void aggregateMetrics(GenericRow row, int docId) {
    List<PreparedMetricWrite> prepared = new ArrayList<>();
    for (MetricFieldSpec metricFieldSpec : _physicalMetricFieldSpecs) {
      String column = metricFieldSpec.getName();
      IndexContainer indexContainer = _indexContainerMap.get(column);
      ValueAggregator valueAggregator = indexContainer._valueAggregator;
      String sourceColumn = indexContainer._sourceColumn;
      Object value = row.getValue(sourceColumn);
      if (value == null) {
        if (sourceColumn.equals(AggregationFunctionColumnPair.STAR)) {
          assert valueAggregator.getAggregationType() == AggregationFunctionType.COUNT;
          value = 1;
        } else {
          continue;
        }
      }
      MutableForwardIndex forwardIndex =
          (MutableForwardIndex) indexContainer._mutableIndexes.get(StandardIndexes.forward());
      prepared.add(
          prepareMetricWrite(column, valueAggregator, forwardIndex, metricFieldSpec.getDataType(), value, docId));
    }

    List<PreparedMetricWrite> committed = new ArrayList<>();
    try {
      for (PreparedMetricWrite write : prepared) {
        maybeIntercept(write._column, StandardIndexes.forward(), write.interceptValue(), docId);
        commitMetricWrite(write, docId);
        committed.add(write);
      }
    } catch (Exception e) {
      Exception restoreError = null;
      for (int i = committed.size() - 1; i >= 0; i--) {
        try {
          maybeIntercept(committed.get(i)._column, StandardIndexes.forward(), committed.get(i).restoreInterceptValue(),
              docId);
          restoreMetricWrite(committed.get(i), docId);
        } catch (Exception restoreException) {
          if (restoreError == null) {
            restoreError = restoreException;
          } else {
            restoreError.addSuppressed(restoreException);
          }
        }
      }
      if (restoreError != null) {
        e.addSuppressed(restoreError);
        _unrecoverableIndexingFailure = true;
      }
      throw wrapIndexingException("AGGREGATE_METRICS", e);
    }
  }

  private PreparedMetricWrite prepareMetricWrite(String column, ValueAggregator valueAggregator,
      MutableForwardIndex forwardIndex, DataType dataType, Object rawValue, int docId) {
    switch (valueAggregator.getAggregatedValueType()) {
      case DOUBLE: {
        double oldDoubleValue;
        double newDoubleValue;
        MetricWriteKind kind;
        switch (dataType) {
          case INT:
            oldDoubleValue = forwardIndex.getInt(docId);
            newDoubleValue = (double) valueAggregator.applyRawValue(oldDoubleValue, rawValue);
            kind = MetricWriteKind.INT;
            break;
          case LONG:
            oldDoubleValue = forwardIndex.getLong(docId);
            newDoubleValue = (double) valueAggregator.applyRawValue(oldDoubleValue, rawValue);
            kind = MetricWriteKind.LONG;
            break;
          case FLOAT:
            oldDoubleValue = forwardIndex.getFloat(docId);
            newDoubleValue = (double) valueAggregator.applyRawValue(oldDoubleValue, rawValue);
            kind = MetricWriteKind.FLOAT;
            break;
          case DOUBLE:
            oldDoubleValue = forwardIndex.getDouble(docId);
            newDoubleValue = (double) valueAggregator.applyRawValue(oldDoubleValue, rawValue);
            kind = MetricWriteKind.DOUBLE;
            break;
          default:
            throw new UnsupportedOperationException(String.format("Aggregation type %s of %s not supported for %s",
                valueAggregator.getAggregatedValueType(), valueAggregator.getAggregationType(), dataType));
        }
        return PreparedMetricWrite.numeric(column, forwardIndex, kind, oldDoubleValue, newDoubleValue);
      }
      case LONG: {
        long oldLongValue;
        long newLongValue;
        MetricWriteKind kind;
        switch (dataType) {
          case INT:
            oldLongValue = forwardIndex.getInt(docId);
            newLongValue = (long) valueAggregator.applyRawValue(oldLongValue, rawValue);
            kind = MetricWriteKind.INT;
            break;
          case LONG:
            oldLongValue = forwardIndex.getLong(docId);
            newLongValue = (long) valueAggregator.applyRawValue(oldLongValue, rawValue);
            kind = MetricWriteKind.LONG;
            break;
          case FLOAT:
            oldLongValue = (long) forwardIndex.getFloat(docId);
            newLongValue = (long) valueAggregator.applyRawValue(oldLongValue, rawValue);
            kind = MetricWriteKind.FLOAT;
            break;
          case DOUBLE:
            oldLongValue = (long) forwardIndex.getDouble(docId);
            newLongValue = (long) valueAggregator.applyRawValue(oldLongValue, rawValue);
            kind = MetricWriteKind.DOUBLE;
            break;
          default:
            throw new UnsupportedOperationException(String.format("Aggregation type %s of %s not supported for %s",
                valueAggregator.getAggregatedValueType(), valueAggregator.getAggregationType(), dataType));
        }
        return PreparedMetricWrite.numeric(column, forwardIndex, kind, oldLongValue, newLongValue);
      }
      case BYTES: {
        Object oldValue = valueAggregator.deserializeAggregatedValue(forwardIndex.getBytes(docId));
        Object newValue = valueAggregator.applyRawValue(oldValue, rawValue);
        return PreparedMetricWrite.bytes(column, forwardIndex, valueAggregator.serializeAggregatedValue(oldValue),
            valueAggregator.serializeAggregatedValue(newValue));
      }
      default:
        throw new UnsupportedOperationException(
            String.format("Aggregation type %s of %s not supported for %s", valueAggregator.getAggregatedValueType(),
                valueAggregator.getAggregationType(), dataType));
    }
  }

  private static void commitMetricWrite(PreparedMetricWrite write, int docId) {
    switch (write._kind) {
      case INT:
        write._forwardIndex.setInt(docId, write._newNumber.intValue());
        break;
      case LONG:
        write._forwardIndex.setLong(docId, write._newNumber.longValue());
        break;
      case FLOAT:
        write._forwardIndex.setFloat(docId, write._newNumber.floatValue());
        break;
      case DOUBLE:
        write._forwardIndex.setDouble(docId, write._newNumber.doubleValue());
        break;
      case BYTES:
        write._forwardIndex.setBytes(docId, write._newBytes);
        break;
      default:
        throw new UnsupportedOperationException("Unsupported metric write kind: " + write._kind);
    }
  }

  private static void restoreMetricWrite(PreparedMetricWrite write, int docId) {
    switch (write._kind) {
      case INT:
        write._forwardIndex.setInt(docId, write._oldNumber.intValue());
        break;
      case LONG:
        write._forwardIndex.setLong(docId, write._oldNumber.longValue());
        break;
      case FLOAT:
        write._forwardIndex.setFloat(docId, write._oldNumber.floatValue());
        break;
      case DOUBLE:
        write._forwardIndex.setDouble(docId, write._oldNumber.doubleValue());
        break;
      case BYTES:
        write._forwardIndex.setBytes(docId, write._oldBytes);
        break;
      default:
        throw new UnsupportedOperationException("Unsupported metric write kind: " + write._kind);
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
    return !_indexCapacityThresholdBreached && !_unrecoverableIndexingFailure;
  }

  /// Row-count capacity plus [#canAddMore]. Consumers use this after a published-then-rethrown repair.
  public boolean canTakeMoreRows() {
    return _numDocsIndexed < _capacity && canAddMore();
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

  /// Per-column cap on the number of values in a multi-value entry, as configured on the mutable index context.
  private record MultiValueLimit(String column, int maxNumMultiValues) {
  }

  /// Per-row publication tracker for issue #16316. Records which columns reached a canonical forward value and
  /// which secondary indexes were committed so repair is idempotent.
  private static final class RowWriteState {
    final Set<String> _columnsWithCanonicalForward = new HashSet<>();
    final Set<String> _committedSecondaries = new HashSet<>();
    boolean _incomplete;
    boolean _repairFailed;
    @Nullable
    Exception _firstError;
    @Nullable
    Exception _repairError;

    void markIncomplete() {
      _incomplete = true;
    }

    void noteError(Exception exception) {
      _incomplete = true;
      if (_firstError == null) {
        _firstError = exception;
      }
    }

    void noteRepairFailure(Exception exception) {
      _repairFailed = true;
      if (_repairError == null) {
        _repairError = exception;
      } else {
        _repairError.addSuppressed(exception);
      }
    }

    void reset() {
      _columnsWithCanonicalForward.clear();
      _committedSecondaries.clear();
      _incomplete = false;
      _repairFailed = false;
      _firstError = null;
      _repairError = null;
    }
  }

  /// Test-only hook invoked immediately before each mutable-index write, including fallback writes.
  @FunctionalInterface
  interface IndexWriteInterceptor {
    void beforeAdd(String column, IndexType<?, ?, ?> indexType, Object value, int docId);
  }

  private enum MetricWriteKind {
    INT,
    LONG,
    FLOAT,
    DOUBLE,
    BYTES
  }

  /// Prepared in-place metric update. All metrics are computed before any forward-index write.
  private static final class PreparedMetricWrite {
    final String _column;
    final MutableForwardIndex _forwardIndex;
    final MetricWriteKind _kind;
    @Nullable
    final Number _oldNumber;
    @Nullable
    final Number _newNumber;
    @Nullable
    final byte[] _oldBytes;
    @Nullable
    final byte[] _newBytes;

    private PreparedMetricWrite(String column, MutableForwardIndex forwardIndex, MetricWriteKind kind,
        @Nullable Number oldNumber, @Nullable Number newNumber, @Nullable byte[] oldBytes,
        @Nullable byte[] newBytes) {
      _column = column;
      _forwardIndex = forwardIndex;
      _kind = kind;
      _oldNumber = oldNumber;
      _newNumber = newNumber;
      _oldBytes = oldBytes;
      _newBytes = newBytes;
    }

    static PreparedMetricWrite numeric(String column, MutableForwardIndex forwardIndex, MetricWriteKind kind,
        Number oldNumber, Number newNumber) {
      return new PreparedMetricWrite(column, forwardIndex, kind, oldNumber, newNumber, null, null);
    }

    static PreparedMetricWrite bytes(String column, MutableForwardIndex forwardIndex, byte[] oldBytes,
        byte[] newBytes) {
      return new PreparedMetricWrite(column, forwardIndex, MetricWriteKind.BYTES, null, null, oldBytes, newBytes);
    }

    Object interceptValue() {
      return _kind == MetricWriteKind.BYTES ? _newBytes : _newNumber;
    }

    Object restoreInterceptValue() {
      return _kind == MetricWriteKind.BYTES ? _oldBytes : _oldNumber;
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
