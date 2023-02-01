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
package org.apache.pinot.segment.local.segment.creator.impl;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.pinot.common.utils.FileUtils;
import org.apache.pinot.segment.local.io.util.PinotDataBitSet;
import org.apache.pinot.segment.local.segment.creator.impl.inv.BitSlicedRangeIndexCreator;
import org.apache.pinot.segment.local.segment.creator.impl.nullvalue.NullValueVectorCreator;
import org.apache.pinot.segment.local.segment.index.dictionary.DictionaryIndexType;
import org.apache.pinot.segment.local.segment.index.forward.ForwardIndexType;
import org.apache.pinot.segment.spi.V1Constants;
import org.apache.pinot.segment.spi.compression.ChunkCompressionType;
import org.apache.pinot.segment.spi.creator.ColumnIndexCreationInfo;
import org.apache.pinot.segment.spi.creator.IndexCreationContext;
import org.apache.pinot.segment.spi.creator.SegmentCreator;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
import org.apache.pinot.segment.spi.index.DictionaryIndexConfig;
import org.apache.pinot.segment.spi.index.FieldIndexConfigs;
import org.apache.pinot.segment.spi.index.IndexCreator;
import org.apache.pinot.segment.spi.index.IndexDeclaration;
import org.apache.pinot.segment.spi.index.IndexService;
import org.apache.pinot.segment.spi.index.IndexType;
import org.apache.pinot.segment.spi.index.StandardIndexes;
import org.apache.pinot.segment.spi.index.creator.SegmentIndexCreationInfo;
import org.apache.pinot.segment.spi.partition.PartitionFunction;
import org.apache.pinot.spi.config.table.SegmentZKPropsConfig;
import org.apache.pinot.spi.data.DateTimeFieldSpec;
import org.apache.pinot.spi.data.DateTimeFormatSpec;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.FieldSpec.FieldType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.utils.TimeUtils;
import org.joda.time.DateTimeZone;
import org.joda.time.Interval;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.pinot.segment.spi.V1Constants.MetadataKeys.Column.*;
import static org.apache.pinot.segment.spi.V1Constants.MetadataKeys.Segment.*;

/**
 * Segment creator which writes data in a columnar form.
 */
// TODO: check resource leaks
public class SegmentColumnarIndexCreator implements SegmentCreator {
  // TODO Refactor class name to match interface name
  private static final Logger LOGGER = LoggerFactory.getLogger(SegmentColumnarIndexCreator.class);
  // Allow at most 512 characters for the metadata property
  private static final int METADATA_PROPERTY_LENGTH_LIMIT = 512;

  private SegmentGeneratorConfig _config;
  private Map<String, ColumnIndexCreationInfo> _indexCreationInfoMap;
  private final Map<String, SegmentDictionaryCreator> _dictionaryCreatorMap = new HashMap<>();
  /**
   * Contains, indexed by column name, the creator associated with each index type.
   *
   * Indexes that are {@link #skipIndexType(IndexType) skipped} are not included here.
   */
  private Map<String, Map<IndexType<?, ?, ?>, IndexCreator>> _creatorsByColAndIndex = new HashMap<>();
  private final Map<String, NullValueVectorCreator> _nullValueVectorCreatorMap = new HashMap<>();
  private String _segmentName;
  private Schema _schema;
  private File _indexDir;
  private int _totalDocs;
  private int _docIdCounter;
  private boolean _nullHandlingEnabled;

  @Override
  public void init(SegmentGeneratorConfig segmentCreationSpec, SegmentIndexCreationInfo segmentIndexCreationInfo,
      Map<String, ColumnIndexCreationInfo> indexCreationInfoMap, Schema schema, File outDir)
      throws Exception {
    _docIdCounter = 0;
    _config = segmentCreationSpec;
    _indexCreationInfoMap = indexCreationInfoMap;

    // Check that the output directory does not exist
    Preconditions.checkState(!outDir.exists(), "Segment output directory: %s already exists", outDir);

    Preconditions.checkState(outDir.mkdirs(), "Failed to create output directory: %s", outDir);
    _indexDir = outDir;

    _schema = schema;
    _totalDocs = segmentIndexCreationInfo.getTotalDocs();
    if (_totalDocs == 0) {
      return;
    }

    // Although NullValueVector is implemented as an index, it needs to be treated in a different way than other indexes
    _nullHandlingEnabled = _config.isNullHandlingEnabled();
    if (_nullHandlingEnabled) {
      for (FieldSpec fieldSpec : schema.getAllFieldSpecs()) {
        // Initialize Null value vector map
        String columnName = fieldSpec.getName();
        _nullValueVectorCreatorMap.put(columnName, new NullValueVectorCreator(_indexDir, columnName));
      }
    }

    Map<String, FieldIndexConfigs> indexConfigs = segmentCreationSpec.getIndexConfigsByColName();

    _creatorsByColAndIndex = Maps.newHashMapWithExpectedSize(indexConfigs.keySet().size());

    for (String columnName : indexConfigs.keySet()) {
      FieldSpec fieldSpec = schema.getFieldSpecFor(columnName);
      if (fieldSpec == null) {
        throw new IllegalStateException("Cannot create index for column: " + columnName + " because it is not in "
            + "schema");
      }
      if (fieldSpec.isVirtualColumn()) {
        LOGGER.warn("Ignoring index creation for virtual column " + columnName);
        continue;
      }

      FieldIndexConfigs fieldIndexConfigs = indexConfigs.get(columnName);
      ColumnIndexCreationInfo columnIndexCreationInfo = indexCreationInfoMap.get(columnName);
      Preconditions.checkNotNull(columnIndexCreationInfo, "Missing index creation info for column: %s", columnName);
      boolean dictEnabledColumn = createDictionaryForColumn(columnIndexCreationInfo, segmentCreationSpec, fieldSpec);

      if (!dictEnabledColumn && fieldIndexConfigs.getConfig(StandardIndexes.inverted()).isEnabled()) {
        throw new IllegalStateException("Cannot create inverted index for raw index column: " + columnName);
      }

      int rangeIndexVersion = _config.getTableConfig().getIndexingConfig().getRangeIndexVersion();
      boolean forwardIndexDisabled = !fieldIndexConfigs.getConfig(StandardIndexes.forward()).isEnabled();

      validateForwardIndexDisabledIndexCompatibility(fieldIndexConfigs, dictEnabledColumn,
          columnIndexCreationInfo, rangeIndexVersion, fieldSpec);

      IndexCreationContext.Common context = IndexCreationContext.builder()
          .withIndexDir(_indexDir)
          .withDictionary(dictEnabledColumn)
          .withFieldSpec(fieldSpec)
          .withTotalDocs(segmentIndexCreationInfo.getTotalDocs())
          .withColumnIndexCreationInfo(columnIndexCreationInfo)
          .withIsOptimizedDictionary(_config.isOptimizeDictionary()
              || _config.isOptimizeDictionaryForMetrics() && fieldSpec.getFieldType() == FieldSpec.FieldType.METRIC)
          .onHeap(segmentCreationSpec.isOnHeap())
          .withforwardIndexDisabled(forwardIndexDisabled)
          .withFixedLength(columnIndexCreationInfo.isFixedLength())
          .withTextCommitOnClose(true)
          .build();

      if (dictEnabledColumn) {
        // Create dictionary-encoded index
        // Initialize dictionary creator
        // TODO: Dictionary creator holds all unique values on heap. Consider keeping dictionary instead of creator
        //       which uses off-heap memory.

        DictionaryIndexType dictIdx = DictionaryIndexType.INSTANCE;
        // Index conf should be present if dictEnabledColumn is true. In case it doesn't, getConfig will throw an
        // exception
        DictionaryIndexConfig dictConfig = fieldIndexConfigs.getConfig(dictIdx).getEnabledConfig();
        SegmentDictionaryCreator creator = dictIdx.createIndexCreator(context, dictConfig);

        try {
          creator.build(context.getSortedUniqueElementsArray());
        } catch (Exception e) {
          LOGGER.error("Error building dictionary for field: {}, cardinality: {}, number of bytes per entry: {}",
              context.getFieldSpec().getName(), context.getCardinality(), creator.getNumBytesPerEntry());
          throw e;
        }

        _dictionaryCreatorMap.put(columnName, creator);
      }

      Map<IndexType<?, ?, ?>, IndexCreator> creatorsByIndex =
          Maps.newHashMapWithExpectedSize(IndexService.getInstance().getAllIndexes().size());
      for (IndexType<?, ?, ?> index : IndexService.getInstance().getAllIndexes()) {
        if (skipIndexType(index)) {
          continue;
        }
        if (fieldIndexConfigs.getConfig(index).isEnabled()) {
          tryCreateCreator(creatorsByIndex, index, context, fieldIndexConfigs);
        }
      }
      _creatorsByColAndIndex.put(columnName, creatorsByIndex);
    }
  }

  /**
   * Returns true if the given index type has their own construction lifecycle and therefore should not be instantiated
   * in the general index loop and shouldn't be notified of each new column.
   */
  private boolean skipIndexType(IndexType<?, ?, ?> indexType) {
    return indexType == StandardIndexes.nullValueVector() || indexType == StandardIndexes.dictionary();
  }

  /**
   * Creates the {@link IndexCreator} in a type safe way.
   *
   * This code needs to be in a specific method instead of inlined in the main loop in order to be able to use the
   * limited generic capabilities of Java.
   */
  private <C> void tryCreateCreator(Map<IndexType<?, ?, ?>, IndexCreator> creatorsByIndex,
      IndexType<C, ?, ?> index, IndexCreationContext.Common context, FieldIndexConfigs fieldIndexConfigs)
      throws Exception {
    IndexDeclaration<C> declaration = fieldIndexConfigs.getConfig(index);
    if (declaration.isEnabled() && index.shouldBeCreated(context, fieldIndexConfigs)) {
      creatorsByIndex.put(index, index.createIndexCreator(context, declaration.getEnabledConfig()));
    }
  }

  /**
   * Validates the compatibility of the indexes if the column has the forward index disabled. Throws exceptions due to
   * compatibility mismatch. The checks performed are:
   *     - Validate dictionary is enabled.
   *     - Validate inverted index is enabled.
   *     - Validate that either no range index exists for column or the range index version is at least 2 and isn't a
   *       multi-value column (since multi-value defaults to index v1).
   * TODO(index-spi): Ideally this should be delegated into each index type
   */
  private void validateForwardIndexDisabledIndexCompatibility(FieldIndexConfigs fieldIndexConfigs,
      boolean dictEnabledColumn, ColumnIndexCreationInfo columnIndexCreationInfo, int rangeIndexVersion,
      FieldSpec fieldSpec) {
    if (isEnabled(fieldIndexConfigs, StandardIndexes.forward())) {
      return;
    }
    String columnName = fieldSpec.getName();

    Preconditions.checkState(dictEnabledColumn,
        String.format("Cannot disable forward index for column %s without dictionary", columnName));
    Preconditions.checkState(isEnabled(fieldIndexConfigs, StandardIndexes.inverted()),
        String.format("Cannot disable forward index for column %s without inverted index enabled", columnName));
    if (isEnabled(fieldIndexConfigs, StandardIndexes.range())) {
      Preconditions.checkState(fieldSpec.isSingleValueField(),
          String.format("Feature not supported for multi-value columns with range index. Cannot disable forward index "
              + "for column %s. Disable range index on this column to use this feature", columnName));
      Preconditions.checkState(rangeIndexVersion == BitSlicedRangeIndexCreator.VERSION,
          String.format("Feature not supported for single-value columns with range index version < 2. Cannot disable "
              + "forward index for column %s. Either disable range index or create range index with version >= 2 to "
              + "use this feature", columnName));
    }
  }

  private boolean isEnabled(FieldIndexConfigs fieldIndexConfigs, IndexType<?, ?, ?> indexType) {
    return fieldIndexConfigs.getConfig(indexType).isEnabled();
  }

  /**
   * Returns true if dictionary should be created for a column, false otherwise.
   * Currently there are two sources for this config:
   * <ul>
   *   <li> ColumnIndexCreationInfo (this is currently hard-coded to always return dictionary). </li>
   *   <li> SegmentGeneratorConfig</li>
   * </ul>
   *
   * This method gives preference to the SegmentGeneratorConfig first.
   *
   * @param info Column index creation info
   * @param config Segment generation config
   * @param spec Field spec for the column
   * @return True if dictionary should be created for the column, false otherwise
   */
  private boolean createDictionaryForColumn(ColumnIndexCreationInfo info, SegmentGeneratorConfig config,
      FieldSpec spec) {
    String column = spec.getName();
    if (config.getRawIndexCreationColumns().contains(column) || config.getRawIndexCompressionType()
        .containsKey(column)) {
      return false;
    }

    if (config.isOptimizeDictionary()) {

      FieldIndexConfigs fieldIndexConfigs = config.getIndexConfigsByColName().get(column);
      // Do not create dictionaries for json or text index columns as they are high-cardinality values almost always
      if ((fieldIndexConfigs.getConfig(StandardIndexes.json()).isEnabled()
          || fieldIndexConfigs.getConfig(StandardIndexes.text()).isEnabled())) {
        return false;
      }

      // Do not create dictionary if index size with dictionary is going to be larger than index size without dictionary
      // This is done to reduce the cost of dictionary for high cardinality columns
      // Off by default and needs optimizeDictionary to be set to true
      if (spec.isSingleValueField() && spec.getDataType().isFixedWidth()) {
        return shouldCreateDictionaryWithinThreshold(info, config, spec);
      }
    }

    if (config.isOptimizeDictionaryForMetrics() && !config.isOptimizeDictionary()) {
      if (spec.isSingleValueField() && spec.getDataType().isFixedWidth() && spec.getFieldType() == FieldType.METRIC) {
        return shouldCreateDictionaryWithinThreshold(info, config, spec);
      }
    }

    return info.isCreateDictionary();
  }

  private boolean shouldCreateDictionaryWithinThreshold(ColumnIndexCreationInfo info,
      SegmentGeneratorConfig config, FieldSpec spec) {
    long dictionarySize = info.getDistinctValueCount() * (long) spec.getDataType().size();
    long forwardIndexSize =
        ((long) info.getTotalNumberOfEntries()
            * PinotDataBitSet.getNumBitsPerValue(info.getDistinctValueCount() - 1) + Byte.SIZE - 1) / Byte.SIZE;

    double indexWithDictSize = dictionarySize + forwardIndexSize;
    double indexWithoutDictSize = info.getTotalNumberOfEntries() * spec.getDataType().size();

    double indexSizeRatio = indexWithoutDictSize / indexWithDictSize;
    if (indexSizeRatio <= config.getNoDictionarySizeRatioThreshold()) {
      return false;
    }
    return info.isCreateDictionary();
  }

  /**
   * @deprecated use {@link ForwardIndexType#getDefaultCompressionType(FieldType)} instead
   */
  @Deprecated
  public static ChunkCompressionType getDefaultCompressionType(FieldType fieldType) {
    return ForwardIndexType.getDefaultCompressionType(fieldType);
  }

  @Override
  public void indexRow(GenericRow row)
      throws IOException {
    for (Map.Entry<String, Map<IndexType<?, ?, ?>, IndexCreator>> byColEntry : _creatorsByColAndIndex.entrySet()) {
      String columnName = byColEntry.getKey();
      Map<IndexType<?, ?, ?>, IndexCreator> creatorsByIndex = byColEntry.getValue();

      FieldSpec fieldSpec = _schema.getFieldSpecFor(columnName);
      SegmentDictionaryCreator dictionaryCreator = _dictionaryCreatorMap.get(columnName);

      Object columnValueToIndex = row.getValue(columnName);
      if (columnValueToIndex == null) {
        throw new RuntimeException("Null value for column:" + columnName);
      }

      if (fieldSpec.isSingleValueField()) {
        indexSingleValueRow(dictionaryCreator, columnValueToIndex, creatorsByIndex);
      } else {
        indexMultiValueRow(dictionaryCreator, (Object[]) columnValueToIndex, creatorsByIndex);
      }
    }

    if (_nullHandlingEnabled) {
      for (Map.Entry<String, NullValueVectorCreator> entry : _nullValueVectorCreatorMap.entrySet()) {
        String columnName = entry.getKey();
        // If row has null value for given column name, add to null value vector
        if (row.isNullValue(columnName)) {
          _nullValueVectorCreatorMap.get(columnName).setNull(_docIdCounter);
        }
      }
    }

    _docIdCounter++;
  }

  private void indexSingleValueRow(SegmentDictionaryCreator dictionaryCreator, Object value,
      Map<IndexType<?, ?, ?>, IndexCreator> creatorsByIndex)
      throws IOException {
    int dictId = dictionaryCreator != null ? dictionaryCreator.indexOfSV(value) : -1;
    Object alternative = calculateAlternativeValue(creatorsByIndex, IndexCreator::alternativeSingleValue, value);
    for (IndexCreator creator : creatorsByIndex.values()) {
      creator.addSingleValueCell(value, dictId, alternative);
    }
  }

  private void indexMultiValueRow(SegmentDictionaryCreator dictionaryCreator, Object[] values,
      Map<IndexType<?, ?, ?>, IndexCreator> creatorsByIndex)
      throws IOException {
    int[] dictId = dictionaryCreator != null ? dictionaryCreator.indexOfMV(values) : null;
    Object[] alternative = calculateAlternativeValue(creatorsByIndex, IndexCreator::alternativeMultiValue, values);
    for (IndexCreator creator : creatorsByIndex.values()) {
      creator.addMultiValueCell(values, dictId, alternative);
    }
  }

  private <E> E calculateAlternativeValue(Map<IndexType<?, ?, ?>, IndexCreator> creatorsByIndex,
      BiFunction<IndexCreator, E, E> extractor, E originalValue) {
    E valueToStore = null;
    IndexType<?, ?, ?> indexChangingValue = null;
    for (Map.Entry<IndexType<?, ?, ?>, IndexCreator> byIndexEntry : creatorsByIndex.entrySet()) {
      E alternative = extractor.apply(byIndexEntry.getValue(), originalValue);
      if (alternative != null) {
        IndexType<?, ?, ?> currentIndex = byIndexEntry.getKey();
        if (valueToStore == null) {
          valueToStore = alternative;
          indexChangingValue = currentIndex;
        } else {
          throw new IllegalStateException("Both " + indexChangingValue + " and " + currentIndex + " indexes are "
              + "trying to change the value to store");
        }
      }
    }
    return valueToStore != null ? valueToStore : originalValue;
  }

  @Override
  public void setSegmentName(String segmentName) {
    _segmentName = segmentName;
  }

  @Override
  public void seal()
      throws ConfigurationException, IOException {
    for (SegmentDictionaryCreator creator : _dictionaryCreatorMap.values()) {
      creator.seal();
    }
    for (NullValueVectorCreator creator : _nullValueVectorCreatorMap.values()) {
      creator.seal();
    }
    for (Map<IndexType<?, ?, ?>, IndexCreator> creatorsByType : _creatorsByColAndIndex.values()) {
      for (IndexCreator creator : creatorsByType.values()) {
        creator.seal();
      }
    }
    writeMetadata();
  }

  private void writeMetadata()
      throws ConfigurationException {
    PropertiesConfiguration properties =
        new PropertiesConfiguration(new File(_indexDir, V1Constants.MetadataKeys.METADATA_FILE_NAME));

    properties.setProperty(SEGMENT_CREATOR_VERSION, _config.getCreatorVersion());
    properties.setProperty(SEGMENT_PADDING_CHARACTER, String.valueOf(V1Constants.Str.DEFAULT_STRING_PAD_CHAR));
    properties.setProperty(SEGMENT_NAME, _segmentName);
    properties.setProperty(TABLE_NAME, _config.getTableName());
    properties.setProperty(DIMENSIONS, _config.getDimensions());
    properties.setProperty(METRICS, _config.getMetrics());
    properties.setProperty(DATETIME_COLUMNS, _config.getDateTimeColumnNames());
    String timeColumnName = _config.getTimeColumnName();
    properties.setProperty(TIME_COLUMN_NAME, timeColumnName);
    properties.setProperty(SEGMENT_TOTAL_DOCS, String.valueOf(_totalDocs));

    // Write time related metadata (start time, end time, time unit)
    if (timeColumnName != null) {
      ColumnIndexCreationInfo timeColumnIndexCreationInfo = _indexCreationInfoMap.get(timeColumnName);
      if (timeColumnIndexCreationInfo != null) {
        long startTime;
        long endTime;
        TimeUnit timeUnit;

        // Use start/end time in config if defined
        if (_config.getStartTime() != null) {
          startTime = Long.parseLong(_config.getStartTime());
          endTime = Long.parseLong(_config.getEndTime());
          timeUnit = Preconditions.checkNotNull(_config.getSegmentTimeUnit());
        } else {
          if (_totalDocs > 0) {
            String startTimeStr = timeColumnIndexCreationInfo.getMin().toString();
            String endTimeStr = timeColumnIndexCreationInfo.getMax().toString();

            if (_config.getTimeColumnType() == SegmentGeneratorConfig.TimeColumnType.SIMPLE_DATE) {
              // For TimeColumnType.SIMPLE_DATE_FORMAT, convert time value into millis since epoch
              // Use DateTimeFormatter from DateTimeFormatSpec to handle default time zone consistently.
              DateTimeFormatSpec formatSpec = _config.getDateTimeFormatSpec();
              Preconditions.checkNotNull(formatSpec, "DateTimeFormatSpec must exist for SimpleDate");
              DateTimeFormatter dateTimeFormatter = formatSpec.getDateTimeFormatter();
              startTime = dateTimeFormatter.parseMillis(startTimeStr);
              endTime = dateTimeFormatter.parseMillis(endTimeStr);
              timeUnit = TimeUnit.MILLISECONDS;
            } else {
              // by default, time column type is TimeColumnType.EPOCH
              startTime = Long.parseLong(startTimeStr);
              endTime = Long.parseLong(endTimeStr);
              timeUnit = Preconditions.checkNotNull(_config.getSegmentTimeUnit());
            }
          } else {
            // No records in segment. Use current time as start/end
            long now = System.currentTimeMillis();
            if (_config.getTimeColumnType() == SegmentGeneratorConfig.TimeColumnType.SIMPLE_DATE) {
              startTime = now;
              endTime = now;
              timeUnit = TimeUnit.MILLISECONDS;
            } else {
              timeUnit = Preconditions.checkNotNull(_config.getSegmentTimeUnit());
              startTime = timeUnit.convert(now, TimeUnit.MILLISECONDS);
              endTime = timeUnit.convert(now, TimeUnit.MILLISECONDS);
            }
          }
        }

        if (!_config.isSkipTimeValueCheck()) {
          Interval timeInterval =
              new Interval(timeUnit.toMillis(startTime), timeUnit.toMillis(endTime), DateTimeZone.UTC);
          Preconditions.checkState(TimeUtils.isValidTimeInterval(timeInterval),
              "Invalid segment start/end time: %s (in millis: %s/%s) for time column: %s, must be between: %s",
              timeInterval, timeInterval.getStartMillis(), timeInterval.getEndMillis(), timeColumnName,
              TimeUtils.VALID_TIME_INTERVAL);
        }

        properties.setProperty(SEGMENT_START_TIME, startTime);
        properties.setProperty(SEGMENT_END_TIME, endTime);
        properties.setProperty(TIME_UNIT, timeUnit);
      }
    }

    for (Map.Entry<String, String> entry : _config.getCustomProperties().entrySet()) {
      properties.setProperty(entry.getKey(), entry.getValue());
    }

    for (Map.Entry<String, ColumnIndexCreationInfo> entry : _indexCreationInfoMap.entrySet()) {
      String column = entry.getKey();
      ColumnIndexCreationInfo columnIndexCreationInfo = entry.getValue();
      SegmentDictionaryCreator dictionaryCreator = _dictionaryCreatorMap.get(column);
      int dictionaryElementSize = (dictionaryCreator != null) ? dictionaryCreator.getNumBytesPerEntry() : 0;
      addColumnMetadataInfo(properties, column, columnIndexCreationInfo, _totalDocs, _schema.getFieldSpecFor(column),
          dictionaryCreator != null, dictionaryElementSize);
    }

    SegmentZKPropsConfig segmentZKPropsConfig = _config.getSegmentZKPropsConfig();
    if (segmentZKPropsConfig != null) {
      properties.setProperty(Realtime.START_OFFSET, segmentZKPropsConfig.getStartOffset());
      properties.setProperty(Realtime.END_OFFSET, segmentZKPropsConfig.getEndOffset());
    }

    properties.save();
  }

  public static void addColumnMetadataInfo(PropertiesConfiguration properties, String column,
      ColumnIndexCreationInfo columnIndexCreationInfo, int totalDocs, FieldSpec fieldSpec, boolean hasDictionary,
      int dictionaryElementSize) {
    int cardinality = columnIndexCreationInfo.getDistinctValueCount();
    properties.setProperty(getKeyFor(column, CARDINALITY), String.valueOf(cardinality));
    properties.setProperty(getKeyFor(column, TOTAL_DOCS), String.valueOf(totalDocs));
    DataType dataType = fieldSpec.getDataType();
    properties.setProperty(getKeyFor(column, DATA_TYPE), String.valueOf(dataType));
    properties.setProperty(getKeyFor(column, BITS_PER_ELEMENT),
        String.valueOf(PinotDataBitSet.getNumBitsPerValue(cardinality - 1)));
    properties.setProperty(getKeyFor(column, DICTIONARY_ELEMENT_SIZE), String.valueOf(dictionaryElementSize));
    properties.setProperty(getKeyFor(column, COLUMN_TYPE), String.valueOf(fieldSpec.getFieldType()));
    properties.setProperty(getKeyFor(column, IS_SORTED), String.valueOf(columnIndexCreationInfo.isSorted()));
    properties.setProperty(getKeyFor(column, HAS_DICTIONARY), String.valueOf(hasDictionary));
    properties.setProperty(getKeyFor(column, IS_SINGLE_VALUED), String.valueOf(fieldSpec.isSingleValueField()));
    properties.setProperty(getKeyFor(column, MAX_MULTI_VALUE_ELEMENTS),
        String.valueOf(columnIndexCreationInfo.getMaxNumberOfMultiValueElements()));
    properties.setProperty(getKeyFor(column, TOTAL_NUMBER_OF_ENTRIES),
        String.valueOf(columnIndexCreationInfo.getTotalNumberOfEntries()));
    properties.setProperty(getKeyFor(column, IS_AUTO_GENERATED),
        String.valueOf(columnIndexCreationInfo.isAutoGenerated()));

    PartitionFunction partitionFunction = columnIndexCreationInfo.getPartitionFunction();
    if (partitionFunction != null) {
      properties.setProperty(getKeyFor(column, PARTITION_FUNCTION), partitionFunction.getName());
      properties.setProperty(getKeyFor(column, NUM_PARTITIONS), columnIndexCreationInfo.getNumPartitions());
      properties.setProperty(getKeyFor(column, PARTITION_VALUES), columnIndexCreationInfo.getPartitions());
      if (columnIndexCreationInfo.getPartitionFunctionConfig() != null) {
        for (Map.Entry<String, String> entry : columnIndexCreationInfo.getPartitionFunctionConfig().entrySet()) {
          properties.setProperty(getKeyFor(column, String.format("%s.%s", PARTITION_FUNCTION_CONFIG, entry.getKey())),
              entry.getValue());
        }
      }
    }

    // datetime field
    if (fieldSpec.getFieldType().equals(FieldType.DATE_TIME)) {
      DateTimeFieldSpec dateTimeFieldSpec = (DateTimeFieldSpec) fieldSpec;
      properties.setProperty(getKeyFor(column, DATETIME_FORMAT), dateTimeFieldSpec.getFormat());
      properties.setProperty(getKeyFor(column, DATETIME_GRANULARITY), dateTimeFieldSpec.getGranularity());
    }

    // NOTE: Min/max could be null for real-time aggregate metrics.
    if (totalDocs > 0) {
      Object min = columnIndexCreationInfo.getMin();
      Object max = columnIndexCreationInfo.getMax();
      if (min != null && max != null) {
        addColumnMinMaxValueInfo(properties, column, min.toString(), max.toString());
      }
    }

    String defaultNullValue = columnIndexCreationInfo.getDefaultNullValue().toString();
    if (isValidPropertyValue(defaultNullValue)) {
      properties.setProperty(getKeyFor(column, DEFAULT_NULL_VALUE), defaultNullValue);
    }
  }

  public static void addColumnMinMaxValueInfo(PropertiesConfiguration properties, String column, String minValue,
      String maxValue) {
    if (isValidPropertyValue(minValue)) {
      properties.setProperty(getKeyFor(column, MIN_VALUE), minValue);
    } else {
      properties.setProperty(getKeyFor(column, MIN_MAX_VALUE_INVALID), true);
    }
    if (isValidPropertyValue(maxValue)) {
      properties.setProperty(getKeyFor(column, MAX_VALUE), maxValue);
    } else {
      properties.setProperty(getKeyFor(column, MIN_MAX_VALUE_INVALID), true);
    }
  }

  /**
   * Helper method to check whether the given value is a valid property value.
   * <p>Value is invalid iff:
   * <ul>
   *   <li>It contains more than 512 characters</li>
   *   <li>It contains leading/trailing whitespace</li>
   *   <li>It contains list separator (',')</li>
   * </ul>
   */
  @VisibleForTesting
  static boolean isValidPropertyValue(String value) {
    int length = value.length();
    if (length == 0) {
      return true;
    }
    if (length > METADATA_PROPERTY_LENGTH_LIMIT) {
      return false;
    }
    if (Character.isWhitespace(value.charAt(0)) || Character.isWhitespace(value.charAt(length - 1))) {
      return false;
    }
    return value.indexOf(',') == -1;
  }

  public static void removeColumnMetadataInfo(PropertiesConfiguration properties, String column) {
    properties.subset(COLUMN_PROPS_KEY_PREFIX + column).clear();
  }

  @Override
  public void close()
      throws IOException {
    List<IndexCreator> creators = _creatorsByColAndIndex.values().stream()
        .flatMap(map -> map.values().stream())
        .collect(Collectors.toList());
    creators.addAll(_nullValueVectorCreatorMap.values());
    creators.addAll(_dictionaryCreatorMap.values());
    FileUtils.close(creators);
  }
}
