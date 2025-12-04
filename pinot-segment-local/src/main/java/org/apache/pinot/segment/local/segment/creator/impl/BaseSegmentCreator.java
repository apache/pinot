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

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import java.io.Closeable;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.configuration2.PropertiesConfiguration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.common.metrics.MinionMeter;
import org.apache.pinot.common.metrics.MinionMetrics;
import org.apache.pinot.common.metrics.ServerMeter;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.segment.local.segment.creator.impl.nullvalue.NullValueVectorCreator;
import org.apache.pinot.segment.local.segment.index.converter.SegmentFormatConverterFactory;
import org.apache.pinot.segment.local.segment.index.dictionary.DictionaryIndexPlugin;
import org.apache.pinot.segment.local.segment.index.dictionary.DictionaryIndexType;
import org.apache.pinot.segment.local.segment.index.loader.IndexLoadingConfig;
import org.apache.pinot.segment.local.segment.index.loader.invertedindex.MultiColumnTextIndexHandler;
import org.apache.pinot.segment.local.startree.v2.builder.MultipleTreesBuilder;
import org.apache.pinot.segment.local.utils.CrcUtils;
import org.apache.pinot.segment.spi.V1Constants;
import org.apache.pinot.segment.spi.converter.SegmentFormatConverter;
import org.apache.pinot.segment.spi.creator.ColumnIndexCreationInfo;
import org.apache.pinot.segment.spi.creator.ColumnStatistics;
import org.apache.pinot.segment.spi.creator.IndexCreationContext;
import org.apache.pinot.segment.spi.creator.SegmentCreator;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
import org.apache.pinot.segment.spi.creator.SegmentVersion;
import org.apache.pinot.segment.spi.index.DictionaryIndexConfig;
import org.apache.pinot.segment.spi.index.FieldIndexConfigs;
import org.apache.pinot.segment.spi.index.ForwardIndexConfig;
import org.apache.pinot.segment.spi.index.IndexCreator;
import org.apache.pinot.segment.spi.index.IndexHandler;
import org.apache.pinot.segment.spi.index.IndexService;
import org.apache.pinot.segment.spi.index.IndexType;
import org.apache.pinot.segment.spi.index.StandardIndexes;
import org.apache.pinot.segment.spi.index.creator.ForwardIndexCreator;
import org.apache.pinot.segment.spi.index.creator.SegmentIndexCreationInfo;
import org.apache.pinot.segment.spi.loader.SegmentDirectoryLoaderContext;
import org.apache.pinot.segment.spi.loader.SegmentDirectoryLoaderRegistry;
import org.apache.pinot.segment.spi.store.SegmentDirectory;
import org.apache.pinot.segment.spi.store.SegmentDirectoryPaths;
import org.apache.pinot.spi.config.instance.InstanceType;
import org.apache.pinot.spi.config.table.IndexConfig;
import org.apache.pinot.spi.config.table.SegmentZKPropsConfig;
import org.apache.pinot.spi.config.table.StarTreeIndexConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.ComplexFieldSpec;
import org.apache.pinot.spi.data.DateTimeFieldSpec;
import org.apache.pinot.spi.data.DateTimeFormatSpec;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.FieldSpec.FieldType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.env.CommonsConfigurationUtils;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.ReadMode;
import org.apache.pinot.spi.utils.TimeUtils;
import org.joda.time.DateTimeZone;
import org.joda.time.Interval;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.pinot.segment.spi.V1Constants.MetadataKeys.Column.*;
import static org.apache.pinot.segment.spi.V1Constants.MetadataKeys.Segment.*;


/**
 * Abstract base class for segment creators containing common functionality and metadata handling.
 */
public abstract class BaseSegmentCreator implements SegmentCreator {
  private static final Logger LOGGER = LoggerFactory.getLogger(BaseSegmentCreator.class);
  // Allow at most 512 characters for the metadata property
  private static final int METADATA_PROPERTY_LENGTH_LIMIT = 512;

  protected Schema _schema;
  protected Map<String, ColumnIndexCreators> _colIndexes;
  protected NavigableMap<String, ColumnIndexCreationInfo> _indexCreationInfoMap;

  private int _totalDocs;
  private SegmentGeneratorConfig _config;
  private String _segmentName;
  private File _indexDir;

  /**
   * Common initialization logic for setting up directory and basic fields.
   */
  protected void initializeCommon(SegmentGeneratorConfig segmentCreationSpec, SegmentIndexCreationInfo creationInfo,
      NavigableMap<String, ColumnIndexCreationInfo> indexCreationInfoMap, Schema schema, File outDir,
      Map<String, ColumnIndexCreators> colIndexes, @Nullable int[] immutableToMutableIdMap)
      throws Exception {
    // Check that the output directory does not exist
    Preconditions.checkState(!outDir.exists(), "Segment output directory: %s already exists", outDir);
    Preconditions.checkState(outDir.mkdirs(), "Failed to create output directory: %s", outDir);

    _config = segmentCreationSpec;
    _colIndexes = colIndexes;
    _indexCreationInfoMap = indexCreationInfoMap;
    _indexDir = outDir;
    _schema = schema;
    _totalDocs = creationInfo.getTotalDocs();

    initColSegmentCreationInfo(immutableToMutableIdMap);
  }

  private void initColSegmentCreationInfo(@Nullable int[] immutableToMutableIdMap)
      throws Exception {
    Map<String, FieldIndexConfigs> indexConfigs = _config.getIndexConfigsByColName();
    for (String columnName : indexConfigs.keySet()) {
      if (canColumnBeIndexed(columnName) && _totalDocs > 0 && _indexCreationInfoMap.containsKey(columnName)) {
        ColumnIndexCreators result = createColIndexeCreators(columnName, immutableToMutableIdMap);
        _colIndexes.put(columnName, result);
      } else {
        FieldSpec fieldSpec = _schema.getFieldSpecFor(columnName);
        _colIndexes.put(columnName,
            new ColumnIndexCreators(columnName, fieldSpec, null, List.of(), null));
      }
    }
  }

  /**
   * Initializes a single column's dictionary and index creators.
   * This encapsulates the common logic shared between different segment creator implementations.
   */
  protected ColumnIndexCreators createColIndexeCreators(String columnName,
      @Nullable int[] immutableToMutableIdMap)
      throws Exception {
    FieldSpec fieldSpec = _schema.getFieldSpecFor(columnName);

    FieldIndexConfigs originalConfig = _config.getIndexConfigsByColName().get(columnName);
    ColumnIndexCreationInfo columnIndexCreationInfo = _indexCreationInfoMap.get(columnName);
    Preconditions.checkNotNull(columnIndexCreationInfo, "Missing index creation info for column: %s", columnName);

    boolean dictEnabledColumn = createDictionaryForColumn(columnIndexCreationInfo, _config, fieldSpec);
    if (originalConfig.getConfig(StandardIndexes.inverted()).isEnabled()) {
      Preconditions.checkState(dictEnabledColumn,
          "Cannot create inverted index for raw index column: %s", columnName);
    }
    IndexCreationContext.Common context =
        getIndexCreationContext(fieldSpec, dictEnabledColumn, immutableToMutableIdMap);

    FieldIndexConfigs config = adaptConfig(columnName, originalConfig, columnIndexCreationInfo, _config);

    SegmentDictionaryCreator dictionaryCreator = null;
    if (dictEnabledColumn) {
      dictionaryCreator = getDictionaryCreator(columnName, originalConfig, context);
    }

    List<IndexCreator> indexCreators = getIndexCreatorsByColumn(fieldSpec, context, config, dictEnabledColumn);

    return new ColumnIndexCreators(columnName, fieldSpec, dictionaryCreator,
        indexCreators, getNullValueCreator(fieldSpec));
  }

  private IndexCreationContext.Common getIndexCreationContext(FieldSpec fieldSpec, boolean dictEnabledColumn,
      @Nullable int[] immutableToMutableIdMap) {
    ColumnIndexCreationInfo columnIndexCreationInfo = _indexCreationInfoMap.get(fieldSpec.getName());
    FieldIndexConfigs fieldIndexConfig = _config.getIndexConfigsByColName().get(fieldSpec.getName());
    boolean forwardIndexDisabled = !fieldIndexConfig.getConfig(StandardIndexes.forward()).isEnabled();

    return IndexCreationContext.builder()
        .withIndexDir(_indexDir)
        .withDictionary(dictEnabledColumn)
        .withFieldSpec(fieldSpec)
        .withTotalDocs(_totalDocs)
        .withColumnIndexCreationInfo(columnIndexCreationInfo)
        .withOptimizedDictionary(_config.isOptimizeDictionary()
            || _config.isOptimizeDictionaryForMetrics() && fieldSpec.getFieldType() == FieldSpec.FieldType.METRIC)
        .onHeap(_config.isOnHeap())
        .withForwardIndexDisabled(forwardIndexDisabled)
        .withTextCommitOnClose(true)
        .withImmutableToMutableIdMap(immutableToMutableIdMap)
        .withRealtimeConversion(_config.isRealtimeConversion())
        .withConsumerDir(_config.getConsumerDir())
        .withTableNameWithType(_config.getTableConfig().getTableName())
        .withContinueOnError(_config.isContinueOnError())
        .build();
  }

  private SegmentDictionaryCreator getDictionaryCreator(String columnName, FieldIndexConfigs config,
      IndexCreationContext.Common context)
      throws IOException {
    ColumnIndexCreationInfo columnIndexCreationInfo = _indexCreationInfoMap.get(columnName);

    // Create dictionary-encoded index
    // Initialize dictionary creator
    // TODO: Dictionary creator holds all unique values on heap. Consider keeping dictionary instead of creator
    //       which uses off-heap memory.
    DictionaryIndexConfig dictConfig = config.getConfig(StandardIndexes.dictionary());
    if (!dictConfig.isEnabled()) {
      LOGGER.info("Creating dictionary index in column {}.{} even when it is disabled in config",
          _config.getTableName(), columnName);
    }

    // override dictionary type if configured to do so
    if (_config.isOptimizeDictionaryType()) {
      LOGGER.info("Overriding dictionary type for column: {} using var-length dictionary: {}", columnName,
          columnIndexCreationInfo.isUseVarLengthDictionary());
      dictConfig = new DictionaryIndexConfig(dictConfig, columnIndexCreationInfo.isUseVarLengthDictionary());
    }

    SegmentDictionaryCreator dictionaryCreator =
        new DictionaryIndexPlugin().getIndexType().createIndexCreator(context, dictConfig);

    try {
      dictionaryCreator.build(context.getSortedUniqueElementsArray());
    } catch (Exception e) {
      LOGGER.error("Error building dictionary for field: {}, cardinality: {}, number of bytes per entry: {}",
          context.getFieldSpec().getName(), context.getCardinality(), dictionaryCreator.getNumBytesPerEntry());
      throw e;
    }
    return dictionaryCreator;
  }

  private List<IndexCreator> getIndexCreatorsByColumn(FieldSpec fieldSpec, IndexCreationContext.Common context,
      FieldIndexConfigs config, boolean dictEnabledColumn)
      throws Exception {
    Map<IndexType<?, ?, ?>, IndexCreator> creatorsByIndex =
        Maps.newHashMapWithExpectedSize(IndexService.getInstance().getAllIndexes().size());
    for (IndexType<?, ?, ?> index : IndexService.getInstance().getAllIndexes()) {
      if (index.getIndexBuildLifecycle() != IndexType.BuildLifecycle.DURING_SEGMENT_CREATION) {
        continue;
      }
      tryCreateIndexCreator(creatorsByIndex, index, context, config);
    }

    // TODO: Remove this when values stored as ForwardIndex stop depending on TextIndex config
    IndexCreator oldFwdCreator = creatorsByIndex.get(StandardIndexes.forward());
    if (oldFwdCreator != null) {
      Object fakeForwardValue = calculateRawValueForTextIndex(dictEnabledColumn, config, fieldSpec);
      if (fakeForwardValue != null) {
        ForwardIndexCreator castedOldFwdCreator = (ForwardIndexCreator) oldFwdCreator;
        SameValueForwardIndexCreator fakeValueFwdCreator =
            new SameValueForwardIndexCreator(fakeForwardValue, castedOldFwdCreator);
        creatorsByIndex.put(StandardIndexes.forward(), fakeValueFwdCreator);
      }
    }
    return new ArrayList<>(creatorsByIndex.values());
  }

  private NullValueVectorCreator getNullValueCreator(FieldSpec fieldSpec) {
    // Although NullValueVector is implemented as an index, it needs to be treated in a different way than other indexes
    String columnName = fieldSpec.getName();
    if (isNullable(fieldSpec)) {
      // Initialize Null value vector map
      LOGGER.info("Column: {} is nullable", columnName);
      return new NullValueVectorCreator(_indexDir, columnName);
    } else {
      LOGGER.info("Column: {} is not nullable", columnName);
      return null;
    }
  }

  protected boolean canColumnBeIndexed(String columnName) {
    FieldSpec fieldSpec = _schema.getFieldSpecFor(columnName);
    Preconditions.checkState(fieldSpec != null, "Failed to find column: %s in the schema", columnName);
    if (fieldSpec.isVirtualColumn()) {
      LOGGER.warn("Ignoring index creation for virtual column {}", columnName);
      return false;
    }
    return true;
  }

  /**
   * Checks if a field is nullable based on schema and config settings.
   */
  private boolean isNullable(FieldSpec fieldSpec) {
    return _schema.isEnableColumnBasedNullHandling() ? fieldSpec.isNullable() : _config.isDefaultNullHandlingEnabled();
  }

  /**
   * Adapts field index configs based on column properties.
   */
  private FieldIndexConfigs adaptConfig(String columnName, FieldIndexConfigs config,
      ColumnIndexCreationInfo columnIndexCreationInfo, SegmentGeneratorConfig segmentCreationSpec) {
    FieldIndexConfigs.Builder builder = new FieldIndexConfigs.Builder(config);
    // Sorted columns treat the 'forwardIndexDisabled' flag as a no-op
    ForwardIndexConfig fwdConfig = config.getConfig(StandardIndexes.forward());
    if (!fwdConfig.isEnabled() && columnIndexCreationInfo.isSorted()) {
      builder.add(StandardIndexes.forward(),
          new ForwardIndexConfig.Builder(fwdConfig).withLegacyProperties(segmentCreationSpec.getColumnProperties(),
              columnName).build());
    }
    // Initialize inverted index creator; skip creating inverted index if sorted
    if (columnIndexCreationInfo.isSorted()) {
      builder.undeclare(StandardIndexes.inverted());
    }
    return builder.build();
  }

  /**
   * Creates the {@link IndexCreator} in a type safe way.
   *
   * This code needs to be in a specific method instead of inlined in the main loop in order to be able to use the
   * limited generic capabilities of Java.
   */
  private <C extends IndexConfig> void tryCreateIndexCreator(Map<IndexType<?, ?, ?>, IndexCreator> creatorsByIndex,
      IndexType<C, ?, ?> index, IndexCreationContext.Common context, FieldIndexConfigs fieldIndexConfigs)
      throws Exception {
    C config = fieldIndexConfigs.getConfig(index);
    if (config.isEnabled()) {
      creatorsByIndex.put(index, index.createIndexCreator(context, config));
    }
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
    if (spec instanceof ComplexFieldSpec) {
      return false;
    }

    String column = spec.getName();
    FieldIndexConfigs fieldIndexConfigs = config.getIndexConfigsByColName().get(column);
    if (fieldIndexConfigs.getConfig(StandardIndexes.dictionary()).isDisabled()) {
      return false;
    }

    return DictionaryIndexType.ignoreDictionaryOverride(config.isOptimizeDictionary(),
        config.isOptimizeDictionaryForMetrics(), config.getNoDictionarySizeRatioThreshold(),
        config.getNoDictionaryCardinalityRatioThreshold(), spec, fieldIndexConfigs, info.getDistinctValueCount(),
        info.getTotalNumberOfEntries());
  }

  /**
   * Calculates the raw value to be used for text index when forward index is disabled.
   */
  @Nullable
  private Object calculateRawValueForTextIndex(boolean dictEnabledColumn, FieldIndexConfigs configs,
      FieldSpec fieldSpec) {
    if (dictEnabledColumn) {
      return null;
    }
    org.apache.pinot.segment.spi.index.TextIndexConfig textConfig = configs.getConfig(StandardIndexes.text());
    if (!textConfig.isEnabled()) {
      return null;
    }

    Object rawValue = textConfig.getRawValueForTextIndex();

    if (rawValue == null) {
      return null;
    } else if (!fieldSpec.isSingleValueField()) {
      if (fieldSpec.getDataType().getStoredType() == FieldSpec.DataType.STRING) {
        if (!(rawValue instanceof String[])) {
          rawValue = new String[]{String.valueOf(rawValue)};
        }
      } else if (fieldSpec.getDataType().getStoredType() == FieldSpec.DataType.BYTES) {
        if (!(rawValue instanceof String[])) {
          rawValue = new byte[][]{String.valueOf(rawValue).getBytes(StandardCharsets.UTF_8)};
        }
      } else {
        throw new RuntimeException("Text Index is only supported for STRING and BYTES stored type");
      }
    }
    return rawValue;
  }

  /**
   * Writes segment metadata to disk.
   */
  protected void writeMetadata()
      throws ConfigurationException {
    File metadataFile = new File(_indexDir, V1Constants.MetadataKeys.METADATA_FILE_NAME);
    PropertiesConfiguration properties = CommonsConfigurationUtils.fromFile(metadataFile);

    properties.setProperty(SEGMENT_CREATOR_VERSION, _config.getCreatorVersion());
    properties.setProperty(SEGMENT_PADDING_CHARACTER, String.valueOf(V1Constants.Str.DEFAULT_STRING_PAD_CHAR));
    properties.setProperty(SEGMENT_NAME, _segmentName);
    properties.setProperty(TABLE_NAME, _config.getTableName());
    properties.setProperty(DIMENSIONS, _config.getDimensions());
    properties.setProperty(METRICS, _config.getMetrics());
    properties.setProperty(DATETIME_COLUMNS, _config.getDateTimeColumnNames());
    properties.setProperty(COMPLEX_COLUMNS, _config.getComplexColumnNames());
    String timeColumnName = _config.getTimeColumnName();
    properties.setProperty(TIME_COLUMN_NAME, timeColumnName);
    properties.setProperty(SEGMENT_TOTAL_DOCS, String.valueOf(_totalDocs));

    // Write time related metadata (start time, end time, time unit)
    if (timeColumnName != null) {
      ColumnIndexCreationInfo timeColumnIndexCreationInfo = _indexCreationInfoMap.get(timeColumnName);
      if (timeColumnIndexCreationInfo != null) {
        try {
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
        } catch (Exception e) {
          if (!_config.isContinueOnError()) {
            throw e;
          }
          TimeUnit timeUnit;
          long now = System.currentTimeMillis();
          long convertedStartTime;
          long convertedEndTime;
          if (_config.getTimeColumnType() == SegmentGeneratorConfig.TimeColumnType.SIMPLE_DATE) {
            convertedEndTime = now;
            convertedStartTime = TimeUtils.getValidMinTimeMillis();
            timeUnit = TimeUnit.MILLISECONDS;
          } else {
            timeUnit = _config.getSegmentTimeUnit();
            if (timeUnit != null) {
              convertedEndTime = timeUnit.convert(now, TimeUnit.MILLISECONDS);
              convertedStartTime = timeUnit.convert(TimeUtils.getValidMinTimeMillis(), TimeUnit.MILLISECONDS);
            } else {
              // Use millis as the time unit if not able to infer from config
              timeUnit = TimeUnit.MILLISECONDS;
              convertedEndTime = now;
              convertedStartTime = TimeUtils.getValidMinTimeMillis();
            }
          }
          LOGGER.warn(
              "Caught exception while writing time metadata for segment: {}, time column: {}, total docs: {}. "
                  + "Continuing using current time ({}) as the end time, and min valid time ({}) as the start time "
                  + "for the segment (time unit: {}).",
              _segmentName, timeColumnName, _totalDocs, convertedEndTime, convertedStartTime, timeUnit, e);
          properties.setProperty(SEGMENT_START_TIME, convertedStartTime);
          properties.setProperty(SEGMENT_END_TIME, convertedEndTime);
          properties.setProperty(TIME_UNIT, timeUnit);
        }
      }
    }

    for (Map.Entry<String, String> entry : _config.getCustomProperties().entrySet()) {
      properties.setProperty(entry.getKey(), entry.getValue());
    }

    for (Map.Entry<String, ColumnIndexCreationInfo> entry : _indexCreationInfoMap.entrySet()) {
      String column = entry.getKey();
      ColumnIndexCreationInfo columnIndexCreationInfo = entry.getValue();
      SegmentDictionaryCreator dictionaryCreator = _colIndexes.get(column).getDictionaryCreator();
      int dictionaryElementSize = (dictionaryCreator != null) ? dictionaryCreator.getNumBytesPerEntry() : 0;
      addColumnMetadataInfo(properties, column, columnIndexCreationInfo, _totalDocs, _schema.getFieldSpecFor(column),
          dictionaryCreator != null, dictionaryElementSize);
    }

    SegmentZKPropsConfig segmentZKPropsConfig = _config.getSegmentZKPropsConfig();
    if (segmentZKPropsConfig != null) {
      properties.setProperty(Realtime.START_OFFSET, segmentZKPropsConfig.getStartOffset());
      properties.setProperty(Realtime.END_OFFSET, segmentZKPropsConfig.getEndOffset());
    }
    CommonsConfigurationUtils.saveToFile(properties, metadataFile);
  }

  /**
   * Adds column metadata information to the properties configuration.
   */
  public static void addColumnMetadataInfo(PropertiesConfiguration properties, String column,
      ColumnIndexCreationInfo columnIndexCreationInfo, int totalDocs, FieldSpec fieldSpec, boolean hasDictionary,
      int dictionaryElementSize) {
    int cardinality = columnIndexCreationInfo.getDistinctValueCount();
    properties.setProperty(getKeyFor(column, CARDINALITY), String.valueOf(cardinality));
    properties.setProperty(getKeyFor(column, TOTAL_DOCS), String.valueOf(totalDocs));
    DataType dataType = fieldSpec.getDataType();
    properties.setProperty(getKeyFor(column, DATA_TYPE), String.valueOf(dataType));
    // TODO: When the column is raw (no dictionary), we should set BITS_PER_ELEMENT to -1 (invalid). Currently we set
    //       it regardless of whether dictionary is created or not for backward compatibility because
    //       ForwardIndexHandler doesn't update this value when converting a raw column to dictionary encoded.
    //       Consider changing it after releasing 1.5.0.
    //       See https://github.com/apache/pinot/pull/16921 for details
    properties.setProperty(getKeyFor(column, BITS_PER_ELEMENT),
        String.valueOf(org.apache.pinot.segment.local.io.util.PinotDataBitSet.getNumBitsPerValue(cardinality - 1)));
    FieldType fieldType = fieldSpec.getFieldType();
    properties.setProperty(getKeyFor(column, DICTIONARY_ELEMENT_SIZE), String.valueOf(dictionaryElementSize));
    properties.setProperty(getKeyFor(column, COLUMN_TYPE), String.valueOf(fieldType));
    properties.setProperty(getKeyFor(column, IS_SORTED), String.valueOf(columnIndexCreationInfo.isSorted()));
    properties.setProperty(getKeyFor(column, HAS_DICTIONARY), String.valueOf(hasDictionary));
    properties.setProperty(getKeyFor(column, IS_SINGLE_VALUED), String.valueOf(fieldSpec.isSingleValueField()));
    properties.setProperty(getKeyFor(column, MAX_MULTI_VALUE_ELEMENTS),
        String.valueOf(columnIndexCreationInfo.getMaxNumberOfMultiValueElements()));
    properties.setProperty(getKeyFor(column, TOTAL_NUMBER_OF_ENTRIES),
        String.valueOf(columnIndexCreationInfo.getTotalNumberOfEntries()));
    properties.setProperty(getKeyFor(column, IS_AUTO_GENERATED),
        String.valueOf(columnIndexCreationInfo.isAutoGenerated()));
    DataType storedType = dataType.getStoredType();
    if (storedType == DataType.STRING || storedType == DataType.BYTES) {
      properties.setProperty(getKeyFor(column, SCHEMA_MAX_LENGTH), fieldSpec.getEffectiveMaxLength());
      // TODO let's revisit writing effective maxLengthStrategy into metadata, as changing it right now may affect
      //  segment's CRC value
      FieldSpec.MaxLengthExceedStrategy maxLengthStrategy = fieldSpec.getMaxLengthExceedStrategy();
      if (maxLengthStrategy != null) {
        properties.setProperty(getKeyFor(column, SCHEMA_MAX_LENGTH_EXCEED_STRATEGY),
            fieldSpec.getMaxLengthExceedStrategy());
      }
    }

    org.apache.pinot.segment.spi.partition.PartitionFunction partitionFunction =
        columnIndexCreationInfo.getPartitionFunction();
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

    // Datetime field
    if (fieldType == FieldType.DATE_TIME) {
      DateTimeFieldSpec dateTimeFieldSpec = (DateTimeFieldSpec) fieldSpec;
      properties.setProperty(getKeyFor(column, DATETIME_FORMAT), dateTimeFieldSpec.getFormat());
      properties.setProperty(getKeyFor(column, DATETIME_GRANULARITY), dateTimeFieldSpec.getGranularity());
    }

    if (fieldType != FieldType.COMPLEX) {
      // Regular (non-complex) field
      if (totalDocs > 0) {
        Object min = columnIndexCreationInfo.getMin();
        Object max = columnIndexCreationInfo.getMax();
        // NOTE:
        // Min/max could be null for real-time aggregate metrics. We don't directly call addColumnMinMaxValueInfo() to
        // avoid setting MIN_MAX_VALUE_INVALID flag, which will prevent ColumnMinMaxValueGenerator from generating them
        // when loading the segment.
        if (min != null && max != null) {
          addColumnMinMaxValueInfo(properties, column, min, max, storedType);
        }
      }
    } else {
      // Complex field
      ComplexFieldSpec complexFieldSpec = (ComplexFieldSpec) fieldSpec;
      properties.setProperty(getKeyFor(column, COMPLEX_CHILD_FIELD_NAMES),
          new ArrayList<>(complexFieldSpec.getChildFieldSpecs().keySet()));
      for (Map.Entry<String, FieldSpec> entry : complexFieldSpec.getChildFieldSpecs().entrySet()) {
        addFieldSpec(properties, ComplexFieldSpec.getFullChildName(column, entry.getKey()), entry.getValue());
      }
    }

    // TODO: Revisit whether we should set default null value for complex field
    String defaultNullValue = columnIndexCreationInfo.getDefaultNullValue().toString();
    if (storedType == DataType.STRING) {
      // NOTE: Do not limit length of default null value because we need exact value to determine whether the default
      //       null value changes
      defaultNullValue = CommonsConfigurationUtils.replaceSpecialCharacterInPropertyValue(defaultNullValue);
    }
    if (defaultNullValue != null) {
      properties.setProperty(getKeyFor(column, DEFAULT_NULL_VALUE), defaultNullValue);
    }
  }

  /**
   * In order to persist complex field metadata, we need to recursively add child field specs
   * So, each complex field spec will have a property for its child field names and each child field will have its
   * own properties of the detailed field spec.
   * E.g. a COMPLEX type `intMap` of Map<String, Integer> has 2 child fields:
   *   - key in STRING type and value in INT type.
   *   Then we will have the following properties to define a COMPLEX field:
   *     column.intMap.childFieldNames = [key, value]
   *     column.intMap$$key.columnType = DIMENSION
   *     column.intMap$$key.dataType = STRING
   *     column.intMap$$key.isSingleValued = true
   *     column.intMap$$value.columnType = DIMENSION
   *     column.intMap$$value.dataType = INT
   *     column.intMap$$value.isSingleValued = true
   */
  public static void addFieldSpec(PropertiesConfiguration properties, String column, FieldSpec fieldSpec) {
    properties.setProperty(getKeyFor(column, COLUMN_TYPE), String.valueOf(fieldSpec.getFieldType()));
    if (!column.equals(fieldSpec.getName())) {
      properties.setProperty(getKeyFor(column, COLUMN_NAME), String.valueOf(fieldSpec.getName()));
    }
    DataType dataType = fieldSpec.getDataType();
    properties.setProperty(getKeyFor(column, DATA_TYPE), String.valueOf(dataType));
    properties.setProperty(getKeyFor(column, IS_SINGLE_VALUED), String.valueOf(fieldSpec.isSingleValueField()));
    if (dataType.equals(DataType.STRING) || dataType.equals(DataType.BYTES) || dataType.equals(DataType.JSON)) {
      properties.setProperty(getKeyFor(column, SCHEMA_MAX_LENGTH), fieldSpec.getEffectiveMaxLength());
      FieldSpec.MaxLengthExceedStrategy maxLengthExceedStrategy = fieldSpec.getEffectiveMaxLengthExceedStrategy();
      if (maxLengthExceedStrategy != null) {
        properties.setProperty(getKeyFor(column, SCHEMA_MAX_LENGTH_EXCEED_STRATEGY), maxLengthExceedStrategy);
      }
    }

    // datetime field
    if (fieldSpec.getFieldType() == FieldType.DATE_TIME) {
      DateTimeFieldSpec dateTimeFieldSpec = (DateTimeFieldSpec) fieldSpec;
      properties.setProperty(getKeyFor(column, DATETIME_FORMAT), dateTimeFieldSpec.getFormat());
      properties.setProperty(getKeyFor(column, DATETIME_GRANULARITY), dateTimeFieldSpec.getGranularity());
    }

    // complex field
    if (fieldSpec.getFieldType() == FieldType.COMPLEX) {
      ComplexFieldSpec complexFieldSpec = (ComplexFieldSpec) fieldSpec;
      properties.setProperty(getKeyFor(column, COMPLEX_CHILD_FIELD_NAMES),
          new ArrayList<>(complexFieldSpec.getChildFieldSpecs().keySet()));
      for (Map.Entry<String, FieldSpec> entry : complexFieldSpec.getChildFieldSpecs().entrySet()) {
        addFieldSpec(properties, ComplexFieldSpec.getFullChildName(column, entry.getKey()), entry.getValue());
      }
    }
  }

  /**
   * Adds column min/max value information to the properties configuration.
   */
  public static void addColumnMinMaxValueInfo(PropertiesConfiguration properties, String column,
      @Nullable Object minValue, @Nullable Object maxValue, DataType storedType) {
    String validMinValue = minValue != null ? getValidPropertyValue(minValue.toString(), storedType) : null;
    if (validMinValue != null) {
      properties.setProperty(getKeyFor(column, MIN_VALUE), validMinValue);
    }
    String validMaxValue = maxValue != null ? getValidPropertyValue(maxValue.toString(), storedType) : null;
    if (validMaxValue != null) {
      properties.setProperty(getKeyFor(column, MAX_VALUE), validMaxValue);
    }
    if (validMinValue == null && validMaxValue == null) {
      properties.setProperty(getKeyFor(column, MIN_MAX_VALUE_INVALID), true);
    }
  }

  /**
   * Helper method to get the valid value for setting min/max. Returns {@code null} if the value is too long (longer
   * than 512 characters), or is not supported in {@link PropertiesConfiguration}, e.g. contains character with
   * surrogate.
   */
  @Nullable
  private static String getValidPropertyValue(String value, DataType storedType) {
    if (value.length() > METADATA_PROPERTY_LENGTH_LIMIT) {
      return null;
    }
    return storedType == DataType.STRING ? CommonsConfigurationUtils.replaceSpecialCharacterInPropertyValue(value)
        : value;
  }

  /**
   * Removes column metadata information from the properties configuration.
   */
  public static void removeColumnMetadataInfo(PropertiesConfiguration properties, String column) {
    properties.subset(COLUMN_PROPS_KEY_PREFIX + column).clear();
  }

  @Override
  public String getSegmentName() {
    return _segmentName;
  }

  /**
   * Writes the index files to disk.
   */
  abstract void seal() throws Exception;

  @Override
  public File createSegment(@Nullable InstanceType instanceType)
      throws Exception {
    _segmentName = generateSegmentName();
    try {
      // Write the index files to disk
      seal();
    } finally {
      close();
    }
    LOGGER.info("Finished segment seal for: {}", _segmentName);

    // Delete the directory named after the segment name, if it exists
    File outputDir = new File(_config.getOutDir());
    File segmentOutputDir = new File(outputDir, _segmentName);
    if (segmentOutputDir.exists()) {
      FileUtils.deleteDirectory(segmentOutputDir);
    }
    // Move the temporary directory into its final location
    FileUtils.moveDirectory(_indexDir, segmentOutputDir);
    FileUtils.deleteQuietly(_indexDir);

    // Format conversion
    convertFormatIfNecessary(segmentOutputDir);

    // Build indexes if there are documents
    if (_totalDocs > 0) {
      buildStarTreeV2IfNecessary(segmentOutputDir, instanceType);
      buildMultiColumnTextIndex(segmentOutputDir);
    }

    // Update post-creation indexes
    updatePostSegmentCreationIndexes(segmentOutputDir);

    // Persist creation metadata
    persistCreationMeta(segmentOutputDir);

    LOGGER.info("Successfully finalized segment: {}", _segmentName);
    return segmentOutputDir;
  }

  /**
   * Generate segment name based on configuration and statistics.
   * @return Generated segment name
   */
  private String generateSegmentName() {
    ColumnStatistics timeStats = null;
    String timeColumn = _config.getTimeColumnName();
    if (timeColumn != null) {
      timeStats = _indexCreationInfoMap.get(timeColumn).getColumnStatistics();
    }

    if (timeStats != null) {
      if (_totalDocs > 0) {
        return _config.getSegmentNameGenerator()
            .generateSegmentName(_config.getSequenceId(), timeStats.getMinValue(), timeStats.getMaxValue());
      } else {
        // When totalDoc is 0, check whether 'failOnEmptySegment' option is true
        Preconditions.checkArgument(!_config.isFailOnEmptySegment(),
            "Failing the empty segment creation as the option 'failOnEmptySegment' is set to: "
                + _config.isFailOnEmptySegment());
        // Generate a unique name for a segment with no rows
        long now = System.currentTimeMillis();
        return _config.getSegmentNameGenerator().generateSegmentName(_config.getSequenceId(), now, now);
      }
    } else {
      return _config.getSegmentNameGenerator().generateSegmentName(_config.getSequenceId(), null, null);
    }
  }

  // Explanation of why we are using format converter:
  // There are 3 options to correctly generate segments to v3 format
  // 1. Generate v3 directly: This is efficient but v3 index writer needs to know buffer size upfront.
  // Inverted, star and raw indexes don't have the index size upfront. This is also least flexible approach
  // if we add more indexes in the future.
  // 2. Hold data in-memory: One way to work around predeclaring sizes in (1) is to allocate "large" buffer (2GB?)
  // and hold the data in memory and write the buffer at the end. The memory requirement in this case increases linearly
  // with the number of columns. Variation of that is to mmap data to separate files...which is what we are doing here
  // 3. Another option is to generate dictionary and fwd indexes in v3 and generate inverted, star and raw indexes in
  // separate files. Then add those files to v3 index file. This leads to lot of hodgepodge code to
  // handle multiple segment formats.
  // Using converter is similar to option (2), plus it's battle-tested code. We will roll out with
  // this change to keep changes limited. Once we've migrated we can implement approach (1) with option to
  // copy for indexes for which we don't know sizes upfront.
  private void convertFormatIfNecessary(File segmentDirectory)
      throws Exception {
    SegmentVersion versionToGenerate = _config.getSegmentVersion();
    if (versionToGenerate.equals(SegmentVersion.v1)) {
      // v1 by default
      return;
    }
    SegmentFormatConverter converter =
        SegmentFormatConverterFactory.getConverter(SegmentVersion.v1, SegmentVersion.v3);
    converter.convert(segmentDirectory);
  }

  /**
   * Build star-tree V2 index if configured.
   *
   * @param indexDir Segment index directory
   * @param instanceType Instance type for metrics tracking (nullable)
   * @throws Exception If star-tree index building fails
   */
  private void buildStarTreeV2IfNecessary(File indexDir, @Nullable InstanceType instanceType)
      throws Exception {
    List<StarTreeIndexConfig> starTreeIndexConfigs = _config.getStarTreeIndexConfigs();
    boolean enableDefaultStarTree = _config.isEnableDefaultStarTree();
    if (CollectionUtils.isNotEmpty(starTreeIndexConfigs) || enableDefaultStarTree) {
      MultipleTreesBuilder.BuildMode buildMode =
          _config.isOnHeap() ? MultipleTreesBuilder.BuildMode.ON_HEAP : MultipleTreesBuilder.BuildMode.OFF_HEAP;
      MultipleTreesBuilder builder = new MultipleTreesBuilder(starTreeIndexConfigs, enableDefaultStarTree, indexDir,
          buildMode);
      // We don't create the builder using the try-with-resources pattern because builder.close() performs
      // some clean-up steps to roll back the star-tree index to the previous state if it exists. If this goes wrong
      // the star-tree index can be in an inconsistent state. To prevent that, when builder.close() throws an
      // exception we want to propagate that up instead of ignoring it. This can get clunky when using
      // try-with-resources as in this scenario the close() exception will be added to the suppressed exception list
      // rather than thrown as the main exception, even though the original exception thrown on build() is ignored.
      try {
        builder.build();
      } catch (Exception e) {
        String tableNameWithType = _config.getTableConfig().getTableName();
        LOGGER.error("Failed to build star-tree index for table: {}, skipping", tableNameWithType, e);
        // Track metrics only if instance type is provided
        if (instanceType != null) {
          if (instanceType == InstanceType.MINION) {
            MinionMetrics.get().addMeteredTableValue(tableNameWithType, MinionMeter.STAR_TREE_INDEX_BUILD_FAILURES, 1);
          } else {
            ServerMetrics.get().addMeteredTableValue(tableNameWithType, ServerMeter.STAR_TREE_INDEX_BUILD_FAILURES, 1);
          }
        }
      } finally {
        builder.close();
      }
    }
  }

  /**
   * Build multi-column text index if configured.
   */
  private void buildMultiColumnTextIndex(File segmentOutputDir)
      throws Exception {
    if (_config.getMultiColumnTextIndexConfig() != null) {
      PinotConfiguration segmentDirectoryConfigs =
          new PinotConfiguration(Map.of(IndexLoadingConfig.READ_MODE_KEY, ReadMode.mmap));

      TableConfig tableConfig = _config.getTableConfig();
      Schema schema = _config.getSchema();
      SegmentDirectoryLoaderContext segmentLoaderContext =
          new SegmentDirectoryLoaderContext.Builder()
              .setTableConfig(tableConfig)
              .setSchema(schema)
              .setSegmentName(_segmentName)
              .setSegmentDirectoryConfigs(segmentDirectoryConfigs)
              .build();

      IndexLoadingConfig indexLoadingConfig = new IndexLoadingConfig(null, tableConfig, schema);

      try (SegmentDirectory segmentDirectory = SegmentDirectoryLoaderRegistry.getDefaultSegmentDirectoryLoader()
          .load(segmentOutputDir.toURI(), segmentLoaderContext);
          SegmentDirectory.Writer segmentWriter = segmentDirectory.createWriter()) {
        MultiColumnTextIndexHandler handler = new MultiColumnTextIndexHandler(segmentDirectory, indexLoadingConfig,
            _config.getMultiColumnTextIndexConfig());
        handler.updateIndices(segmentWriter);
        handler.postUpdateIndicesCleanup(segmentWriter);
      }
    }
  }

  /**
   * Update indexes that are created post-segment creation.
   */
  private void updatePostSegmentCreationIndexes(File indexDir)
      throws Exception {
    Set<IndexType> postSegCreationIndexes = IndexService.getInstance().getAllIndexes().stream()
        .filter(indexType -> indexType.getIndexBuildLifecycle() == IndexType.BuildLifecycle.POST_SEGMENT_CREATION)
        .collect(Collectors.toSet());

    if (!postSegCreationIndexes.isEmpty()) {
      // Build other indexes
      Map<String, Object> props = new HashMap<>();
      props.put(IndexLoadingConfig.READ_MODE_KEY, ReadMode.mmap);
      PinotConfiguration segmentDirectoryConfigs = new PinotConfiguration(props);

      TableConfig tableConfig = _config.getTableConfig();
      Schema schema = _config.getSchema();
      SegmentDirectoryLoaderContext segmentLoaderContext =
          new SegmentDirectoryLoaderContext.Builder()
              .setTableConfig(tableConfig)
              .setSchema(schema)
              .setSegmentName(_segmentName)
              .setSegmentDirectoryConfigs(segmentDirectoryConfigs)
              .build();

      IndexLoadingConfig indexLoadingConfig = new IndexLoadingConfig(null, tableConfig, schema);

      try (SegmentDirectory segmentDirectory = SegmentDirectoryLoaderRegistry.getDefaultSegmentDirectoryLoader()
          .load(indexDir.toURI(), segmentLoaderContext);
          SegmentDirectory.Writer segmentWriter = segmentDirectory.createWriter()) {
        for (IndexType indexType : postSegCreationIndexes) {
          IndexHandler handler =
              indexType.createIndexHandler(segmentDirectory, indexLoadingConfig.getFieldIndexConfigByColName(), schema,
                  tableConfig);
          handler.updateIndices(segmentWriter);
        }
      }
    }
  }

  /**
   * Compute CRC and creation time, and persist to segment metadata file.
   *
   * @param indexDir Segment index directory
   * @throws IOException If writing metadata fails
   */
  private void persistCreationMeta(File indexDir)
      throws IOException {
    long crc = CrcUtils.forAllFilesInFolder(indexDir).computeCrc();
    long creationTime;
    String creationTimeInConfig = _config.getCreationTime();
    if (creationTimeInConfig != null) {
      try {
        creationTime = Long.parseLong(creationTimeInConfig);
      } catch (Exception e) {
        LOGGER.error("Caught exception while parsing creation time in config, use current time as creation time");
        creationTime = System.currentTimeMillis();
      }
    } else {
      creationTime = System.currentTimeMillis();
    }
    File segmentDir = SegmentDirectoryPaths.findSegmentDirectory(indexDir);
    File creationMetaFile = new File(segmentDir, V1Constants.SEGMENT_CREATION_META);
    try (DataOutputStream output = new DataOutputStream(new FileOutputStream(creationMetaFile))) {
      output.writeLong(crc);
      output.writeLong(creationTime);
    }
  }

  @Override
  public void close()
      throws IOException {
    List<Closeable> creators = new ArrayList<>();
    for (ColumnIndexCreators colIndexes : _colIndexes.values()) {
      if (colIndexes.getDictionaryCreator() != null) {
        creators.add(colIndexes.getDictionaryCreator());
      }
      if (colIndexes.getNullValueVectorCreator() != null) {
        creators.add(colIndexes.getNullValueVectorCreator());
      }
      creators.addAll(colIndexes.getIndexCreators());
    }
    org.apache.pinot.common.utils.FileUtils.close(creators);
  }
}
