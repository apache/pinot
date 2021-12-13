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
import com.google.common.collect.Iterables;
import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.pinot.common.utils.FileUtils;
import org.apache.pinot.segment.local.io.util.PinotDataBitSet;
import org.apache.pinot.segment.local.segment.creator.impl.nullvalue.NullValueVectorCreator;
import org.apache.pinot.segment.local.utils.GeometrySerializer;
import org.apache.pinot.segment.spi.V1Constants;
import org.apache.pinot.segment.spi.compression.ChunkCompressionType;
import org.apache.pinot.segment.spi.creator.ColumnIndexCreationInfo;
import org.apache.pinot.segment.spi.creator.IndexCreationContext;
import org.apache.pinot.segment.spi.creator.IndexCreatorProvider;
import org.apache.pinot.segment.spi.creator.SegmentCreator;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
import org.apache.pinot.segment.spi.index.IndexingOverrides;
import org.apache.pinot.segment.spi.index.creator.DictionaryBasedInvertedIndexCreator;
import org.apache.pinot.segment.spi.index.creator.ForwardIndexCreator;
import org.apache.pinot.segment.spi.index.creator.GeoSpatialIndexCreator;
import org.apache.pinot.segment.spi.index.creator.H3IndexConfig;
import org.apache.pinot.segment.spi.index.creator.JsonIndexCreator;
import org.apache.pinot.segment.spi.index.creator.SegmentIndexCreationInfo;
import org.apache.pinot.segment.spi.index.creator.TextIndexCreator;
import org.apache.pinot.segment.spi.partition.PartitionFunction;
import org.apache.pinot.spi.config.table.FieldConfig;
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

import static java.nio.charset.StandardCharsets.UTF_8;
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
  private final IndexCreatorProvider _indexCreatorProvider = IndexingOverrides.getIndexCreatorProvider();
  private final Map<String, SegmentDictionaryCreator> _dictionaryCreatorMap = new HashMap<>();
  private final Map<String, ForwardIndexCreator> _forwardIndexCreatorMap = new HashMap<>();
  private final Map<String, DictionaryBasedInvertedIndexCreator> _invertedIndexCreatorMap = new HashMap<>();
  private final Map<String, TextIndexCreator> _textIndexCreatorMap = new HashMap<>();
  private final Map<String, TextIndexCreator> _fstIndexCreatorMap = new HashMap<>();
  private final Map<String, JsonIndexCreator> _jsonIndexCreatorMap = new HashMap<>();
  private final Map<String, GeoSpatialIndexCreator> _h3IndexCreatorMap = new HashMap<>();
  private final Map<String, NullValueVectorCreator> _nullValueVectorCreatorMap = new HashMap<>();
  private String _segmentName;
  private Schema _schema;
  private File _indexDir;
  private int _totalDocs;
  private int _docIdCounter;
  private boolean _nullHandlingEnabled;
  private Map<String, Map<String, String>> _columnProperties;

  @Override
  public void init(SegmentGeneratorConfig segmentCreationSpec, SegmentIndexCreationInfo segmentIndexCreationInfo,
      Map<String, ColumnIndexCreationInfo> indexCreationInfoMap, Schema schema, File outDir)
      throws Exception {
    _docIdCounter = 0;
    _config = segmentCreationSpec;
    _indexCreationInfoMap = indexCreationInfoMap;
    _columnProperties = segmentCreationSpec.getColumnProperties();

    // Check that the output directory does not exist
    Preconditions.checkState(!outDir.exists(), "Segment output directory: %s already exists", outDir);

    Preconditions.checkState(outDir.mkdirs(), "Failed to create output directory: %s", outDir);
    _indexDir = outDir;

    _schema = schema;
    _totalDocs = segmentIndexCreationInfo.getTotalDocs();
    if (_totalDocs == 0) {
      return;
    }

    Collection<FieldSpec> fieldSpecs = schema.getAllFieldSpecs();
    Set<String> invertedIndexColumns = new HashSet<>();
    for (String columnName : _config.getInvertedIndexCreationColumns()) {
      Preconditions.checkState(schema.hasColumn(columnName),
          "Cannot create inverted index for column: %s because it is not in schema", columnName);
      invertedIndexColumns.add(columnName);
    }

    Set<String> textIndexColumns = new HashSet<>();
    for (String columnName : _config.getTextIndexCreationColumns()) {
      Preconditions.checkState(schema.hasColumn(columnName),
          "Cannot create text index for column: %s because it is not in schema", columnName);
      textIndexColumns.add(columnName);
    }

    Set<String> fstIndexColumns = new HashSet<>();
    for (String columnName : _config.getFSTIndexCreationColumns()) {
      Preconditions.checkState(schema.hasColumn(columnName),
          "Cannot create FST index for column: %s because it is not in schema", columnName);
      fstIndexColumns.add(columnName);
    }

    Set<String> jsonIndexColumns = new HashSet<>();
    for (String columnName : _config.getJsonIndexCreationColumns()) {
      Preconditions.checkState(schema.hasColumn(columnName),
          "Cannot create text index for column: %s because it is not in schema", columnName);
      jsonIndexColumns.add(columnName);
    }

    Map<String, H3IndexConfig> h3IndexConfigs = _config.getH3IndexConfigs();
    for (String columnName : h3IndexConfigs.keySet()) {
      Preconditions.checkState(schema.hasColumn(columnName),
          "Cannot create H3 index for column: %s because it is not in schema", columnName);
    }

    // Initialize creators for dictionary, forward index and inverted index
    for (FieldSpec fieldSpec : fieldSpecs) {
      // Ignore virtual columns
      if (fieldSpec.isVirtualColumn()) {
        continue;
      }

      String columnName = fieldSpec.getName();
      ColumnIndexCreationInfo columnIndexCreationInfo = indexCreationInfoMap.get(columnName);
      Preconditions.checkNotNull(columnIndexCreationInfo, "Missing index creation info for column: %s", columnName);
      boolean dictEnabledColumn = createDictionaryForColumn(columnIndexCreationInfo, segmentCreationSpec, fieldSpec);
      Preconditions.checkState(dictEnabledColumn || !invertedIndexColumns.contains(columnName),
          "Cannot create inverted index for raw index column: %s", columnName);

      IndexCreationContext.Common context = IndexCreationContext.builder()
          .withIndexDir(_indexDir)
          .withCardinality(columnIndexCreationInfo.getDistinctValueCount())
          .withDictionary(dictEnabledColumn)
          .withFieldSpec(fieldSpec)
          .withTotalDocs(segmentIndexCreationInfo.getTotalDocs())
          .withTotalNumberOfEntries(columnIndexCreationInfo.getTotalNumberOfEntries())
          .withColumnIndexCreationInfo(columnIndexCreationInfo)
          .sorted(columnIndexCreationInfo.isSorted())
          .onHeap(segmentCreationSpec.isOnHeap())
          .build();
      // Initialize forward index creator
      ChunkCompressionType chunkCompressionType =
          dictEnabledColumn ? null : getColumnCompressionType(segmentCreationSpec, fieldSpec);
      _forwardIndexCreatorMap.put(columnName, _indexCreatorProvider.newForwardIndexCreator(
          context.forForwardIndex(chunkCompressionType, segmentCreationSpec.getColumnProperties())));

      // Initialize inverted index creator; skip creating inverted index if sorted
      if (invertedIndexColumns.contains(columnName) && !columnIndexCreationInfo.isSorted()) {
        _invertedIndexCreatorMap.put(columnName,
            _indexCreatorProvider.newInvertedIndexCreator(context.forInvertedIndex()));
      }
      if (dictEnabledColumn) {
        // Create dictionary-encoded index
        // Initialize dictionary creator
        SegmentDictionaryCreator dictionaryCreator =
            new SegmentDictionaryCreator(columnIndexCreationInfo.getSortedUniqueElementsArray(), fieldSpec, _indexDir,
                columnIndexCreationInfo.isUseVarLengthDictionary());
        _dictionaryCreatorMap.put(columnName, dictionaryCreator);
        // Create dictionary
        try {
          dictionaryCreator.build();
        } catch (Exception e) {
          LOGGER.error("Error building dictionary for field: {}, cardinality: {}, number of bytes per entry: {}",
              fieldSpec.getName(), columnIndexCreationInfo.getDistinctValueCount(),
              dictionaryCreator.getNumBytesPerEntry());
          throw e;
        }
      }

      if (textIndexColumns.contains(columnName)) {
        _textIndexCreatorMap.put(columnName, _indexCreatorProvider.newTextIndexCreator(context.forTextIndex(true)));
      }

      if (fstIndexColumns.contains(columnName)) {
        _fstIndexCreatorMap.put(columnName, _indexCreatorProvider.newTextIndexCreator(
            context.forFSTIndex(_config.getFSTIndexType(),
                (String[]) columnIndexCreationInfo.getSortedUniqueElementsArray())));
      }

      if (jsonIndexColumns.contains(columnName)) {
        _jsonIndexCreatorMap.put(columnName, _indexCreatorProvider.newJsonIndexCreator(context.forJsonIndex()));
      }

      H3IndexConfig h3IndexConfig = h3IndexConfigs.get(columnName);
      if (h3IndexConfig != null) {
        _h3IndexCreatorMap.put(columnName,
            _indexCreatorProvider.newGeoSpatialIndexCreator(context.forGeospatialIndex(h3IndexConfig)));
      }

      _nullHandlingEnabled = _config.isNullHandlingEnabled();
      if (_nullHandlingEnabled) {
        // Initialize Null value vector map
        _nullValueVectorCreatorMap.put(columnName, new NullValueVectorCreator(_indexDir, columnName));
      }
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
    String column = spec.getName();
    if (config.getRawIndexCreationColumns().contains(column) || config.getRawIndexCompressionType()
        .containsKey(column)) {
      return false;
    }
    return info.isCreateDictionary();
  }

  /**
   * Helper method that returns compression type to use based on segment creation spec and field type.
   * <ul>
   *   <li> Returns compression type from segment creation spec, if specified there.</li>
   *   <li> Else, returns PASS_THROUGH for metrics, and SNAPPY for dimensions. This is because metrics are likely
   *        to be spread in different chunks after applying predicates. Same could be true for dimensions, but in that
   *        case, clients are expected to explicitly specify the appropriate compression type in the spec. </li>
   * </ul>
   * @param segmentCreationSpec Segment creation spec
   * @param fieldSpec Field spec for the column
   * @return Compression type to use
   */
  private ChunkCompressionType getColumnCompressionType(SegmentGeneratorConfig segmentCreationSpec,
      FieldSpec fieldSpec) {
    ChunkCompressionType compressionType = segmentCreationSpec.getRawIndexCompressionType().get(fieldSpec.getName());
    if (compressionType == null) {
      if (fieldSpec.getFieldType() == FieldSpec.FieldType.METRIC) {
        return ChunkCompressionType.PASS_THROUGH;
      } else {
        return ChunkCompressionType.LZ4;
      }
    } else {
      return compressionType;
    }
  }

  @Override
  public void indexRow(GenericRow row)
      throws IOException {
    for (Map.Entry<String, ForwardIndexCreator> entry : _forwardIndexCreatorMap.entrySet()) {
      String columnName = entry.getKey();
      ForwardIndexCreator forwardIndexCreator = entry.getValue();

      Object columnValueToIndex = row.getValue(columnName);
      if (columnValueToIndex == null) {
        throw new RuntimeException("Null value for column:" + columnName);
      }

      FieldSpec fieldSpec = _schema.getFieldSpecFor(columnName);

      //get dictionaryCreator, will be null if column is not dictionaryEncoded
      SegmentDictionaryCreator dictionaryCreator = _dictionaryCreatorMap.get(columnName);

      // text-index
      TextIndexCreator textIndexCreator = _textIndexCreatorMap.get(columnName);
      if (textIndexCreator != null) {
        if (fieldSpec.isSingleValueField()) {
          textIndexCreator.add((String) columnValueToIndex);
        } else {
          Object[] values = (Object[]) columnValueToIndex;
          int length = values.length;
          if (values instanceof String[]) {
            textIndexCreator.add((String[]) values, length);
          } else {
            String[] strings = new String[length];
            for (int i = 0; i < length; i++) {
              strings[i] = (String) values[i];
            }
            textIndexCreator.add(strings, length);
            columnValueToIndex = strings;
          }
        }
      }

      if (fieldSpec.isSingleValueField()) {
        // Single Value column
        JsonIndexCreator jsonIndexCreator = _jsonIndexCreatorMap.get(columnName);
        if (jsonIndexCreator != null) {
          jsonIndexCreator.add((String) columnValueToIndex);
        }
        GeoSpatialIndexCreator h3IndexCreator = _h3IndexCreatorMap.get(columnName);
        if (h3IndexCreator != null) {
          h3IndexCreator.add(GeometrySerializer.deserialize((byte[]) columnValueToIndex));
        }
        if (dictionaryCreator != null) {
          // dictionary encoded SV column
          // get dictID from dictionary
          int dictId = dictionaryCreator.indexOfSV(columnValueToIndex);
          // store the docID -> dictID mapping in forward index
          forwardIndexCreator.putDictId(dictId);
          DictionaryBasedInvertedIndexCreator invertedIndexCreator = _invertedIndexCreatorMap.get(columnName);
          if (invertedIndexCreator != null) {
            // if inverted index enabled during segment creation,
            // then store dictID -> docID mapping in inverted index
            invertedIndexCreator.add(dictId);
          }
        } else {
          // non-dictionary encoded SV column
          // store the docId -> raw value mapping in forward index
          if (textIndexCreator != null && !shouldStoreRawValueForTextIndex(columnName)) {
            // for text index on raw columns, check the config to determine if actual raw value should
            // be stored or not
            columnValueToIndex = _columnProperties.get(columnName).get(FieldConfig.TEXT_INDEX_RAW_VALUE);
            if (columnValueToIndex == null) {
              columnValueToIndex = FieldConfig.TEXT_INDEX_DEFAULT_RAW_VALUE;
            }
          }
          switch (forwardIndexCreator.getValueType()) {
            case INT:
              forwardIndexCreator.putInt((int) columnValueToIndex);
              break;
            case LONG:
              forwardIndexCreator.putLong((long) columnValueToIndex);
              break;
            case FLOAT:
              forwardIndexCreator.putFloat((float) columnValueToIndex);
              break;
            case DOUBLE:
              forwardIndexCreator.putDouble((double) columnValueToIndex);
              break;
            case STRING:
              forwardIndexCreator.putString((String) columnValueToIndex);
              break;
            case BYTES:
              forwardIndexCreator.putBytes((byte[]) columnValueToIndex);
              break;
            default:
              throw new IllegalStateException();
          }
        }
      } else {
        if (dictionaryCreator != null) {
          //dictionary encoded
          int[] dictIds = dictionaryCreator.indexOfMV(columnValueToIndex);
          forwardIndexCreator.putDictIdMV(dictIds);
          DictionaryBasedInvertedIndexCreator invertedIndexCreator = _invertedIndexCreatorMap.get(columnName);
          if (invertedIndexCreator != null) {
            invertedIndexCreator.add(dictIds, dictIds.length);
          }
        } else {
          // for text index on raw columns, check the config to determine if actual raw value should
          // be stored or not
          if (textIndexCreator != null && !shouldStoreRawValueForTextIndex(columnName)) {
            Object value = _columnProperties.get(columnName).get(FieldConfig.TEXT_INDEX_RAW_VALUE);
            if (value == null) {
              value = FieldConfig.TEXT_INDEX_DEFAULT_RAW_VALUE;
            }
            if (forwardIndexCreator.getValueType().getStoredType() == DataType.STRING) {
              columnValueToIndex = new String[]{String.valueOf(value)};
            } else if (forwardIndexCreator.getValueType().getStoredType() == DataType.BYTES) {
              columnValueToIndex = new byte[][]{String.valueOf(value).getBytes(UTF_8)};
            } else {
              throw new RuntimeException("Text Index is only supported for STRING and BYTES stored type");
            }
          }
          Object[] values = (Object[]) columnValueToIndex;
          int length = values.length;
          switch (forwardIndexCreator.getValueType()) {
            case INT:
              int[] ints = new int[length];
              for (int i = 0; i < length; i++) {
                ints[i] = (Integer) values[i];
              }
              forwardIndexCreator.putIntMV(ints);
              break;
            case LONG:
              long[] longs = new long[length];
              for (int i = 0; i < length; i++) {
                longs[i] = (Long) values[i];
              }
              forwardIndexCreator.putLongMV(longs);
              break;
            case FLOAT:
              float[] floats = new float[length];
              for (int i = 0; i < length; i++) {
                floats[i] = (Float) values[i];
              }
              forwardIndexCreator.putFloatMV(floats);
              break;
            case DOUBLE:
              double[] doubles = new double[length];
              for (int i = 0; i < length; i++) {
                doubles[i] = (Double) values[i];
              }
              forwardIndexCreator.putDoubleMV(doubles);
              break;
            case STRING:
              if (values instanceof String[]) {
                forwardIndexCreator.putStringMV((String[]) values);
              } else {
                String[] strings = new String[length];
                for (int i = 0; i < length; i++) {
                  strings[i] = (String) values[i];
                }
                forwardIndexCreator.putStringMV(strings);
              }
              break;
            case BYTES:
              if (values instanceof byte[][]) {
                forwardIndexCreator.putBytesMV((byte[][]) values);
              } else {
                byte[][] bytesArray = new byte[length][];
                for (int i = 0; i < length; i++) {
                  bytesArray[i] = (byte[]) values[i];
                }
                forwardIndexCreator.putBytesMV(bytesArray);
              }
              break;
            default:
              throw new IllegalStateException();
          }
        }
      }

      if (_nullHandlingEnabled) {
        // If row has null value for given column name, add to null value vector
        if (row.isNullValue(columnName)) {
          _nullValueVectorCreatorMap.get(columnName).setNull(_docIdCounter);
        }
      }
    }
    _docIdCounter++;
  }

  private boolean shouldStoreRawValueForTextIndex(String column) {
    if (_columnProperties != null) {
      Map<String, String> props = _columnProperties.get(column);
      // by default always store the raw value
      // if the config is set to true, don't store the actual raw value
      // there will be a dummy value
      return props == null || !Boolean.parseBoolean(props.get(FieldConfig.TEXT_INDEX_NO_RAW_DATA));
    }

    return true;
  }

  @Override
  public void setSegmentName(String segmentName) {
    _segmentName = segmentName;
  }

  @Override
  public void seal()
      throws ConfigurationException, IOException {
    for (DictionaryBasedInvertedIndexCreator invertedIndexCreator : _invertedIndexCreatorMap.values()) {
      invertedIndexCreator.seal();
    }
    for (TextIndexCreator textIndexCreator : _textIndexCreatorMap.values()) {
      textIndexCreator.seal();
    }
    for (TextIndexCreator fstIndexCreator : _fstIndexCreatorMap.values()) {
      fstIndexCreator.seal();
    }
    for (JsonIndexCreator jsonIndexCreator : _jsonIndexCreatorMap.values()) {
      jsonIndexCreator.seal();
    }
    for (GeoSpatialIndexCreator h3IndexCreator : _h3IndexCreatorMap.values()) {
      h3IndexCreator.seal();
    }
    for (NullValueVectorCreator nullValueVectorCreator : _nullValueVectorCreatorMap.values()) {
      nullValueVectorCreator.seal();
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
          _dictionaryCreatorMap.containsKey(column), dictionaryElementSize);
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
    }
    if (isValidPropertyValue(maxValue)) {
      properties.setProperty(getKeyFor(column, MAX_VALUE), maxValue);
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
    FileUtils.close(Iterables.concat(_dictionaryCreatorMap.values(), _forwardIndexCreatorMap.values(),
        _invertedIndexCreatorMap.values(), _textIndexCreatorMap.values(), _fstIndexCreatorMap.values(),
        _jsonIndexCreatorMap.values(), _h3IndexCreatorMap.values(), _nullValueVectorCreatorMap.values()));
  }
}
