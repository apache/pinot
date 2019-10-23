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
package org.apache.pinot.core.segment.creator.impl;

import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.pinot.common.data.DateTimeFieldSpec;
import org.apache.pinot.common.data.FieldSpec;
import org.apache.pinot.common.data.FieldSpec.FieldType;
import org.apache.pinot.common.data.Schema;
import org.apache.pinot.common.data.StarTreeIndexSpec;
import org.apache.pinot.common.utils.BytesUtils;
import org.apache.pinot.common.utils.CommonConstants;
import org.apache.pinot.common.utils.FileUtils;
import org.apache.pinot.common.utils.time.TimeUtils;
import org.apache.pinot.core.data.GenericRow;
import org.apache.pinot.core.data.partition.PartitionFunction;
import org.apache.pinot.core.indexsegment.generator.SegmentGeneratorConfig;
import org.apache.pinot.core.io.compression.ChunkCompressorFactory;
import org.apache.pinot.core.io.util.PinotDataBitSet;
import org.apache.pinot.core.segment.creator.ColumnIndexCreationInfo;
import org.apache.pinot.core.segment.creator.ForwardIndexCreator;
import org.apache.pinot.core.segment.creator.InvertedIndexCreator;
import org.apache.pinot.core.segment.creator.MultiValueForwardIndexCreator;
import org.apache.pinot.core.segment.creator.SegmentCreator;
import org.apache.pinot.core.segment.creator.SegmentIndexCreationInfo;
import org.apache.pinot.core.segment.creator.SingleValueForwardIndexCreator;
import org.apache.pinot.core.segment.creator.SingleValueRawIndexCreator;
import org.apache.pinot.core.segment.creator.impl.fwd.MultiValueUnsortedForwardIndexCreator;
import org.apache.pinot.core.segment.creator.impl.fwd.SingleValueFixedByteRawIndexCreator;
import org.apache.pinot.core.segment.creator.impl.fwd.SingleValueSortedForwardIndexCreator;
import org.apache.pinot.core.segment.creator.impl.fwd.SingleValueUnsortedForwardIndexCreator;
import org.apache.pinot.core.segment.creator.impl.fwd.SingleValueVarByteRawIndexCreator;
import org.apache.pinot.core.segment.creator.impl.inv.OffHeapBitmapInvertedIndexCreator;
import org.apache.pinot.core.segment.creator.impl.inv.OnHeapBitmapInvertedIndexCreator;
import org.apache.pinot.core.segment.creator.impl.presence.PresenceVectorCreator;
import org.apache.pinot.startree.hll.HllConfig;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.roaringbitmap.RoaringBitmap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.pinot.core.segment.creator.impl.V1Constants.MetadataKeys.Column.*;
import static org.apache.pinot.core.segment.creator.impl.V1Constants.MetadataKeys.Segment.*;
import static org.apache.pinot.core.segment.creator.impl.V1Constants.MetadataKeys.StarTree.*;


/**
 * Segment creator which writes data in a columnar form.
 */
// TODO: check resource leaks
public class SegmentColumnarIndexCreator implements SegmentCreator {
  // TODO Refactor class name to match interface name
  private static final Logger LOGGER = LoggerFactory.getLogger(SegmentColumnarIndexCreator.class);
  private SegmentGeneratorConfig config;
  private Map<String, ColumnIndexCreationInfo> indexCreationInfoMap;
  private Map<String, SegmentDictionaryCreator> _dictionaryCreatorMap = new HashMap<>();
  private Map<String, ForwardIndexCreator> _forwardIndexCreatorMap = new HashMap<>();
  private Map<String, InvertedIndexCreator> _invertedIndexCreatorMap = new HashMap<>();
  private Map<String, PresenceVectorCreator> _presenceVectorCreatorMap = new HashMap<>();
  private String segmentName;
  private Schema schema;
  private File _indexDir;
  private int totalDocs;
  private int totalRawDocs;
  private int totalAggDocs;
  private int docIdCounter;
  private boolean _nullHandlingEnabled;

  @Override
  public void init(SegmentGeneratorConfig segmentCreationSpec, SegmentIndexCreationInfo segmentIndexCreationInfo,
      Map<String, ColumnIndexCreationInfo> indexCreationInfoMap, Schema schema, File outDir)
      throws Exception {
    docIdCounter = 0;
    config = segmentCreationSpec;
    this.indexCreationInfoMap = indexCreationInfoMap;

    // Check that the output directory does not exist
    Preconditions.checkState(!outDir.exists(), "Segment output directory: %s already exists", outDir);

    Preconditions.checkState(outDir.mkdirs(), "Failed to create output directory: %s", outDir);
    _indexDir = outDir;

    this.schema = schema;
    this.totalDocs = segmentIndexCreationInfo.getTotalDocs();
    this.totalAggDocs = segmentIndexCreationInfo.getTotalAggDocs();
    this.totalRawDocs = segmentIndexCreationInfo.getTotalRawDocs();

    Collection<FieldSpec> fieldSpecs = schema.getAllFieldSpecs();
    Set<String> invertedIndexColumns = new HashSet<>();
    for (String columnName : config.getInvertedIndexCreationColumns()) {
      Preconditions.checkState(schema.hasColumn(columnName),
          "Cannot create inverted index for column: %s because it is not in schema", columnName);
      invertedIndexColumns.add(columnName);
    }

    // Initialize creators for dictionary, forward index and inverted index
    for (FieldSpec fieldSpec : fieldSpecs) {
      // Ignore virtual columns
      if (fieldSpec.isVirtualColumn()) {
        continue;
      }

      String columnName = fieldSpec.getName();
      ColumnIndexCreationInfo indexCreationInfo = indexCreationInfoMap.get(columnName);
      Preconditions.checkNotNull(indexCreationInfo, "Missing index creation info for column: %s", columnName);

      if (createDictionaryForColumn(indexCreationInfo, segmentCreationSpec, fieldSpec)) {
        // Create dictionary-encoded index

        // Initialize dictionary creator
        SegmentDictionaryCreator dictionaryCreator =
            new SegmentDictionaryCreator(indexCreationInfo.getSortedUniqueElementsArray(), fieldSpec, _indexDir,
                indexCreationInfo.isUseVarLengthDictionary());
        _dictionaryCreatorMap.put(columnName, dictionaryCreator);

        // Create dictionary
        try {
          dictionaryCreator.build();
        } catch (Exception e) {
          LOGGER.error("Error building dictionary for field: {}, cardinality: {}, number of bytes per entry: {}",
              fieldSpec.getName(), indexCreationInfo.getDistinctValueCount(), dictionaryCreator.getNumBytesPerEntry());
          throw e;
        }

        // Initialize forward index creator
        int cardinality = indexCreationInfo.getDistinctValueCount();
        if (fieldSpec.isSingleValueField()) {
          if (indexCreationInfo.isSorted()) {
            _forwardIndexCreatorMap
                .put(columnName, new SingleValueSortedForwardIndexCreator(_indexDir, columnName, cardinality));
          } else {
            _forwardIndexCreatorMap.put(columnName,
                new SingleValueUnsortedForwardIndexCreator(_indexDir, columnName, cardinality, totalDocs));
          }
        } else {
          _forwardIndexCreatorMap.put(columnName,
              new MultiValueUnsortedForwardIndexCreator(_indexDir, columnName, cardinality, totalDocs,
                  indexCreationInfo.getTotalNumberOfEntries()));
        }

        // Initialize inverted index creator; skip creating inverted index if sorted
        if (invertedIndexColumns.contains(columnName) && !indexCreationInfo.isSorted()) {
          if (segmentCreationSpec.isOnHeap()) {
            _invertedIndexCreatorMap
                .put(columnName, new OnHeapBitmapInvertedIndexCreator(_indexDir, columnName, cardinality));
          } else {
            _invertedIndexCreatorMap.put(columnName,
                new OffHeapBitmapInvertedIndexCreator(_indexDir, fieldSpec, cardinality, totalDocs,
                    indexCreationInfo.getTotalNumberOfEntries()));
          }
        }
      } else {
        // Create raw index

        // TODO: add support to multi-value column and inverted index
        Preconditions.checkState(fieldSpec.isSingleValueField(), "Cannot create raw index for multi-value column: %s",
            columnName);
        Preconditions.checkState(!invertedIndexColumns.contains(columnName),
            "Cannot create inverted index for raw index column: %s", columnName);

        ChunkCompressorFactory.CompressionType compressionType =
            getColumnCompressionType(segmentCreationSpec, fieldSpec);

        // Initialize forward index creator
        _forwardIndexCreatorMap.put(columnName,
            getRawIndexCreatorForColumn(_indexDir, compressionType, columnName, fieldSpec.getDataType(), totalDocs,
                indexCreationInfo.getLengthOfLongestEntry()));
      }

      _nullHandlingEnabled = config.isNullHandlingEnabled();

      if (_nullHandlingEnabled) {
        // Initialize Presence vector map
        _presenceVectorCreatorMap.put(columnName, new PresenceVectorCreator(_indexDir, columnName));
      }
    }
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
  private ChunkCompressorFactory.CompressionType getColumnCompressionType(SegmentGeneratorConfig segmentCreationSpec,
      FieldSpec fieldSpec) {
    ChunkCompressorFactory.CompressionType compressionType =
        segmentCreationSpec.getRawIndexCompressionType().get(fieldSpec.getName());

    if (compressionType == null) {
      if (fieldSpec.getFieldType().equals(FieldType.METRIC)) {
        return ChunkCompressorFactory.CompressionType.PASS_THROUGH;
      } else {
        return ChunkCompressorFactory.CompressionType.SNAPPY;
      }
    } else {
      return compressionType;
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
      if (!spec.isSingleValueField()) {
        throw new RuntimeException(
            "Creation of indices without dictionaries is supported for single valued columns only.");
      }
      return false;
    } else if (spec.getDataType().equals(FieldSpec.DataType.BYTES) && !info.isFixedLength()) {
      return false;
    }
    return info.isCreateDictionary();
  }

  @Override
  public void indexRow(GenericRow row) {
    for (String columnName : _forwardIndexCreatorMap.keySet()) {
      Object columnValueToIndex = row.getValue(columnName);
      if (columnValueToIndex == null) {
        throw new RuntimeException("Null value for column:" + columnName);
      }

      SegmentDictionaryCreator dictionaryCreator = _dictionaryCreatorMap.get(columnName);
      if (schema.getFieldSpecFor(columnName).isSingleValueField()) {
        if (dictionaryCreator != null) {
          int dictId = dictionaryCreator.indexOfSV(columnValueToIndex);
          ((SingleValueForwardIndexCreator) _forwardIndexCreatorMap.get(columnName)).index(docIdCounter, dictId);
          if (_invertedIndexCreatorMap.containsKey(columnName)) {
            _invertedIndexCreatorMap.get(columnName).add(dictId);
          }
        } else {
          ((SingleValueRawIndexCreator) _forwardIndexCreatorMap.get(columnName))
              .index(docIdCounter, columnValueToIndex);
        }
      } else {
        int[] dictIds = dictionaryCreator.indexOfMV(columnValueToIndex);
        ((MultiValueForwardIndexCreator) _forwardIndexCreatorMap.get(columnName)).index(docIdCounter, dictIds);
        if (_invertedIndexCreatorMap.containsKey(columnName)) {
          _invertedIndexCreatorMap.get(columnName).add(dictIds, dictIds.length);
        }
      }

      if (_nullHandlingEnabled) {
        // If row has null value for given column name, add to presence vector
        if (row.isNullValue(columnName)) {
          _presenceVectorCreatorMap.get(columnName).setNull(docIdCounter);
        }
      }
    }
    docIdCounter++;
  }

  @Override
  public void setSegmentName(String segmentName) {
    this.segmentName = segmentName;
  }

  @Override
  public void seal()
      throws ConfigurationException, IOException {
    for (InvertedIndexCreator invertedIndexCreator : _invertedIndexCreatorMap.values()) {
      invertedIndexCreator.seal();
    }
    writeMetadata();
  }

  void writeMetadata()
      throws ConfigurationException {
    PropertiesConfiguration properties =
        new PropertiesConfiguration(new File(_indexDir, V1Constants.MetadataKeys.METADATA_FILE_NAME));

    properties.setProperty(SEGMENT_CREATOR_VERSION, config.getCreatorVersion());
    properties.setProperty(SEGMENT_PADDING_CHARACTER, String.valueOf(V1Constants.Str.DEFAULT_STRING_PAD_CHAR));
    properties.setProperty(SEGMENT_NAME, segmentName);
    properties.setProperty(TABLE_NAME, config.getTableName());
    properties.setProperty(DIMENSIONS, config.getDimensions());
    properties.setProperty(METRICS, config.getMetrics());
    properties.setProperty(DATETIME_COLUMNS, config.getDateTimeColumnNames());
    properties.setProperty(TIME_COLUMN_NAME, config.getTimeColumnName());
    properties.setProperty(SEGMENT_TOTAL_RAW_DOCS, String.valueOf(totalRawDocs));
    properties.setProperty(SEGMENT_TOTAL_AGGREGATE_DOCS, String.valueOf(totalAggDocs));
    properties.setProperty(SEGMENT_TOTAL_DOCS, String.valueOf(totalDocs));
    properties.setProperty(STAR_TREE_ENABLED, String.valueOf(config.isEnableStarTreeIndex()));

    StarTreeIndexSpec starTreeIndexSpec = config.getStarTreeIndexSpec();
    if (starTreeIndexSpec != null) {
      properties.setProperty(STAR_TREE_SPLIT_ORDER, starTreeIndexSpec.getDimensionsSplitOrder());
      properties.setProperty(STAR_TREE_MAX_LEAF_RECORDS, starTreeIndexSpec.getMaxLeafRecords());
      properties.setProperty(STAR_TREE_SKIP_STAR_NODE_CREATION_FOR_DIMENSIONS,
          starTreeIndexSpec.getSkipStarNodeCreationForDimensions());
      properties.setProperty(STAR_TREE_SKIP_MATERIALIZATION_CARDINALITY,
          starTreeIndexSpec.getSkipMaterializationCardinalityThreshold());
      properties.setProperty(STAR_TREE_SKIP_MATERIALIZATION_FOR_DIMENSIONS,
          starTreeIndexSpec.getSkipMaterializationForDimensions());
    }

    HllConfig hllConfig = config.getHllConfig();
    Map<String, String> derivedHllFieldToOriginMap = null;
    if (hllConfig != null) {
      properties.setProperty(SEGMENT_HLL_LOG2M, hllConfig.getHllLog2m());
      derivedHllFieldToOriginMap = hllConfig.getDerivedHllFieldToOriginMap();
    }

    // Write time related metadata (start time, end time, time unit)
    String timeColumn = config.getTimeColumnName();
    ColumnIndexCreationInfo timeColumnIndexCreationInfo = indexCreationInfoMap.get(timeColumn);
    if (timeColumnIndexCreationInfo != null) {
      // Use start/end time in config if defined
      if (config.getStartTime() != null) {
        checkTime(config, config.getStartTime(), config.getEndTime(), segmentName);
        properties.setProperty(SEGMENT_START_TIME, config.getStartTime());
        properties.setProperty(SEGMENT_END_TIME, Preconditions.checkNotNull(config.getEndTime()));
        properties.setProperty(TIME_UNIT, Preconditions.checkNotNull(config.getSegmentTimeUnit()));
      } else {
        Object minTime = Preconditions.checkNotNull(timeColumnIndexCreationInfo.getMin());
        Object maxTime = Preconditions.checkNotNull(timeColumnIndexCreationInfo.getMax());

        if (config.getTimeColumnType() == SegmentGeneratorConfig.TimeColumnType.SIMPLE_DATE) {
          // For TimeColumnType.SIMPLE_DATE_FORMAT, convert time value into millis since epoch
          DateTimeFormatter dateTimeFormatter = DateTimeFormat.forPattern(config.getSimpleDateFormat());
          final long minTimeMillis = dateTimeFormatter.parseMillis(minTime.toString());
          final long maxTimeMillis = dateTimeFormatter.parseMillis(maxTime.toString());
          checkTime(config, minTimeMillis, maxTimeMillis, segmentName);
          properties.setProperty(SEGMENT_START_TIME, minTimeMillis);
          properties.setProperty(SEGMENT_END_TIME, maxTimeMillis);
          properties.setProperty(TIME_UNIT, TimeUnit.MILLISECONDS);
        } else {
          // by default, time column type is TimeColumnType.EPOCH
          checkTime(config, minTime, maxTime, segmentName);
          properties.setProperty(SEGMENT_START_TIME, minTime);
          properties.setProperty(SEGMENT_END_TIME, maxTime);
          properties.setProperty(TIME_UNIT, Preconditions.checkNotNull(config.getSegmentTimeUnit()));
        }
      }
    }

    for (Map.Entry<String, String> entry : config.getCustomProperties().entrySet()) {
      properties.setProperty(entry.getKey(), entry.getValue());
    }

    for (Map.Entry<String, ColumnIndexCreationInfo> entry : indexCreationInfoMap.entrySet()) {
      String column = entry.getKey();
      ColumnIndexCreationInfo columnIndexCreationInfo = entry.getValue();
      SegmentDictionaryCreator dictionaryCreator = _dictionaryCreatorMap.get(column);
      int dictionaryElementSize = (dictionaryCreator != null) ? dictionaryCreator.getNumBytesPerEntry() : 0;

      // TODO: after fixing the server-side dependency on HAS_INVERTED_INDEX and deployed, set HAS_INVERTED_INDEX properly
      // The hasInvertedIndex flag in segment metadata is picked up in ColumnMetadata, and will be used during the query
      // plan phase. If it is set to false, then inverted indexes are not used in queries even if they are created via table
      // configs on segment load. So, we set it to true here for now, until we fix the server to update the value inside
      // ColumnMetadata, export information to the query planner that the inverted index available is current and can be used.
      //
      //    boolean hasInvertedIndex = invertedIndexCreatorMap.containsKey();
      boolean hasInvertedIndex = true;

      String hllOriginColumn = null;
      if (derivedHllFieldToOriginMap != null) {
        hllOriginColumn = derivedHllFieldToOriginMap.get(column);
      }

      addColumnMetadataInfo(properties, column, columnIndexCreationInfo, totalDocs, totalRawDocs, totalAggDocs,
          schema.getFieldSpecFor(column), _dictionaryCreatorMap.containsKey(column), dictionaryElementSize,
          hasInvertedIndex, hllOriginColumn);
    }

    properties.save();
  }

  /**
   * Check for the validity of segment start and end time
   * @param startTime segment start time
   * @param endTime segment end time
   * @param segmentName segment name
   */
  private void checkTime(final SegmentGeneratorConfig config, final Object startTime, final Object endTime,
      final String segmentName) {
    if (!config.isCheckTimeColumnValidityDuringGeneration()) {
      return;
    }

    if (startTime == null || endTime == null) {
      throw new RuntimeException("Expecting non-null start/end time for segment: " + segmentName);
    }

    if (!(startTime.getClass().equals(endTime.getClass()))) {
      final StringBuilder err = new StringBuilder();
      err.append("Start and end time of segment should be of same type.").append(" segment name: ").append(segmentName)
          .append(" start time: ").append(startTime).append(" end time: ").append(endTime).append(" start time class: ")
          .append(startTime.getClass()).append(" end time class: ").append(endTime.getClass());
      throw new RuntimeException(err.toString());
    }

    long start;
    long end;

    final String cl = startTime.getClass().getSimpleName();

    switch (cl) {
      case "Long":
        start = (long) startTime;
        end = (long) endTime;
        break;
      case "String":
        start = Long.parseLong((String) startTime);
        end = Long.parseLong((String) endTime);
        break;
      case "Integer":
        start = ((Integer) startTime).longValue();
        end = ((Integer) endTime).longValue();
        break;
      default:
        final StringBuilder err = new StringBuilder();
        err.append("Unable to interpret type of time column value. Failed to validate start and end time of segment")
            .append(" uninterpreted type: ").append(startTime.getClass()).append(" start time: ").append(startTime)
            .append(" end time: ").append(endTime).append(" time column name: ").append(config.getTimeColumnName())
            .append(" segment name: ").append(segmentName).append(" segment time column unit: ")
            .append(config.getSegmentTimeUnit().toString()).append(" segment time column type: ")
            .append(config.getTimeColumnType().toString()).append(" time field spec data type: ")
            .append(config.getSchema().getTimeFieldSpec().getDataType().toString());
        LOGGER.error(err.toString());
        throw new RuntimeException(err.toString());
    }

    // note that handling of SimpleDateFormat (TimeColumnType.SIMPLE)
    // is done by the caller of this function that converts the simple format
    // into millis since epoch before calling this function for validation.
    // For TimeColumnType.EPOCH, the time field spec could still have unit
    // as any of the following and we need to convert to millis for doing the
    // min-max comparison against TimeUtils.getValidMinTimeMillis() and
    // TimeUtils.getValidMaxTimeMillis()
    if (config.getTimeColumnType() == SegmentGeneratorConfig.TimeColumnType.EPOCH) {
      switch (config.getSegmentTimeUnit()) {
        case DAYS:
          start = TimeUnit.DAYS.toMillis(start);
          end = TimeUnit.DAYS.toMillis(end);
          break;
        case HOURS:
          start = TimeUnit.HOURS.toMillis(start);
          end = TimeUnit.HOURS.toMillis(end);
          break;
        case MINUTES:
          start = TimeUnit.MINUTES.toMillis(start);
          end = TimeUnit.MINUTES.toMillis(end);
          break;
        case SECONDS:
          start = TimeUnit.SECONDS.toMillis(start);
          end = TimeUnit.SECONDS.toMillis(end);
          break;
        case MICROSECONDS:
          start = TimeUnit.MICROSECONDS.toMillis(start);
          end = TimeUnit.MICROSECONDS.toMillis(end);
          break;
        case NANOSECONDS:
          start = TimeUnit.NANOSECONDS.toMillis(start);
          end = TimeUnit.NANOSECONDS.toMillis(end);
          break;
        default:
          if (config.getSegmentTimeUnit() != TimeUnit.MILLISECONDS) {
            // we should never be here
            final StringBuilder err = new StringBuilder();
            err.append("Unexpected time unit: ").append(config.getSegmentTimeUnit().toString())
                .append(" for time column: ").append(config.getTimeColumnName()).append(" for segment: ")
                .append(segmentName);
            LOGGER.error(err.toString());
            throw new RuntimeException(err.toString());
          }
      }
    }

    if (!TimeUtils.checkSegmentTimeValidity(start, end)) {
      final Date minDate = new Date(TimeUtils.getValidMinTimeMillis());
      final Date maxDate = new Date(TimeUtils.getValidMaxTimeMillis());
      final StringBuilder err = new StringBuilder();
      err.append("Invalid start/end time.").append(" segment name: ").append(segmentName).append(" time column name: ")
          .append(config.getTimeColumnName()).append(" given start time: ").append(start).append("ms")
          .append(" given end time: ").append(end).append("ms").append(" start and end time must be between ")
          .append(minDate).append(" and ").append(maxDate).append(" segment time column unit: ")
          .append(config.getSegmentTimeUnit().toString()).append(" segment time column type: ")
          .append(config.getTimeColumnType().toString()).append(" time field spec data type: ")
          .append(config.getSchema().getTimeFieldSpec().getDataType().toString());
      LOGGER.error(err.toString());
      throw new RuntimeException(err.toString());
    }
  }

  public static void addColumnMetadataInfo(PropertiesConfiguration properties, String column,
      ColumnIndexCreationInfo columnIndexCreationInfo, int totalDocs, int totalRawDocs, int totalAggDocs,
      FieldSpec fieldSpec, boolean hasDictionary, int dictionaryElementSize, boolean hasInvertedIndex,
      String hllOriginColumn) {
    int cardinality = columnIndexCreationInfo.getDistinctValueCount();
    properties.setProperty(getKeyFor(column, CARDINALITY), String.valueOf(cardinality));
    properties.setProperty(getKeyFor(column, TOTAL_DOCS), String.valueOf(totalDocs));
    properties.setProperty(getKeyFor(column, TOTAL_RAW_DOCS), String.valueOf(totalRawDocs));
    properties.setProperty(getKeyFor(column, TOTAL_AGG_DOCS), String.valueOf(totalAggDocs));
    properties.setProperty(getKeyFor(column, DATA_TYPE), String.valueOf(fieldSpec.getDataType()));
    properties.setProperty(getKeyFor(column, BITS_PER_ELEMENT),
        String.valueOf(PinotDataBitSet.getNumBitsPerValue(cardinality - 1)));
    properties.setProperty(getKeyFor(column, DICTIONARY_ELEMENT_SIZE), String.valueOf(dictionaryElementSize));
    properties.setProperty(getKeyFor(column, COLUMN_TYPE), String.valueOf(fieldSpec.getFieldType()));
    properties.setProperty(getKeyFor(column, IS_SORTED), String.valueOf(columnIndexCreationInfo.isSorted()));
    properties.setProperty(getKeyFor(column, HAS_NULL_VALUE), String.valueOf(columnIndexCreationInfo.hasNulls()));
    properties.setProperty(getKeyFor(column, HAS_DICTIONARY), String.valueOf(hasDictionary));
    properties.setProperty(V1Constants.MetadataKeys.Column.getKeyFor(column, HAS_INVERTED_INDEX),
        String.valueOf(hasInvertedIndex));
    properties.setProperty(V1Constants.MetadataKeys.Column.getKeyFor(column, IS_SINGLE_VALUED),
        String.valueOf(fieldSpec.isSingleValueField()));
    properties.setProperty(V1Constants.MetadataKeys.Column.getKeyFor(column, MAX_MULTI_VALUE_ELEMTS),
        String.valueOf(columnIndexCreationInfo.getMaxNumberOfMultiValueElements()));
    properties.setProperty(V1Constants.MetadataKeys.Column.getKeyFor(column, TOTAL_NUMBER_OF_ENTRIES),
        String.valueOf(columnIndexCreationInfo.getTotalNumberOfEntries()));
    properties.setProperty(V1Constants.MetadataKeys.Column.getKeyFor(column, IS_AUTO_GENERATED),
        String.valueOf(columnIndexCreationInfo.isAutoGenerated()));

    PartitionFunction partitionFunction = columnIndexCreationInfo.getPartitionFunction();
    if (partitionFunction != null) {
      properties.setProperty(V1Constants.MetadataKeys.Column.getKeyFor(column, PARTITION_FUNCTION),
          partitionFunction.toString());
      properties.setProperty(V1Constants.MetadataKeys.Column.getKeyFor(column, NUM_PARTITIONS),
          columnIndexCreationInfo.getNumPartitions());
      properties.setProperty(V1Constants.MetadataKeys.Column.getKeyFor(column, PARTITION_VALUES),
          columnIndexCreationInfo.getPartitions());
    }

    // datetime field
    if (fieldSpec.getFieldType().equals(FieldType.DATE_TIME)) {
      DateTimeFieldSpec dateTimeFieldSpec = (DateTimeFieldSpec) fieldSpec;
      properties.setProperty(V1Constants.MetadataKeys.Column.getKeyFor(column, DATETIME_FORMAT),
          dateTimeFieldSpec.getFormat());
      properties.setProperty(V1Constants.MetadataKeys.Column.getKeyFor(column, DATETIME_GRANULARITY),
          dateTimeFieldSpec.getGranularity());
    }

    // HLL derived fields
    if (hllOriginColumn != null) {
      properties.setProperty(V1Constants.MetadataKeys.Column.getKeyFor(column, ORIGIN_COLUMN), hllOriginColumn);
      properties.setProperty(V1Constants.MetadataKeys.Column.getKeyFor(column, DERIVED_METRIC_TYPE), "HLL");
    }

    Object defaultNullValue = columnIndexCreationInfo.getDefaultNullValue();
    if (defaultNullValue instanceof byte[]) {
      String defaultNullValueString = BytesUtils.toHexString((byte[]) defaultNullValue);
      properties
          .setProperty(V1Constants.MetadataKeys.Column.getKeyFor(column, DEFAULT_NULL_VALUE), defaultNullValueString);
    } else {
      properties.setProperty(V1Constants.MetadataKeys.Column.getKeyFor(column, DEFAULT_NULL_VALUE),
          String.valueOf(defaultNullValue));
    }
  }

  public static void addColumnMinMaxValueInfo(PropertiesConfiguration properties, String column, String minValue,
      String maxValue) {
    properties.setProperty(getKeyFor(column, MIN_VALUE), minValue);
    properties.setProperty(getKeyFor(column, MAX_VALUE), maxValue);
  }

  public static void removeColumnMetadataInfo(PropertiesConfiguration properties, String column) {
    properties.clearProperty(getKeyFor(column, CARDINALITY));
    properties.clearProperty(getKeyFor(column, TOTAL_DOCS));
    properties.clearProperty(getKeyFor(column, TOTAL_RAW_DOCS));
    properties.clearProperty(getKeyFor(column, TOTAL_AGG_DOCS));
    properties.clearProperty(getKeyFor(column, DATA_TYPE));
    properties.clearProperty(getKeyFor(column, BITS_PER_ELEMENT));
    properties.clearProperty(getKeyFor(column, DICTIONARY_ELEMENT_SIZE));
    properties.clearProperty(getKeyFor(column, COLUMN_TYPE));
    properties.clearProperty(getKeyFor(column, IS_SORTED));
    properties.clearProperty(getKeyFor(column, HAS_NULL_VALUE));
    properties.clearProperty(getKeyFor(column, HAS_DICTIONARY));
    properties.clearProperty(getKeyFor(column, HAS_INVERTED_INDEX));
    properties.clearProperty(getKeyFor(column, IS_SINGLE_VALUED));
    properties.clearProperty(getKeyFor(column, MAX_MULTI_VALUE_ELEMTS));
    properties.clearProperty(getKeyFor(column, TOTAL_NUMBER_OF_ENTRIES));
    properties.clearProperty(getKeyFor(column, IS_AUTO_GENERATED));
    properties.clearProperty(getKeyFor(column, DEFAULT_NULL_VALUE));
    properties.clearProperty(getKeyFor(column, DERIVED_METRIC_TYPE));
    properties.clearProperty(getKeyFor(column, ORIGIN_COLUMN));
    properties.clearProperty(getKeyFor(column, MIN_VALUE));
    properties.clearProperty(getKeyFor(column, MAX_VALUE));
  }

  /**
   * Helper method to build the raw index creator for the column.
   * Assumes that column to be indexed is single valued.
   *
   * @param file Output index file
   * @param column Column name
   * @param totalDocs Total number of documents to index
   * @param lengthOfLongestEntry Length of longest entry
   * @return
   * @throws IOException
   */
  public static SingleValueRawIndexCreator getRawIndexCreatorForColumn(File file,
      ChunkCompressorFactory.CompressionType compressionType, String column, FieldSpec.DataType dataType, int totalDocs,
      int lengthOfLongestEntry)
      throws IOException {

    SingleValueRawIndexCreator indexCreator;
    switch (dataType) {
      case INT:
        indexCreator = new SingleValueFixedByteRawIndexCreator(file, compressionType, column, totalDocs, Integer.BYTES);
        break;

      case LONG:
        indexCreator = new SingleValueFixedByteRawIndexCreator(file, compressionType, column, totalDocs, Long.BYTES);
        break;

      case FLOAT:
        indexCreator = new SingleValueFixedByteRawIndexCreator(file, compressionType, column, totalDocs, Float.BYTES);
        break;

      case DOUBLE:
        indexCreator = new SingleValueFixedByteRawIndexCreator(file, compressionType, column, totalDocs, Double.BYTES);
        break;

      case STRING:
      case BYTES:
        indexCreator =
            new SingleValueVarByteRawIndexCreator(file, compressionType, column, totalDocs, lengthOfLongestEntry);
        break;

      default:
        throw new UnsupportedOperationException("Data type not supported for raw indexing: " + dataType);
    }

    return indexCreator;
  }

  @Override
  public void close()
      throws IOException {
    FileUtils.close(Iterables
        .concat(_dictionaryCreatorMap.values(), _forwardIndexCreatorMap.values(), _invertedIndexCreatorMap.values(), _presenceVectorCreatorMap.values()));
  }
}
