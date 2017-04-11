/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.core.segment.creator.impl;

import com.linkedin.pinot.common.data.FieldSpec;
import com.linkedin.pinot.common.data.Schema;
import com.linkedin.pinot.common.data.StarTreeIndexSpec;
import com.linkedin.pinot.core.data.GenericRow;
import com.linkedin.pinot.core.data.partition.PartitionFunction;
import com.linkedin.pinot.common.config.ColumnPartitionConfig;
import com.linkedin.pinot.core.indexsegment.generator.SegmentGeneratorConfig;
import com.linkedin.pinot.core.segment.creator.ColumnIndexCreationInfo;
import com.linkedin.pinot.core.segment.creator.ForwardIndexCreator;
import com.linkedin.pinot.core.segment.creator.InvertedIndexCreator;
import com.linkedin.pinot.core.segment.creator.MultiValueForwardIndexCreator;
import com.linkedin.pinot.core.segment.creator.SegmentCreator;
import com.linkedin.pinot.core.segment.creator.SegmentIndexCreationInfo;
import com.linkedin.pinot.core.segment.creator.SingleValueForwardIndexCreator;
import com.linkedin.pinot.core.segment.creator.SingleValueRawIndexCreator;
import com.linkedin.pinot.core.segment.creator.impl.fwd.MultiValueUnsortedForwardIndexCreator;
import com.linkedin.pinot.core.segment.creator.impl.fwd.SingleValueFixedByteRawIndexCreator;
import com.linkedin.pinot.core.segment.creator.impl.fwd.SingleValueSortedForwardIndexCreator;
import com.linkedin.pinot.core.segment.creator.impl.fwd.SingleValueUnsortedForwardIndexCreator;
import com.linkedin.pinot.core.segment.creator.impl.fwd.SingleValueVarByteRawIndexCreator;
import com.linkedin.pinot.core.segment.creator.impl.inv.OffHeapBitmapInvertedIndexCreator;
import com.linkedin.pinot.core.startree.hll.HllConfig;
import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.lang.StringEscapeUtils;
import org.apache.commons.lang.math.IntRange;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.linkedin.pinot.core.segment.creator.impl.V1Constants.MetadataKeys.Column.*;
import static com.linkedin.pinot.core.segment.creator.impl.V1Constants.MetadataKeys.Segment.*;
import static com.linkedin.pinot.core.segment.creator.impl.V1Constants.MetadataKeys.StarTree.*;


/**
 * Segment creator which writes data in a columnar form.
 */
public class SegmentColumnarIndexCreator implements SegmentCreator {
  private Logger LOGGER = LoggerFactory.getLogger(SegmentColumnarIndexCreator.class);

  // TODO Refactor class name to match interface name
  private SegmentGeneratorConfig config;
  private Map<String, ColumnIndexCreationInfo> indexCreationInfoMap;
  private Map<String, SegmentDictionaryCreator> dictionaryCreatorMap;
  private Map<String, ForwardIndexCreator> forwardIndexCreatorMap;
  private Map<String, ForwardIndexCreator> rawIndexCreatorMap;
  private Map<String, InvertedIndexCreator> invertedIndexCreatorMap;
  private String segmentName;

  private Schema schema;
  private File file;
  private int totalDocs;
  private int totalRawDocs;
  private int totalAggDocs;
  private int totalErrors;
  private int totalNulls;
  private int totalConversions;
  private int totalNullCols;
  private int docIdCounter;
  private char paddingCharacter;
  private Map<String, Map<Object, Object>> dictionaryCache = new HashMap<String, Map<Object, Object>>();

  @Override
  public void init(SegmentGeneratorConfig segmentCreationSpec, SegmentIndexCreationInfo segmentIndexCreationInfo,
      Map<String, ColumnIndexCreationInfo> indexCreationInfoMap, Schema schema, File outDir) throws Exception {
    docIdCounter = 0;
    config = segmentCreationSpec;
    this.indexCreationInfoMap = indexCreationInfoMap;
    dictionaryCreatorMap = new HashMap<String, SegmentDictionaryCreator>();
    forwardIndexCreatorMap = new HashMap<String, ForwardIndexCreator>();
    this.indexCreationInfoMap = indexCreationInfoMap;
    invertedIndexCreatorMap = new HashMap<String, InvertedIndexCreator>();
    file = outDir;

    // Check that the output directory does not exist
    if (file.exists()) {
      throw new RuntimeException("Segment output directory " + file.getAbsolutePath() + " already exists.");
    }

    file.mkdir();

    this.schema = schema;

    this.totalDocs = segmentIndexCreationInfo.getTotalDocs();
    this.totalAggDocs = segmentIndexCreationInfo.getTotalAggDocs();
    this.totalRawDocs = segmentIndexCreationInfo.getTotalRawDocs();
    this.totalErrors = segmentIndexCreationInfo.getTotalErrors();
    this.totalNulls = segmentIndexCreationInfo.getTotalNulls();
    this.totalConversions = segmentIndexCreationInfo.getTotalConversions();
    this.totalNullCols = segmentIndexCreationInfo.getTotalNullCols();
    this.paddingCharacter = segmentCreationSpec.getPaddingCharacter();

    // Initialize and build dictionaries
    for (final FieldSpec spec : schema.getAllFieldSpecs()) {
      String column = spec.getName();
      final ColumnIndexCreationInfo info = indexCreationInfoMap.get(column);

      if (createDictionaryForColumn(info, config, spec)) {
        dictionaryCreatorMap.put(column,
            new SegmentDictionaryCreator(info.hasNulls(), info.getSortedUniqueElementsArray(), spec, file,
                paddingCharacter));
      }
    }

    // For each column, build its dictionary and initialize a forwards and an inverted index
    for (final String column : indexCreationInfoMap.keySet()) {
      ColumnIndexCreationInfo indexCreationInfo = indexCreationInfoMap.get(column);
      boolean[] isSorted = new boolean[1];
      isSorted[0] = indexCreationInfo.isSorted();
      SegmentDictionaryCreator dictionaryCreator = dictionaryCreatorMap.get(column);

      if (dictionaryCreator != null) {
        dictionaryCreator.build(isSorted);
        indexCreationInfo.setSorted(isSorted[0]);
        dictionaryCache.put(column, new HashMap<Object, Object>());
      }
      int uniqueValueCount = indexCreationInfo.getDistinctValueCount();
      int maxLength = indexCreationInfo.getLegnthOfLongestEntry();

      boolean buildRawIndex = config.getRawIndexCreationColumns().contains(column);
      FieldSpec fieldSpec = schema.getFieldSpecFor(column);
      if (fieldSpec.isSingleValueField()) {
        // Raw indexes store actual values, instead of dictionary ids.
        if (buildRawIndex) {
          forwardIndexCreatorMap.put(column,
              getRawIndexCreatorForColumn(file, column, fieldSpec.getDataType(), totalDocs, maxLength));
        } else {
          if (indexCreationInfo.isSorted()) {
            forwardIndexCreatorMap.put(column,
                new SingleValueSortedForwardIndexCreator(file, uniqueValueCount, fieldSpec));
          } else {
            forwardIndexCreatorMap.put(column,
                new SingleValueUnsortedForwardIndexCreator(fieldSpec, file, uniqueValueCount,
                    totalDocs, indexCreationInfo.getTotalNumberOfEntries(), indexCreationInfo.hasNulls()));
          }
        }
      } else {
        if (buildRawIndex) { // TODO: Add support for multi-valued columns.
          throw new RuntimeException("Raw index generation not supported for multi-valued columns: " + column);
        }
        forwardIndexCreatorMap.put(column,
            new MultiValueUnsortedForwardIndexCreator(fieldSpec, file, uniqueValueCount, totalDocs,
                indexCreationInfo.getTotalNumberOfEntries(), indexCreationInfo.hasNulls()));
      }
    }

    for (String column : config.getInvertedIndexCreationColumns()) {
      if (!schema.hasColumn(column)) {
        LOGGER.warn("Skipping enabling index on column:{} since its missing in schema", column);
        continue;
      }
      ColumnIndexCreationInfo indexCreationInfo = indexCreationInfoMap.get(column);
      int uniqueValueCount = indexCreationInfo.getDistinctValueCount();
      OffHeapBitmapInvertedIndexCreator invertedIndexCreator = new OffHeapBitmapInvertedIndexCreator(file,
          uniqueValueCount, totalDocs, indexCreationInfo.getTotalNumberOfEntries(), schema.getFieldSpecFor(column));
      invertedIndexCreatorMap.put(column, invertedIndexCreator);
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

    if (config.getRawIndexCreationColumns().contains(column)) {
      if (!spec.isSingleValueField()) {
        throw new RuntimeException(
            "Creation of indices without dictionaries is supported for single valued columns only.");
      }
      return false;
    }
    return info.isCreateDictionary();
  }

  @Override
  public void indexRow(GenericRow row) {
    for (final String column : forwardIndexCreatorMap.keySet()) {
      try {
        Object columnValueToIndex = row.getValue(column);
        if (columnValueToIndex == null) {
          throw new RuntimeException("Null value for column:" + column);
        }

        SegmentDictionaryCreator dictionaryCreator = dictionaryCreatorMap.get(column);
        if (schema.getFieldSpecFor(column).isSingleValueField()) {
          if (dictionaryCreator != null) {
            int dictionaryIndex = dictionaryCreator.indexOfSV(columnValueToIndex);
            ((SingleValueForwardIndexCreator) forwardIndexCreatorMap.get(column)).index(docIdCounter, dictionaryIndex);
            // TODO : {refactor inverted index addition}
            if (invertedIndexCreatorMap.containsKey(column)) {
              invertedIndexCreatorMap.get(column).add(docIdCounter, dictionaryIndex);
            }
          } else {
            ((SingleValueRawIndexCreator) forwardIndexCreatorMap.get(column)).index(docIdCounter, columnValueToIndex);
          }
        } else {
          int[] dictionaryIndex = dictionaryCreator.indexOfMV(columnValueToIndex);
          ((MultiValueForwardIndexCreator) forwardIndexCreatorMap.get(column)).index(docIdCounter, dictionaryIndex);

          // TODO : {refactor inverted index addition}
          if (invertedIndexCreatorMap.containsKey(column)) {
            invertedIndexCreatorMap.get(column).add(docIdCounter, dictionaryIndex);
          }
        }
      } catch (Exception e) {
        throw new RuntimeException("Exception while indexing column:"+ column, e);
      }
    }
    docIdCounter++;
  }

  @Override
  public void setSegmentName(String segmentName) {
    this.segmentName = segmentName;
  }

  @Override
  public void seal() throws ConfigurationException, IOException {
    for (final String column : forwardIndexCreatorMap.keySet()) {
      forwardIndexCreatorMap.get(column).close();
      SegmentDictionaryCreator dictionaryCreator = dictionaryCreatorMap.get(column);
      if (dictionaryCreator != null) {
        dictionaryCreator.close();
      }
    }

    // The map is only initialized for columns that have inverted index creation enabled.
    for (final String invertedColumn : invertedIndexCreatorMap.keySet()) {
      invertedIndexCreatorMap.get(invertedColumn).seal();
    }
    writeMetadata();
  }

  void writeMetadata() throws ConfigurationException {
    PropertiesConfiguration properties =
        new PropertiesConfiguration(new File(file, V1Constants.MetadataKeys.METADATA_FILE_NAME));

    properties.setProperty(SEGMENT_CREATOR_VERSION, config.getCreatorVersion());
    properties.setProperty(SEGMENT_PADDING_CHARACTER,
        StringEscapeUtils.escapeJava(Character.toString(config.getPaddingCharacter())));
    properties.setProperty(SEGMENT_NAME, segmentName);
    properties.setProperty(TABLE_NAME, config.getTableName());
    properties.setProperty(DIMENSIONS, config.getDimensions());
    properties.setProperty(METRICS, config.getMetrics());
    properties.setProperty(TIME_COLUMN_NAME, config.getTimeColumnName());
    properties.setProperty(TIME_INTERVAL, "not_there");
    properties.setProperty(SEGMENT_TOTAL_RAW_DOCS, String.valueOf(totalRawDocs));
    properties.setProperty(SEGMENT_TOTAL_AGGREGATE_DOCS, String.valueOf(totalAggDocs));
    properties.setProperty(SEGMENT_TOTAL_DOCS, String.valueOf(totalDocs));
    properties.setProperty(STAR_TREE_ENABLED, String.valueOf(config.isEnableStarTreeIndex()));
    properties.setProperty(SEGMENT_TOTAL_ERRORS, String.valueOf(totalErrors));
    properties.setProperty(SEGMENT_TOTAL_NULLS, String.valueOf(totalNulls));
    properties.setProperty(SEGMENT_TOTAL_CONVERSIONS, String.valueOf(totalConversions));
    properties.setProperty(SEGMENT_TOTAL_NULL_COLS, String.valueOf(totalNullCols));

    StarTreeIndexSpec starTreeIndexSpec = config.getStarTreeIndexSpec();
    if (starTreeIndexSpec != null) {
      properties.setProperty(STAR_TREE_SPLIT_ORDER, starTreeIndexSpec.getDimensionsSplitOrder());
      properties.setProperty(STAR_TREE_MAX_LEAF_RECORDS, starTreeIndexSpec.getMaxLeafRecords());
      properties.setProperty(STAR_TREE_SKIP_STAR_NODE_CREATION_FOR_DIMENSIONS,
          starTreeIndexSpec.getSkipStarNodeCreationForDimensions());
      properties.setProperty(STAR_TREE_SKIP_MATERIALIZATION_CARDINALITY,
          starTreeIndexSpec.getskipMaterializationCardinalityThreshold());
      properties.setProperty(STAR_TREE_SKIP_MATERIALIZATION_FOR_DIMENSIONS,
          starTreeIndexSpec.getskipMaterializationForDimensions());
    }

    HllConfig hllConfig = config.getHllConfig();
    Map<String, String> derivedHllFieldToOriginMap = null;
    if (hllConfig != null) {
      properties.setProperty(SEGMENT_HLL_LOG2M, hllConfig.getHllLog2m());
      derivedHllFieldToOriginMap = hllConfig.getDerivedHllFieldToOriginMap();
    }

    String timeColumn = config.getTimeColumnName();
    if (indexCreationInfoMap.get(timeColumn) != null) {
      properties.setProperty(SEGMENT_START_TIME, indexCreationInfoMap.get(timeColumn).getMin());
      properties.setProperty(SEGMENT_END_TIME, indexCreationInfoMap.get(timeColumn).getMax());
      properties.setProperty(TIME_UNIT, config.getSegmentTimeUnit());
    }

    if (config.containsCustomProperty(SEGMENT_START_TIME)) {
      properties.setProperty(SEGMENT_START_TIME, config.getStartTime());
    }
    if (config.containsCustomProperty(SEGMENT_END_TIME)) {
      properties.setProperty(SEGMENT_END_TIME, config.getEndTime());
    }
    if (config.containsCustomProperty(TIME_UNIT)) {
      properties.setProperty(TIME_UNIT, config.getSegmentTimeUnit());
    }

    for (Map.Entry<String, String> entry : config.getCustomProperties().entrySet()) {
      properties.setProperty(entry.getKey(), entry.getValue());
    }

    for (Map.Entry<String, ColumnIndexCreationInfo> entry : indexCreationInfoMap.entrySet()) {
      String column = entry.getKey();
      ColumnIndexCreationInfo columnIndexCreationInfo = entry.getValue();
      SegmentDictionaryCreator dictionaryCreator = dictionaryCreatorMap.get(column);
      int dictionaryElementSize = (dictionaryCreator != null) ? dictionaryCreator.getStringColumnMaxLength() : 0;

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
          schema.getFieldSpecFor(column), dictionaryCreatorMap.containsKey(column), dictionaryElementSize,
          hasInvertedIndex, hllOriginColumn);
    }

    properties.save();
  }

  public static void addColumnMetadataInfo(PropertiesConfiguration properties, String column,
      ColumnIndexCreationInfo columnIndexCreationInfo, int totalDocs, int totalRawDocs,
      int totalAggDocs, FieldSpec fieldSpec, boolean hasDictionary, int dictionaryElementSize, boolean hasInvertedIndex,
      String hllOriginColumn) {
    int distinctValueCount = columnIndexCreationInfo.getDistinctValueCount();
    properties.setProperty(getKeyFor(column, CARDINALITY), String.valueOf(distinctValueCount));
    properties.setProperty(getKeyFor(column, TOTAL_DOCS), String.valueOf(totalDocs));
    properties.setProperty(getKeyFor(column, TOTAL_RAW_DOCS), String.valueOf(totalRawDocs));
    properties.setProperty(getKeyFor(column, TOTAL_AGG_DOCS), String.valueOf(totalAggDocs));
    properties.setProperty(getKeyFor(column, DATA_TYPE), String.valueOf(fieldSpec.getDataType()));
    properties.setProperty(getKeyFor(column, BITS_PER_ELEMENT),
        String.valueOf(SingleValueUnsortedForwardIndexCreator.getNumOfBits(distinctValueCount)));
    properties.setProperty(getKeyFor(column, DICTIONARY_ELEMENT_SIZE), String.valueOf(dictionaryElementSize));
    properties.setProperty(getKeyFor(column, COLUMN_TYPE), String.valueOf(fieldSpec.getFieldType()));
    properties.setProperty(getKeyFor(column, IS_SORTED), String.valueOf(columnIndexCreationInfo.isSorted()));
    properties.setProperty(getKeyFor(column, HAS_NULL_VALUE), String.valueOf(columnIndexCreationInfo.hasNulls()));
    properties.setProperty(getKeyFor(column, HAS_DICTIONARY),
        String.valueOf(hasDictionary));
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
    int numPartitions = columnIndexCreationInfo.getNumPartitions();
    List<IntRange> partitionRanges = columnIndexCreationInfo.getPartitionRanges();
    if (partitionFunction != null && partitionRanges != null) {
      properties.setProperty(V1Constants.MetadataKeys.Column.getKeyFor(column, PARTITION_FUNCTION),
          partitionFunction.toString());
      properties.setProperty(V1Constants.MetadataKeys.Column.getKeyFor(column, NUM_PARTITIONS), numPartitions);
      String partitionValues = ColumnPartitionConfig.rangesToString(partitionRanges);
      properties.setProperty(V1Constants.MetadataKeys.Column.getKeyFor(column, PARTITION_VALUES), partitionValues);
    }

    // HLL derived fields
    if (hllOriginColumn != null) {
      properties.setProperty(V1Constants.MetadataKeys.Column.getKeyFor(column, ORIGIN_COLUMN), hllOriginColumn);
      properties.setProperty(V1Constants.MetadataKeys.Column.getKeyFor(column, DERIVED_METRIC_TYPE), "HLL");
    }

    Object defaultNullValue = columnIndexCreationInfo.getDefaultNullValue();
    if (defaultNullValue == null) {
      defaultNullValue = fieldSpec.getDefaultNullValue();
    }
    properties.setProperty(V1Constants.MetadataKeys.Column.getKeyFor(column, DEFAULT_NULL_VALUE),
        String.valueOf(defaultNullValue));
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
  public static SingleValueRawIndexCreator getRawIndexCreatorForColumn(File file, String column,
      FieldSpec.DataType dataType, int totalDocs, int lengthOfLongestEntry)
      throws IOException {

    SingleValueRawIndexCreator indexCreator;
    switch(dataType) {
      case INT:
        indexCreator =
            new SingleValueFixedByteRawIndexCreator(file, column, totalDocs, V1Constants.Numbers.INTEGER_SIZE);
        break;

      case LONG:
        indexCreator =
            new SingleValueFixedByteRawIndexCreator(file, column, totalDocs, V1Constants.Numbers.LONG_SIZE);
        break;

      case FLOAT:
        indexCreator =
            new SingleValueFixedByteRawIndexCreator(file, column, totalDocs, V1Constants.Numbers.FLOAT_SIZE);
        break;

      case DOUBLE:
        indexCreator =
            new SingleValueFixedByteRawIndexCreator(file, column, totalDocs, V1Constants.Numbers.DOUBLE_SIZE);
        break;

      case STRING:
        indexCreator = new SingleValueVarByteRawIndexCreator(file, column, totalDocs, lengthOfLongestEntry);
        break;

      default:
        throw new UnsupportedOperationException("Data type not supported for raw indexing: " + dataType);
    }

    return indexCreator;
  }
}
