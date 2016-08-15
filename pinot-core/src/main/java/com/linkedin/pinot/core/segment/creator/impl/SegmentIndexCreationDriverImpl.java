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

import com.google.common.collect.HashBiMap;
import com.linkedin.pinot.common.data.FieldSpec;
import com.linkedin.pinot.common.data.Schema;
import com.linkedin.pinot.common.data.StarTreeIndexSpec;
import com.linkedin.pinot.common.utils.SegmentNameBuilder;
import com.linkedin.pinot.core.data.GenericRow;
import com.linkedin.pinot.core.data.extractors.FieldExtractorFactory;
import com.linkedin.pinot.core.data.extractors.PlainFieldExtractor;
import com.linkedin.pinot.core.data.readers.RecordReader;
import com.linkedin.pinot.core.data.readers.RecordReaderFactory;
import com.linkedin.pinot.core.indexsegment.generator.SegmentGeneratorConfig;
import com.linkedin.pinot.core.segment.creator.AbstractColumnStatisticsCollector;
import com.linkedin.pinot.core.segment.creator.ColumnIndexCreationInfo;
import com.linkedin.pinot.core.segment.creator.ColumnStatistics;
import com.linkedin.pinot.core.segment.creator.ForwardIndexType;
import com.linkedin.pinot.core.segment.creator.InvertedIndexType;
import com.linkedin.pinot.core.segment.creator.SegmentCreator;
import com.linkedin.pinot.core.segment.creator.SegmentIndexCreationDriver;
import com.linkedin.pinot.core.segment.creator.SegmentIndexCreationInfo;
import com.linkedin.pinot.core.segment.creator.SegmentPreIndexStatsCollector;
import com.linkedin.pinot.core.segment.creator.impl.stats.SegmentPreIndexStatsCollectorImpl;
import com.linkedin.pinot.core.startree.OffHeapStarTreeBuilder;
import com.linkedin.pinot.core.startree.StarTree;
import com.linkedin.pinot.core.startree.StarTreeBuilder;
import com.linkedin.pinot.core.startree.StarTreeBuilderConfig;
import com.linkedin.pinot.core.startree.StarTreeIndexNode;
import com.linkedin.pinot.core.util.CrcUtils;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.mutable.MutableLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Implementation of an index segment creator.
 */

public class SegmentIndexCreationDriverImpl implements SegmentIndexCreationDriver {

  private static final Logger LOGGER = LoggerFactory.getLogger(SegmentIndexCreationDriverImpl.class);

  SegmentGeneratorConfig config;
  RecordReader recordReader;
  SegmentPreIndexStatsCollector statsCollector;
  Map<String, ColumnIndexCreationInfo> indexCreationInfoMap;
  SegmentCreator indexCreator;
  SegmentIndexCreationInfo segmentIndexCreationInfo;
  Schema dataSchema;
  PlainFieldExtractor extractor;
  int totalDocs = 0;
  int totalRawDocs = 0;
  int totalAggDocs = 0;
  File tempIndexDir;
  String segmentName;
  long totalRecordReadTime = 0;
  long totalIndexTime = 0;
  long totalStatsCollectorTime = 0;
  boolean createStarTree = false;

  private File starTreeTempDir;

  @Override
  public void init(SegmentGeneratorConfig config) throws Exception {
    init(config, RecordReaderFactory.get(config));
  }

  public void init(SegmentGeneratorConfig config, RecordReader reader) throws Exception {
    this.config = config;
    this.createStarTree = config.isEnableStarTreeIndex();
    // Initialize the record reader
    recordReader = reader;
    recordReader.init();
    dataSchema = recordReader.getSchema();
    extractor = (PlainFieldExtractor) FieldExtractorFactory.getPlainFieldExtractor(dataSchema);

    // Initialize stats collection
    statsCollector = new SegmentPreIndexStatsCollectorImpl(recordReader.getSchema());
    statsCollector.init();

    // Initialize index creation
    segmentIndexCreationInfo = new SegmentIndexCreationInfo();
    indexCreationInfoMap = new HashMap<String, ColumnIndexCreationInfo>();

    // Check if has star tree
    indexCreator = new SegmentColumnarIndexCreator();

    // Ensure that the output directory exists
    final File indexDir = new File(config.getOutDir());
    if (!indexDir.exists()) {
      indexDir.mkdirs();
    }
    // Create a temporary directory used in segment creation
    tempIndexDir = new File(indexDir, com.linkedin.pinot.common.utils.FileUtils.getRandomFileName());
    starTreeTempDir = new File(indexDir, com.linkedin.pinot.common.utils.FileUtils.getRandomFileName());
    LOGGER.debug("tempIndexDir:{}", tempIndexDir);
    LOGGER.debug("starTreeTempDir:{}", starTreeTempDir);
  }

  @Override
  public void build() throws Exception {
    if (createStarTree) {
      buildStarTree();
    } else {
      buildRaw();
    }
  }

  public void buildStarTree() throws Exception {
    long start = System.currentTimeMillis();
    //construct star tree builder config
    StarTreeIndexSpec starTreeIndexSpec = config.getStarTreeIndexSpec();
    if (starTreeIndexSpec == null) {
      starTreeIndexSpec = new StarTreeIndexSpec();
      starTreeIndexSpec.setMaxLeafRecords(StarTreeIndexSpec.DEFAULT_MAX_LEAF_RECORDS);
      config.setStarTreeIndexSpec(starTreeIndexSpec);
    }
    List<String> dimensionsSplitOrder = starTreeIndexSpec.getDimensionsSplitOrder();
    if (dimensionsSplitOrder != null && !dimensionsSplitOrder.isEmpty()) {
      String timeColumnName = config.getTimeColumnName();
      if (timeColumnName != null) {
        dimensionsSplitOrder.remove(timeColumnName);
      }
    }
    //create star builder config from startreeindexspec. Merge these two in one later.
    StarTreeBuilderConfig starTreeBuilderConfig = new StarTreeBuilderConfig();
    starTreeBuilderConfig.setSchema(dataSchema);
    starTreeBuilderConfig.setDimensionsSplitOrder(dimensionsSplitOrder);
    starTreeBuilderConfig.setMaxLeafRecords(starTreeIndexSpec.getMaxLeafRecords());
    starTreeBuilderConfig.setSkipStarNodeCreationForDimensions(starTreeIndexSpec.getSkipStarNodeCreationForDimensions());
    Set<String> skipMaterializationForDimensions = starTreeIndexSpec.getskipMaterializationForDimensions();
    starTreeBuilderConfig.setSkipMaterializationForDimensions(skipMaterializationForDimensions);
    starTreeBuilderConfig.setSkipMaterializationCardinalityThreshold(starTreeIndexSpec.getskipMaterializationCardinalityThreshold());
    starTreeBuilderConfig.setOutDir(starTreeTempDir);
    //initialize star tree builder
    StarTreeBuilder starTreeBuilder = new OffHeapStarTreeBuilder();
    starTreeBuilder.init(starTreeBuilderConfig);
    //build star tree along with collecting stats
    recordReader.rewind();
    LOGGER.info("Start building star tree!");
    totalDocs = 0;
    while (recordReader.hasNext()) {
      GenericRow row = readNextRowSanitized();
      starTreeBuilder.append(row);
      statsCollector.collectRow(row);
      totalRawDocs++;
      totalDocs++;
    }
    recordReader.close();
    LOGGER.info("Start building star tree!");
    starTreeBuilder.build();
    LOGGER.info("Finished building star tree!");
    long starTreeBuildFinishTime = System.currentTimeMillis();
    //build stats
    // Count the number of documents and gather per-column statistics
    LOGGER.info("Start building StatsCollector!");
    Iterator<GenericRow> aggregatedRowsIterator = starTreeBuilder.iterator(starTreeBuilder.getTotalRawDocumentCount(),
        starTreeBuilder.getTotalRawDocumentCount() + starTreeBuilder.getTotalAggregateDocumentCount());
    while (aggregatedRowsIterator.hasNext()) {
      GenericRow genericRow = aggregatedRowsIterator.next();
      statsCollector.collectRow(genericRow, true /* isAggregated */);
      totalAggDocs++;
      totalDocs++;
    }
    buildIndexCreationInfo();
    LOGGER.info("Collected stats for {} raw documents, {} aggregated documents", totalRawDocs, totalAggDocs);
    long statCollectionFinishTime = System.currentTimeMillis();
    // Initialize the index creation using the per-column statistics information
    indexCreator.init(config, segmentIndexCreationInfo, indexCreationInfoMap, dataSchema, tempIndexDir);

    //iterate over the data again,
    Iterator<GenericRow> allRowsIterator = starTreeBuilder.iterator(0,
        starTreeBuilder.getTotalRawDocumentCount() + starTreeBuilder.getTotalAggregateDocumentCount());

    while (allRowsIterator.hasNext()) {
      GenericRow genericRow = allRowsIterator.next();
      indexCreator.indexRow(genericRow);
    }

    // If no dimensionsSplitOrder was specified in starTreeIndexSpec, set the order used by the starTreeBuilder.
    // This is required so the dimensionsSplitOrder used by the builder can be written into the segment metadata.
    if (dimensionsSplitOrder == null || dimensionsSplitOrder.isEmpty()) {
      starTreeIndexSpec.setDimensionsSplitOrder(starTreeBuilder.getDimensionsSplitOrder());
    }

    if (skipMaterializationForDimensions == null || skipMaterializationForDimensions.isEmpty()) {
      starTreeIndexSpec.setSkipMaterializationForDimensions(starTreeBuilder.getSkipMaterializationForDimensions());
    }

    serializeTree(starTreeBuilder);
    //post creation
    handlePostCreation();
    starTreeBuilder.cleanup();
    long end = System.currentTimeMillis();
    LOGGER.info("Total time:{} \n star tree build time:{} \n stat collection time:{} \n column index build time:{}",
        (end - start), (starTreeBuildFinishTime - start), statCollectionFinishTime - starTreeBuildFinishTime,
        end - statCollectionFinishTime);
  }

  private void serializeTree(StarTreeBuilder starTreeBuilder) throws Exception {
    //star tree was built using its own dictionary, we need to re-map dimension value id
    Map<String, HashBiMap<Object, Integer>> dictionaryMap = starTreeBuilder.getDictionaryMap();
    StarTree tree = starTreeBuilder.getTree();
    HashBiMap<String, Integer> dimensionNameToIndexMap = starTreeBuilder.getDimensionNameToIndexMap();
    StarTreeIndexNode node = tree.getRoot();
    updateTree(node, dictionaryMap, dimensionNameToIndexMap);
    tree.writeTree(new FileOutputStream(new File(tempIndexDir, V1Constants.STAR_TREE_INDEX_FILE)));
  }

  /**
   * Startree built its only dictionary that is different from the columnar segment dictionary.
   * This method updates the tree with new mapping
   * @param node
   * @param dictionaryMap
   * @param dimensionNameToIndexMap
   */
  private void updateTree(StarTreeIndexNode node, Map<String, HashBiMap<Object, Integer>> dictionaryMap,
      HashBiMap<String, Integer> dimensionNameToIndexMap) {
    //current node needs to update only if its not star
    if (node.getDimensionName() != StarTreeIndexNode.all()) {
      String dimName = dimensionNameToIndexMap.inverse().get(node.getDimensionName());
      int dimensionValue = node.getDimensionValue();
      if (dimensionValue != StarTreeIndexNode.all()) {
        Object sortedValuesForDim = indexCreationInfoMap.get(dimName).getSortedUniqueElementsArray();
        int indexForDimValue =
            searchValueInArray(sortedValuesForDim, dictionaryMap.get(dimName).inverse().get(dimensionValue));
        node.setDimensionValue(indexForDimValue);
      }
    }
    //update children map
    Map<Integer, StarTreeIndexNode> children = node.getChildren();
    Map<Integer, StarTreeIndexNode> newChildren = new HashMap<>();
    if (children != null && !children.isEmpty()) {
      String childDimName = dimensionNameToIndexMap.inverse().get(node.getChildDimensionName());
      Object sortedValuesForDim = indexCreationInfoMap.get(childDimName).getSortedUniqueElementsArray();
      for (Entry<Integer, StarTreeIndexNode> entry : children.entrySet()) {
        int childDimValue = entry.getKey();
        int childMappedDimValue = StarTreeIndexNode.all();
        if (childDimValue != StarTreeIndexNode.all()) {
          childMappedDimValue = searchValueInArray(sortedValuesForDim,
              dictionaryMap.get(childDimName).inverse().get(childDimValue));
        }
        newChildren.put(childMappedDimValue, entry.getValue());
        updateTree(entry.getValue(), dictionaryMap, dimensionNameToIndexMap);
      }
      node.setChildren(newChildren);
    }
  }

  public void buildRaw() throws Exception {
    // Count the number of documents and gather per-column statistics
    LOGGER.debug("Start building StatsCollector!");
    totalDocs = 0;
    while (recordReader.hasNext()) {
      totalDocs++;
      totalRawDocs++;
      long start = System.currentTimeMillis();
      GenericRow row = readNextRowSanitized();
      long stop = System.currentTimeMillis();
      statsCollector.collectRow(row);
      long stop1 = System.currentTimeMillis();
      totalRecordReadTime += (stop - start);
      totalStatsCollectorTime += (stop1 - stop);
    }
    buildIndexCreationInfo();
    LOGGER.info("Finished building StatsCollector!");
    LOGGER.info("Collected stats for {} documents", totalDocs);

    // Initialize the index creation using the per-column statistics information
    indexCreator.init(config, segmentIndexCreationInfo, indexCreationInfoMap, dataSchema, tempIndexDir);

    // Build the index
    recordReader.rewind();
    LOGGER.info("Start building IndexCreator!");
    while (recordReader.hasNext()) {
      long start = System.currentTimeMillis();
      GenericRow row = readNextRowSanitized();
      long stop = System.currentTimeMillis();
      indexCreator.indexRow(row);
      long stop1 = System.currentTimeMillis();
      totalRecordReadTime += (stop - start);
      totalIndexTime += (stop1 - stop);
    }
    recordReader.close();
    LOGGER.info("Finished records indexing in IndexCreator!");
    int numErrors, numConversions, numNulls, numNullCols;
    if ((numErrors = extractor.getTotalErrors()) > 0) {
      LOGGER.warn("Index creator for schema {} had {} rows with errors", dataSchema.getSchemaName(), numErrors);
    }
    Map<String, Integer> errorCount = extractor.getError_count();
    for (String column : errorCount.keySet()) {
      if ((numErrors = errorCount.get(column)) > 0) {
        LOGGER.info("Column {} had {} rows with errors", column, numErrors);
      }
    }
    if ((numConversions = extractor.getTotalConversions()) > 0) {
      LOGGER.info("Index creator for schema {} had {} rows with type conversions", dataSchema.getSchemaName(),
          numConversions);
    }
    if ((numNulls = extractor.getTotalNulls()) > 0) {
      LOGGER.info("Index creator for schema {} had {} rows with null columns", dataSchema.getSchemaName(), numNulls);
    }
    if ((numNullCols = extractor.getTotalNullCols()) > 0) {
      LOGGER.info("Index creator for schema {} had {}  null columns", dataSchema.getSchemaName(), numNullCols);
    }

    handlePostCreation();
  }

  private void handlePostCreation() throws Exception {
    // Build the segment name, if necessary
    final String timeColumn = config.getTimeColumnName();

    if (config.getSegmentName() != null) {
      segmentName = config.getSegmentName();
    } else {
      if (timeColumn != null && timeColumn.length() > 0) {
        final Object minTimeValue = statsCollector.getColumnProfileFor(timeColumn).getMinValue();
        final Object maxTimeValue = statsCollector.getColumnProfileFor(timeColumn).getMaxValue();
        segmentName = SegmentNameBuilder.buildBasic(config.getTableName(), minTimeValue, maxTimeValue,
            config.getSegmentNamePostfix());
      } else {
        segmentName = SegmentNameBuilder.buildBasic(config.getTableName(), config.getSegmentNamePostfix());
      }
    }

    // Write the index files to disk
    indexCreator.setSegmentName(segmentName);
    indexCreator.seal();
    LOGGER.info("Finished segment seal!");

    // Delete the directory named after the segment name, if it exists
    final File outputDir = new File(config.getOutDir());
    final File segmentOutputDir = new File(outputDir, segmentName);
    if (segmentOutputDir.exists()) {
      FileUtils.deleteDirectory(segmentOutputDir);
    }

    // Move the temporary directory into its final location
    FileUtils.moveDirectory(tempIndexDir, segmentOutputDir);

    // Delete the temporary directory
    FileUtils.deleteQuietly(tempIndexDir);

    // Compute CRC
    final long crc = CrcUtils.forAllFilesInFolder(segmentOutputDir).computeCrc();

    // Persist creation metadata to disk
    persistCreationMeta(segmentOutputDir, crc);
    Map<String, MutableLong> nullCountMap = recordReader.getNullCountMap();
    if (nullCountMap != null) {
      for (Map.Entry<String, MutableLong> entry : nullCountMap.entrySet()) {
        AbstractColumnStatisticsCollector columnStatisticsCollector =
            statsCollector.getColumnProfileFor(entry.getKey());
        columnStatisticsCollector.setNumInputNullValues(entry.getValue().intValue());
      }
    }

    LOGGER.info("Driver, record read time : {}", totalRecordReadTime);
    LOGGER.info("Driver, stats collector time : {}", totalStatsCollectorTime);
    LOGGER.info("Driver, indexing time : {}", totalIndexTime);
  }

  public ColumnStatistics getColumnStatisticsCollector(final String columnName) throws Exception {
    return statsCollector.getColumnProfileFor(columnName);
  }

  public void overWriteSegmentName(String segmentName) {
    this.segmentName = segmentName;
  }

  /**
   * Writes segment creation metadata to disk.
   */
  void persistCreationMeta(File outputDir, long crc) throws IOException {
    final File crcFile = new File(outputDir, V1Constants.SEGMENT_CREATION_META);
    final DataOutputStream out = new DataOutputStream(new FileOutputStream(crcFile));
    out.writeLong(crc);

    long creationTime = System.currentTimeMillis();

    // Use the creation time from the configuration if it exists and is not -1
    try {
      long configCreationTime = Long.parseLong(config.getCreationTime());
      if (0L < configCreationTime) {
        creationTime = configCreationTime;
      }
    } catch (Exception nfe) {
      // Ignore NPE and NFE, use the current time.
    }

    out.writeLong(creationTime);
    out.close();
  }

  /**
   * Complete the stats gathering process and store the stats information in indexCreationInfoMap.
   */
  void buildIndexCreationInfo()
      throws Exception {
    statsCollector.build();
    for (FieldSpec spec : dataSchema.getAllFieldSpecs()) {
      String column = spec.getName();
      indexCreationInfoMap.put(column, new ColumnIndexCreationInfo(true/*createDictionary*/,
          statsCollector.getColumnProfileFor(column).getMinValue(),
          statsCollector.getColumnProfileFor(column).getMaxValue(),
          statsCollector.getColumnProfileFor(column).getUniqueValuesSet(), ForwardIndexType.FIXED_BIT_COMPRESSED,
          InvertedIndexType.ROARING_BITMAPS, statsCollector.getColumnProfileFor(column).isSorted(),
          statsCollector.getColumnProfileFor(column).hasNull(),
          statsCollector.getColumnProfileFor(column).getTotalNumberOfEntries(),
          statsCollector.getColumnProfileFor(column).getMaxNumberOfMultiValues(), false/*isAutoGenerated*/,
          dataSchema.getFieldSpecFor(column).getDefaultNullValue()));
    }
    segmentIndexCreationInfo.setTotalDocs(totalDocs);
    segmentIndexCreationInfo.setTotalRawDocs(totalRawDocs);
    segmentIndexCreationInfo.setTotalAggDocs(totalAggDocs);
    segmentIndexCreationInfo.setStarTreeEnabled(createStarTree);
    segmentIndexCreationInfo.setTotalConversions(extractor.getTotalConversions());
    segmentIndexCreationInfo.setTotalErrors(extractor.getTotalErrors());
    segmentIndexCreationInfo.setTotalNullCols(extractor.getTotalNullCols());
    segmentIndexCreationInfo.setTotalNulls(extractor.getTotalNulls());
  }

  @Override
  /**
   * Returns the name of the segment associated with this index creation driver.
   */
  public String getSegmentName() {
    return segmentName;
  }

  private GenericRow readNextRowSanitized() {
    GenericRow rowOrig = recordReader.next();
    return extractor.transform(rowOrig);
  }

  /**
   * Helper method to binary-search a given key in an input array.
   * Both input array and the key to search are passed in as 'Object'.
   *
   * - Supported data types are int, long, float, double & String.
   * - Throws an exception for any other data type.
   *
   * @param inputArray Input array to search
   * @param key Key to search for
   * @return
   */
  private int searchValueInArray(Object inputArray, Object key) {
    if (inputArray instanceof int[]) {
      return Arrays.binarySearch((int[]) inputArray, (Integer) key);
    } else if (inputArray instanceof long[]) {
      return Arrays.binarySearch((long[]) inputArray, (Long) key);
    } else if (inputArray instanceof float[]) {
      return Arrays.binarySearch((float[]) inputArray, (Float) key);
    } else if (inputArray instanceof double[]) {
      return Arrays.binarySearch((double[]) inputArray, (Double) key);
    } else if (inputArray instanceof String[]) {
      return Arrays.binarySearch((String[]) inputArray, key);
    } else if (inputArray instanceof Object[]) {
      return Arrays.binarySearch((Object[]) inputArray, key);
    } else {
      throw new RuntimeException(
          "Unexpected data type encountered while updating StarTree node" + inputArray.getClass().getName());
    }
  }
}
