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

import com.google.common.base.Preconditions;
import com.linkedin.pinot.common.data.FieldSpec;
import com.linkedin.pinot.common.data.MetricFieldSpec;
import com.linkedin.pinot.common.data.Schema;
import com.linkedin.pinot.common.data.StarTreeIndexSpec;
import com.linkedin.pinot.core.data.GenericRow;
import com.linkedin.pinot.core.data.extractors.FieldExtractorFactory;
import com.linkedin.pinot.core.data.extractors.PlainFieldExtractor;
import com.linkedin.pinot.core.data.readers.RecordReader;
import com.linkedin.pinot.core.data.readers.RecordReaderFactory;
import com.linkedin.pinot.core.indexsegment.generator.SegmentGeneratorConfig;
import com.linkedin.pinot.core.indexsegment.generator.SegmentVersion;
import com.linkedin.pinot.core.segment.creator.ColumnIndexCreationInfo;
import com.linkedin.pinot.core.segment.creator.ColumnStatistics;
import com.linkedin.pinot.core.segment.creator.ForwardIndexType;
import com.linkedin.pinot.core.segment.creator.InvertedIndexType;
import com.linkedin.pinot.core.segment.creator.RecordReaderSegmentCreationDataSource;
import com.linkedin.pinot.core.segment.creator.SegmentCreationDataSource;
import com.linkedin.pinot.core.segment.creator.SegmentCreator;
import com.linkedin.pinot.core.segment.creator.SegmentIndexCreationDriver;
import com.linkedin.pinot.core.segment.creator.SegmentIndexCreationInfo;
import com.linkedin.pinot.core.segment.creator.SegmentPreIndexStatsContainer;
import com.linkedin.pinot.core.segment.creator.StatsCollectorConfig;
import com.linkedin.pinot.core.segment.creator.impl.stats.SegmentPreIndexStatsCollectorImpl;
import com.linkedin.pinot.core.segment.index.converter.SegmentFormatConverter;
import com.linkedin.pinot.core.segment.index.converter.SegmentFormatConverterFactory;
import com.linkedin.pinot.core.segment.store.SegmentDirectoryPaths;
import com.linkedin.pinot.core.startree.OffHeapStarTreeBuilder;
import com.linkedin.pinot.core.startree.StarTreeBuilder;
import com.linkedin.pinot.core.startree.StarTreeBuilderConfig;
import com.linkedin.pinot.core.startree.hll.HllUtil;
import com.linkedin.pinot.core.util.CrcUtils;
import com.linkedin.pinot.startree.hll.HllConfig;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import org.apache.commons.io.FileUtils;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Implementation of an index segment creator.
 */
// TODO: Check resource leaks
public class SegmentIndexCreationDriverImpl implements SegmentIndexCreationDriver {

  private static final Logger LOGGER = LoggerFactory.getLogger(SegmentIndexCreationDriverImpl.class);

  private SegmentGeneratorConfig config;
  private RecordReader recordReader;
  private SegmentPreIndexStatsContainer segmentStats;
  private Map<String, ColumnIndexCreationInfo> indexCreationInfoMap;
  private SegmentCreator indexCreator;
  private SegmentIndexCreationInfo segmentIndexCreationInfo;
  private Schema dataSchema;
  private PlainFieldExtractor extractor;
  private int totalDocs = 0;
  private int totalRawDocs = 0;
  private int totalAggDocs = 0;
  private File tempIndexDir;
  private String segmentName;
  private long totalRecordReadTime = 0;
  private long totalIndexTime = 0;
  private long totalStatsCollectorTime = 0;
  private boolean createStarTree = false;
  // flag indicates if the this segment generator code
  // will create the HLL index for the given columns.
  // This will be false if HLL column is provided to us
  private boolean createHllIndex = false;

  private File starTreeTempDir;

  @Override
  public void init(SegmentGeneratorConfig config) throws Exception {
    init(config, new RecordReaderSegmentCreationDataSource(RecordReaderFactory.getRecordReader(config)));
  }

  public void init(SegmentGeneratorConfig config, SegmentCreationDataSource dataSource) throws Exception {
    this.config = config;
    this.createStarTree = config.isEnableStarTreeIndex();
    recordReader = dataSource.getRecordReader();
    Preconditions.checkState(recordReader.hasNext(), "No record in data source");
    dataSchema = recordReader.getSchema();

    if (config.getHllConfig() != null) {
      HllConfig hllConfig = config.getHllConfig();
      // create hll index is true only if we're provided with columns to
      // generate HLL fields
      if (hllConfig.getColumnsToDeriveHllFields() != null && !hllConfig.getColumnsToDeriveHllFields().isEmpty()) {
        if (!createStarTree) {
          throw new IllegalArgumentException("Derived HLL fields generation will not work if StarTree is not enabled.");
        } else {
          createHllIndex = true;
        }
      } // else columnsToDeriveHllFields is null...don't do anything in this case
      // segment seal() will write the log2m value to the metadata
    }

    addDerivedFieldsInSchema();

    extractor = FieldExtractorFactory.getPlainFieldExtractor(dataSchema);

    // Initialize stats collection
    if (!createStarTree) { // For star tree, the stats are gathered in buildStarTree()
      segmentStats = dataSource.gatherStats(new StatsCollectorConfig(dataSchema, config.getSegmentPartitionConfig()));
      totalDocs = segmentStats.getTotalDocCount();
      totalRawDocs = segmentStats.getRawDocCount();
      totalAggDocs = segmentStats.getAggregatedDocCount();
    }

    // Initialize index creation
    segmentIndexCreationInfo = new SegmentIndexCreationInfo();
    indexCreationInfoMap = new HashMap<>();

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

  public void init(SegmentGeneratorConfig config, RecordReader reader) throws Exception {
    init(config, new RecordReaderSegmentCreationDataSource(reader));
  }

  private void addDerivedFieldsInSchema() {
    if (createHllIndex) {
      Collection<String> columnNames = dataSchema.getColumnNames();
      HllConfig hllConfig = config.getHllConfig();
      for (String derivedFieldName : hllConfig.getDerivedHllFieldToOriginMap().keySet()) {
        if (columnNames.contains(derivedFieldName)) {
          throw new IllegalArgumentException(
              "Cannot add derived field: " + derivedFieldName + " since it already exists in schema.");
        } else {
          dataSchema.addField(
              new MetricFieldSpec(derivedFieldName, FieldSpec.DataType.STRING, hllConfig.getHllFieldSize(),
                  MetricFieldSpec.DerivedMetricType.HLL));
        }
      }
    }
  }

  private void populateDefaultDerivedColumnValues(GenericRow row) throws IOException {
    //add default hll value in each row
    if (createHllIndex) {
      HllConfig hllConfig = config.getHllConfig();
      for (Entry<String, String> entry : hllConfig.getDerivedHllFieldToOriginMap().entrySet()) {
        String derivedFieldName = entry.getKey();
        String originFieldName = entry.getValue();
        row.putField(derivedFieldName,
            HllUtil.singleValueHllAsString(hllConfig.getHllLog2m(), row.getValue(originFieldName)));
      }
    }
  }

  @Override
  public void build() throws Exception {
    if (createStarTree) {
      // TODO: add on-heap star-tree builder
      buildStarTree();
    } else {
      buildRaw();
    }
  }

  private void buildStarTree() throws Exception {
    // Create stats collector
    StatsCollectorConfig statsCollectorConfig = new StatsCollectorConfig(dataSchema, config.getSegmentPartitionConfig());
    SegmentPreIndexStatsCollectorImpl statsCollector = new SegmentPreIndexStatsCollectorImpl(statsCollectorConfig);
    statsCollector.init();
    segmentStats = statsCollector;

    long start = System.currentTimeMillis();
    //construct star tree builder config
    StarTreeIndexSpec starTreeIndexSpec = config.getStarTreeIndexSpec();
    if (starTreeIndexSpec == null) {
      starTreeIndexSpec = new StarTreeIndexSpec();
      starTreeIndexSpec.setMaxLeafRecords(StarTreeIndexSpec.DEFAULT_MAX_LEAF_RECORDS);

      // Overwrite the null index spec with default one.
      config.enableStarTreeIndex(starTreeIndexSpec);
    }
    //create star builder config from startreeindexspec. Merge these two in one later.
    StarTreeBuilderConfig starTreeBuilderConfig = new StarTreeBuilderConfig();
    starTreeBuilderConfig.setOutDir(starTreeTempDir);
    starTreeBuilderConfig.setSchema(dataSchema);
    starTreeBuilderConfig.setDimensionsSplitOrder(starTreeIndexSpec.getDimensionsSplitOrder());
    starTreeBuilderConfig.setSkipStarNodeCreationDimensions(
        starTreeIndexSpec.getSkipStarNodeCreationForDimensions());
    starTreeBuilderConfig.setSkipMaterializationDimensions(starTreeIndexSpec.getSkipMaterializationForDimensions());
    starTreeBuilderConfig.setSkipMaterializationCardinalityThreshold(
        starTreeIndexSpec.getSkipMaterializationCardinalityThreshold());
    starTreeBuilderConfig.setMaxNumLeafRecords(starTreeIndexSpec.getMaxLeafRecords());
    starTreeBuilderConfig.setExcludeSkipMaterializationDimensionsForStarTreeIndex(
        starTreeIndexSpec.isExcludeSkipMaterializationDimensionsForStarTreeIndex());

    //initialize star tree builder
    try (StarTreeBuilder starTreeBuilder = new OffHeapStarTreeBuilder()) {
      starTreeBuilder.init(starTreeBuilderConfig);
      //build star tree along with collecting stats
      recordReader.rewind();
      LOGGER.info("Start append raw data to star tree builder!");
      totalDocs = 0;
      GenericRow readRow = new GenericRow();
      GenericRow transformedRow = new GenericRow();
      while (recordReader.hasNext()) {
        //PlainFieldExtractor conducts necessary type conversions
        transformedRow = readNextRowSanitized(readRow, transformedRow);
        //must be called after previous step since type conversion for derived values is unnecessary
        populateDefaultDerivedColumnValues(transformedRow);
        starTreeBuilder.append(transformedRow);
        statsCollector.collectRow(transformedRow);
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
      statsCollector.build();
      buildIndexCreationInfo();
      LOGGER.info("Collected stats for {} raw documents, {} aggregated documents", totalRawDocs, totalAggDocs);
      long statCollectionFinishTime = System.currentTimeMillis();

      try {
        // Initialize the index creation using the per-column statistics information
        indexCreator.init(config, segmentIndexCreationInfo, indexCreationInfoMap, dataSchema, tempIndexDir);

        //iterate over the data again,
        Iterator<GenericRow> allRowsIterator = starTreeBuilder.iterator(0,
            starTreeBuilder.getTotalRawDocumentCount() + starTreeBuilder.getTotalAggregateDocumentCount());

        while (allRowsIterator.hasNext()) {
          GenericRow genericRow = allRowsIterator.next();
          indexCreator.indexRow(genericRow);
        }
      } catch (Exception e) {
        indexCreator.close();
        throw e;
      }

      // Serialize the star tree into a file
      starTreeBuilder.serializeTree(new File(tempIndexDir, V1Constants.STAR_TREE_INDEX_FILE), indexCreationInfoMap);

      // Update the dimensions split order and skip materialization dimensions spec so that then can be written into
      // the segment metadata
      starTreeIndexSpec.setDimensionsSplitOrder(starTreeBuilder.getDimensionsSplitOrder());
      starTreeIndexSpec.setSkipMaterializationForDimensions(starTreeBuilder.getSkipMaterializationDimensions());

      //post creation
      handlePostCreation();
      long end = System.currentTimeMillis();
      LOGGER.info("Total time:{} \n star tree build time:{} \n stat collection time:{} \n column index build time:{}",
          (end - start), (starTreeBuildFinishTime - start), statCollectionFinishTime - starTreeBuildFinishTime,
          end - statCollectionFinishTime);
    }
  }

  private void buildRaw()
      throws Exception {
    // Count the number of documents and gather per-column statistics
    LOGGER.debug("Start building StatsCollector!");
    buildIndexCreationInfo();
    LOGGER.info("Finished building StatsCollector!");
    LOGGER.info("Collected stats for {} documents", totalDocs);

    try {
      // Initialize the index creation using the per-column statistics information
      indexCreator.init(config, segmentIndexCreationInfo, indexCreationInfoMap, dataSchema, tempIndexDir);

      // Build the index
      recordReader.rewind();
      LOGGER.info("Start building IndexCreator!");
      GenericRow readRow = new GenericRow();
      GenericRow transformedRow = new GenericRow();
      while (recordReader.hasNext()) {
        long start = System.currentTimeMillis();
        transformedRow = readNextRowSanitized(readRow, transformedRow);
        long stop = System.currentTimeMillis();
        indexCreator.indexRow(transformedRow);
        long stop1 = System.currentTimeMillis();
        totalRecordReadTime += (stop - start);
        totalIndexTime += (stop1 - stop);
      }
    } catch (Exception e) {
      indexCreator.close();
      throw e;
    } finally {
      recordReader.close();
    }
    LOGGER.info("Finished records indexing in IndexCreator!");
    int numErrors, numConversions, numNulls, numNullCols;
    if ((numErrors = extractor.getTotalErrors()) > 0) {
      LOGGER.warn("Index creator for schema {} had {} rows with errors", dataSchema.getSchemaName(), numErrors);
    }
    Map<String, Integer> errorCount = extractor.getErrorCount();
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

  private void handlePostCreation()
      throws Exception {
    final String timeColumn = config.getTimeColumnName();
    segmentName = config.getSegmentNameGenerator().generateSegmentName(segmentStats.getColumnProfileFor(timeColumn));
    updateSegmentStartEndTimeIfNecessary(segmentStats.getColumnProfileFor(timeColumn));

    try {
      // Write the index files to disk
      indexCreator.setSegmentName(segmentName);
      indexCreator.seal();
    } finally {
      indexCreator.close();
    }
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

    // Convert segment format if necessary
    convertFormatIfNeeded(segmentOutputDir);

    // Compute CRC and creation time
    long crc = CrcUtils.forAllFilesInFolder(segmentOutputDir).computeCrc();
    long creationTime;
    String creationTimeInConfig = config.getCreationTime();
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

    // Persist creation metadata to disk
    persistCreationMeta(segmentOutputDir, crc, creationTime);

    LOGGER.info("Driver, record read time : {}", totalRecordReadTime);
    LOGGER.info("Driver, stats collector time : {}", totalStatsCollectorTime);
    LOGGER.info("Driver, indexing time : {}", totalIndexTime);
  }

  private void updateSegmentStartEndTimeIfNecessary(ColumnStatistics timeColumnStats) {
    switch (config.getTimeColumnType()) {
      case EPOCH:
        break;
      case SIMPLE_DATE:
        long startTime = convertStartTimeSDFToMillis(timeColumnStats);
        config.getCustomProperties().put(V1Constants.MetadataKeys.Segment.SEGMENT_START_TIME, String.valueOf(startTime));
        long endTime = convertEndTimeSDFToMillis(timeColumnStats);
        config.getCustomProperties().put(V1Constants.MetadataKeys.Segment.SEGMENT_END_TIME, String.valueOf(endTime));
        break;
    }
  }

  public long convertStartTimeSDFToMillis(ColumnStatistics timeColumnStats) {
    final String minTimeStr = timeColumnStats.getMinValue().toString();
    return convertSDFToMillis(minTimeStr);
  }

  public long convertEndTimeSDFToMillis(ColumnStatistics timeColumnStats) {
    final String maxTimeStr = timeColumnStats.getMaxValue().toString();
    return convertSDFToMillis(maxTimeStr);
  }

  private long convertSDFToMillis(final String colValue) {
    final String sdfFormatStr = config.getSimpleDateFormat();
    DateTimeFormatter sdfFormatter = DateTimeFormat.forPattern(sdfFormatStr);
    DateTime dateTime = DateTime.parse(colValue, sdfFormatter);
    return dateTime.getMillis();
  }

  // Explanation of why we are using format converter:
  // There are 3 options to correctly generate segments to v3 format
  // 1. Generate v3 directly: This is efficient but v3 index writer needs to know buffer size upfront.
  // Inverted, star and raw indexes don't have the index size upfront. This is also least flexible approach
  // if we add more indexes in future.
  // 2. Hold data in-memory: One way to work around predeclaring sizes in (1) is to allocate "large" buffer (2GB?)
  // and hold the data in memory and write the buffer at the end. The memory requirement in this case increases linearly
  // with the number of columns. Variation of that is to mmap data to separate files...which is what we are doing here
  // 3. Another option is to generate dictionary and fwd indexes in v3 and generate inverted, star and raw indexes in
  // separate files. Then add those files to v3 index file. This leads to lot of hodgepodge code to
  // handle multiple segment formats.
  // Using converter is similar to option (2), plus it's battle-tested code. We will roll out with
  // this change to keep changes limited. Once we've migrated we can implement approach (1) with option to
  // copy for indexes for which we don't know sizes upfront.
  private void convertFormatIfNeeded(File segmentDirectory)
      throws Exception {
    SegmentVersion versionToGenerate = config.getSegmentVersion();
    if (versionToGenerate.equals(SegmentVersion.v1)) {
      // v1 by default
      return;
    }
    SegmentFormatConverter converter = SegmentFormatConverterFactory.getConverter(SegmentVersion.v1, SegmentVersion.v3);
    converter.convert(segmentDirectory);
  }

  public ColumnStatistics getColumnStatisticsCollector(final String columnName)
      throws Exception {
    return segmentStats.getColumnProfileFor(columnName);
  }

  public static void persistCreationMeta(File indexDir, long crc, long creationTime) throws IOException {
    File segmentDir = SegmentDirectoryPaths.findSegmentDirectory(indexDir);
    File creationMetaFile = new File(segmentDir, V1Constants.SEGMENT_CREATION_META);
    try (DataOutputStream output = new DataOutputStream(new FileOutputStream(creationMetaFile))) {
      output.writeLong(crc);
      output.writeLong(creationTime);
    }
  }

  /**
   * Complete the stats gathering process and store the stats information in indexCreationInfoMap.
   */
  void buildIndexCreationInfo()
      throws Exception {
    for (FieldSpec spec : dataSchema.getAllFieldSpecs()) {
      String column = spec.getName();
      indexCreationInfoMap.put(column,
          new ColumnIndexCreationInfo(true/*createDictionary*/, segmentStats.getColumnProfileFor(column).getMinValue(),
              segmentStats.getColumnProfileFor(column).getMaxValue(),
              segmentStats.getColumnProfileFor(column).getUniqueValuesSet(), ForwardIndexType.FIXED_BIT_COMPRESSED,
              InvertedIndexType.ROARING_BITMAPS, segmentStats.getColumnProfileFor(column).isSorted(),
              segmentStats.getColumnProfileFor(column).hasNull(),
              segmentStats.getColumnProfileFor(column).getTotalNumberOfEntries(),
              segmentStats.getColumnProfileFor(column).getMaxNumberOfMultiValues(),
              segmentStats.getColumnProfileFor(column).getLengthOfLargestElement(), false/*isAutoGenerated*/,
              segmentStats.getColumnProfileFor(column).getPartitionFunction(),
              segmentStats.getColumnProfileFor(column).getNumPartitions(),
              segmentStats.getColumnProfileFor(column).getPartitionRanges(),
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

  /**
   * Returns the name of the segment associated with this index creation driver.
   */
  @Override
  public String getSegmentName() {
    return segmentName;
  }

  /**
   * Returns the path of the output directory
   */
  @Override
  public File getOutputDirectory() {
    return new File(new File(config.getOutDir()), segmentName);
  }

  private GenericRow readNextRowSanitized(GenericRow readRow, GenericRow transformedRow) throws IOException {
    return extractor.transform(recordReader.next(readRow), transformedRow);
  }

  public SegmentPreIndexStatsContainer getSegmentStats() {
    return segmentStats;
  }
}
