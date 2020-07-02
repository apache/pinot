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
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.core.data.readers.PinotSegmentRecordReader;
import org.apache.pinot.core.data.recordtransformer.CompositeTransformer;
import org.apache.pinot.core.data.recordtransformer.RecordTransformer;
import org.apache.pinot.core.indexsegment.generator.SegmentGeneratorConfig;
import org.apache.pinot.core.indexsegment.generator.SegmentVersion;
import org.apache.pinot.core.segment.creator.ColumnIndexCreationInfo;
import org.apache.pinot.core.segment.creator.ColumnStatistics;
import org.apache.pinot.core.segment.creator.ForwardIndexType;
import org.apache.pinot.core.segment.creator.InvertedIndexType;
import org.apache.pinot.core.segment.creator.RecordReaderSegmentCreationDataSource;
import org.apache.pinot.core.segment.creator.SegmentCreationDataSource;
import org.apache.pinot.core.segment.creator.SegmentCreator;
import org.apache.pinot.core.segment.creator.SegmentIndexCreationDriver;
import org.apache.pinot.core.segment.creator.SegmentIndexCreationInfo;
import org.apache.pinot.core.segment.creator.SegmentPreIndexStatsContainer;
import org.apache.pinot.core.segment.creator.StatsCollectorConfig;
import org.apache.pinot.core.segment.index.converter.SegmentFormatConverter;
import org.apache.pinot.core.segment.index.converter.SegmentFormatConverterFactory;
import org.apache.pinot.core.segment.store.SegmentDirectoryPaths;
import org.apache.pinot.core.startree.v2.builder.MultipleTreesBuilder;
import org.apache.pinot.core.startree.v2.builder.StarTreeV2BuilderConfig;
import org.apache.pinot.core.util.CrcUtils;
import org.apache.pinot.core.util.SchemaUtils;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.FileFormat;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.data.readers.RecordReader;
import org.apache.pinot.spi.data.readers.RecordReaderFactory;
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
  private RecordTransformer _recordTransformer;
  private int totalDocs = 0;
  private File tempIndexDir;
  private String segmentName;
  private long totalRecordReadTime = 0;
  private long totalIndexTime = 0;
  private long totalStatsCollectorTime = 0;

  @Override
  public void init(SegmentGeneratorConfig config)
      throws Exception {
    init(config, getRecordReader(config));
  }

  private RecordReader getRecordReader(SegmentGeneratorConfig segmentGeneratorConfig)
      throws Exception {
    File dataFile = new File(segmentGeneratorConfig.getInputFilePath());
    Preconditions.checkState(dataFile.exists(), "Input file: " + dataFile.getAbsolutePath() + " does not exist");

    Schema schema = segmentGeneratorConfig.getSchema();
    FileFormat fileFormat = segmentGeneratorConfig.getFormat();
    String recordReaderClassName = segmentGeneratorConfig.getRecordReaderPath();
    Set<String> sourceFields = SchemaUtils.extractSourceFields(segmentGeneratorConfig.getSchema());

    // Allow for instantiation general record readers from a record reader path passed into segment generator config
    // If this is set, this will override the file format
    if (recordReaderClassName != null) {
      if (fileFormat != FileFormat.OTHER) {
        // NOTE: we currently have default file format set to AVRO inside segment generator config, do not want to break
        // this behavior for clients.
        LOGGER.warn("Using class: {} to read segment, ignoring configured file format: {}", recordReaderClassName,
            fileFormat);
      }
      return RecordReaderFactory.getRecordReaderByClass(recordReaderClassName, dataFile, sourceFields,
          segmentGeneratorConfig.getReaderConfig());
    }

    // NOTE: PinotSegmentRecordReader does not support time conversion (field spec must match)
    if (fileFormat == FileFormat.PINOT) {
      return new PinotSegmentRecordReader(dataFile, schema, segmentGeneratorConfig.getColumnSortOrder());
    } else {
      return RecordReaderFactory
          .getRecordReader(fileFormat, dataFile, sourceFields, segmentGeneratorConfig.getReaderConfig());
    }
  }

  public void init(SegmentGeneratorConfig config, RecordReader recordReader) {
    init(config, new RecordReaderSegmentCreationDataSource(recordReader),
        CompositeTransformer.getDefaultTransformer(config.getSchema()));
  }

  public void init(SegmentGeneratorConfig config, SegmentCreationDataSource dataSource,
      RecordTransformer recordTransformer) {
    this.config = config;
    recordReader = dataSource.getRecordReader();
    Preconditions.checkState(recordReader.hasNext(), "No record in data source");
    dataSchema = config.getSchema();

    _recordTransformer = recordTransformer;

    // Initialize stats collection
    segmentStats = dataSource.gatherStats(new StatsCollectorConfig(dataSchema, config.getSegmentPartitionConfig()));
    totalDocs = segmentStats.getTotalDocCount();

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
    tempIndexDir = new File(indexDir, org.apache.pinot.common.utils.FileUtils.getRandomFileName());
    LOGGER.debug("tempIndexDir:{}", tempIndexDir);
  }

  @Override
  public void build()
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
      GenericRow reuse = new GenericRow();
      while (recordReader.hasNext()) {
        long start = System.currentTimeMillis();
        reuse.clear();
        GenericRow transformedRow = _recordTransformer.transform(recordReader.next(reuse));
        long stop = System.currentTimeMillis();
        totalRecordReadTime += (stop - start);
        if (transformedRow != null) {
          indexCreator.indexRow(transformedRow);
          long stop1 = System.currentTimeMillis();
          totalIndexTime += (stop1 - stop);
        }
      }
    } catch (Exception e) {
      indexCreator.close();
      throw e;
    } finally {
      recordReader.close();
    }
    LOGGER.info("Finished records indexing in IndexCreator!");

    handlePostCreation();
  }

  private void handlePostCreation()
      throws Exception {
    ColumnStatistics timeColumnStatistics = segmentStats.getColumnProfileFor(config.getTimeColumnName());
    int sequenceId = config.getSequenceId();
    if (timeColumnStatistics != null) {
      segmentName = config.getSegmentNameGenerator()
          .generateSegmentName(sequenceId, timeColumnStatistics.getMinValue(), timeColumnStatistics.getMaxValue());
    } else {
      segmentName = config.getSegmentNameGenerator().generateSegmentName(sequenceId, null, null);
    }

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

    // Build star-tree V2 if necessary
    buildStarTreeV2IfNecessary(segmentOutputDir);

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

  private void buildStarTreeV2IfNecessary(File indexDir)
      throws Exception {
    List<StarTreeV2BuilderConfig> starTreeV2BuilderConfigs = new ArrayList<>();
    if (config.getStarTreeV2BuilderConfigs() != null) {
      starTreeV2BuilderConfigs.addAll(config.getStarTreeV2BuilderConfigs());
    }
    // Append the default star-tree after the customized star-trees if enabled
    if (config.isEnableDefaultStarTree()) {
      starTreeV2BuilderConfigs.add(null);
    }
    if (!starTreeV2BuilderConfigs.isEmpty()) {
      MultipleTreesBuilder.BuildMode buildMode =
          config.isOnHeap() ? MultipleTreesBuilder.BuildMode.ON_HEAP : MultipleTreesBuilder.BuildMode.OFF_HEAP;
      new MultipleTreesBuilder(starTreeV2BuilderConfigs, indexDir, buildMode).build();
    }
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

  public static void persistCreationMeta(File indexDir, long crc, long creationTime)
      throws IOException {
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
    for (FieldSpec fieldSpec : dataSchema.getAllFieldSpecs()) {
      // Ignore virtual columns
      if (fieldSpec.isVirtualColumn()) {
        continue;
      }

      String columnName = fieldSpec.getName();
      ColumnStatistics columnProfile = segmentStats.getColumnProfileFor(columnName);
      Set<String> varLengthDictionaryColumns = new HashSet<>(config.getVarLengthDictionaryColumns());
      indexCreationInfoMap.put(columnName, new ColumnIndexCreationInfo(columnProfile, true/*createDictionary*/,
          varLengthDictionaryColumns.contains(columnName), ForwardIndexType.FIXED_BIT_COMPRESSED,
          InvertedIndexType.ROARING_BITMAPS, false/*isAutoGenerated*/,
          dataSchema.getFieldSpecFor(columnName).getDefaultNullValue()));
    }
    segmentIndexCreationInfo.setTotalDocs(totalDocs);
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

  public SegmentPreIndexStatsContainer getSegmentStats() {
    return segmentStats;
  }
}
