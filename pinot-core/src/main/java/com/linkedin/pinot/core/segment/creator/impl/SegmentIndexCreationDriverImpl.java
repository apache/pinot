/**
 * Copyright (C) 2014-2015 LinkedIn Corp. (pinot-core@linkedin.com)
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

import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.pinot.common.data.FieldSpec;
import com.linkedin.pinot.common.data.Schema;
import com.linkedin.pinot.common.utils.SegmentNameBuilder;
import com.linkedin.pinot.core.data.GenericRow;
import com.linkedin.pinot.core.data.readers.RecordReader;
import com.linkedin.pinot.core.data.readers.RecordReaderFactory;
import com.linkedin.pinot.core.indexsegment.generator.SegmentGeneratorConfig;
import com.linkedin.pinot.core.segment.creator.ColumnIndexCreationInfo;
import com.linkedin.pinot.core.segment.creator.ForwardIndexType;
import com.linkedin.pinot.core.segment.creator.InvertedIndexType;
import com.linkedin.pinot.core.segment.creator.SegmentCreator;
import com.linkedin.pinot.core.segment.creator.SegmentIndexCreationDriver;
import com.linkedin.pinot.core.segment.creator.SegmentPreIndexStatsCollector;
import com.linkedin.pinot.core.segment.creator.impl.stats.SegmentPreIndexStatsCollectorImpl;
import com.linkedin.pinot.core.util.CrcUtils;


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
  Schema dataSchema;
  int totalDocs;
  File tempIndexDir;
  String segmentName;
  long totalRecordReadTime = 0;
  long totalIndexTime = 0;
  long totalStatsCollectorTime = 0;

  @Override
  public void init(SegmentGeneratorConfig config) throws Exception {
    init(config, RecordReaderFactory.get(config));
  }

  public void init(SegmentGeneratorConfig config, RecordReader reader) throws Exception {
    this.config = config;
    // Initialize the record reader
    recordReader = reader;
    recordReader.init();
    dataSchema = recordReader.getSchema();

    // Initialize stats collection
    statsCollector = new SegmentPreIndexStatsCollectorImpl(recordReader.getSchema());
    statsCollector.init();

    // Initialize index creation
    indexCreationInfoMap = new HashMap<String, ColumnIndexCreationInfo>();
    indexCreator = new SegmentColumnarIndexCreator();

    // Ensure that the output directory exists
    final File indexDir = new File(config.getIndexOutputDir());
    if (!indexDir.exists()) {
      indexDir.mkdirs();
    }

    // Create a temporary directory used in segment creation
    tempIndexDir = new File(indexDir, com.linkedin.pinot.common.utils.FileUtils.getRandomFileName());
  }

  @Override
  public void build() throws Exception {
    // Count the number of documents and gather per-column statistics
    LOGGER.info("Start building StatsCollector!");
    totalDocs = 0;
    GenericRow row;
    long start;
    long stop;
    long stop1;
    while (recordReader.hasNext()) {
      totalDocs++;
      start = System.currentTimeMillis();
      row = recordReader.next();
      stop = System.currentTimeMillis();
      //TODO:  ?
      statsCollector.collectRow(row);
      stop1 = System.currentTimeMillis();
      totalRecordReadTime += (stop - start);
      totalStatsCollectorTime += (stop1 - stop);
    }
    buildIndexCreationInfo();
    LOGGER.info("Finished building StatsCollector!");
    LOGGER.info("Collected stats for {} documents", totalDocs);

    // Initialize the index creation using the per-column statistics information
    indexCreator.init(config, indexCreationInfoMap, dataSchema, totalDocs, tempIndexDir);

    // Build the index
    recordReader.rewind();
    LOGGER.info("Start building IndexCreator!");
    while (recordReader.hasNext()) {
      start = System.currentTimeMillis();
      row = recordReader.next();
      stop = System.currentTimeMillis();
      indexCreator.indexRow(row);
      stop1 = System.currentTimeMillis();
      totalRecordReadTime += (stop - start);
      totalIndexTime += (stop1 - stop);
    }
    recordReader.close();
    LOGGER.info("Finished records indexing in IndexCreator!");

    // Build the segment name, if necessary
    final String timeColumn = config.getTimeColumnName();

    if (config.getSegmentName() != null) {
      segmentName = config.getSegmentName();
    } else {
      if (timeColumn != null && timeColumn.length() > 0) {
        final Object minTimeValue = statsCollector.getColumnProfileFor(timeColumn).getMinValue();
        final Object maxTimeValue = statsCollector.getColumnProfileFor(timeColumn).getMaxValue();
        segmentName =
            SegmentNameBuilder.buildBasic(config.getTableName(), minTimeValue, maxTimeValue,
                config.getSegmentNamePostfix());
      } else {
        segmentName =
            SegmentNameBuilder.buildBasic(config.getTableName(), config.getSegmentNamePostfix());
      }
    }

    // Write the index files to disk
    indexCreator.setSegmentName(segmentName);
    indexCreator.seal();
    LOGGER.info("Finished segment seal!");

    // Delete the directory named after the segment name, if it exists
    final File outputDir = new File(config.getIndexOutputDir());
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

    LOGGER.info("Driver, record read time : {}", totalRecordReadTime);
    LOGGER.info("Driver, stats collector time : {}", totalStatsCollectorTime);
    LOGGER.info("Driver, indexing time : {}", totalIndexTime);
  }

  public void ovveriteSegmentName(String segmentName) {
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
  void buildIndexCreationInfo() throws Exception {
    statsCollector.build();
    for (final FieldSpec spec : dataSchema.getAllFieldSpecs()) {
      final String column = spec.getName();
      indexCreationInfoMap.put(
          column,
          new ColumnIndexCreationInfo(true, // Use dictionary encoding
              statsCollector.getColumnProfileFor(column).getMinValue(), statsCollector.getColumnProfileFor(column)
                  .getMaxValue(), statsCollector.getColumnProfileFor(column).getUniqueValuesSet(),
              ForwardIndexType.FIXED_BIT_COMPRESSED, InvertedIndexType.P4_DELTA, statsCollector.getColumnProfileFor(
                  column).isSorted(), statsCollector.getColumnProfileFor(column).hasNull(), statsCollector
                  .getColumnProfileFor(column).getTotalNumberOfEntries(), statsCollector.getColumnProfileFor(column)
                  .getMaxNumberOfMultiValues()));
    }
  }

  @Override
  /**
   * Returns the name of the segment associated with this index creation driver.
   */
  public String getSegmentName() {
    return segmentName;
  }

}
