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
package org.apache.pinot.plugin.minion.tasks.refreshsegment;

import java.io.File;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadataCustomMapModifier;
import org.apache.pinot.core.common.MinionConstants;
import org.apache.pinot.core.minion.PinotTaskConfig;
import org.apache.pinot.plugin.minion.tasks.BaseSingleSegmentConversionExecutor;
import org.apache.pinot.plugin.minion.tasks.MinionTaskUtils;
import org.apache.pinot.plugin.minion.tasks.SegmentConversionResult;
import org.apache.pinot.segment.local.indexsegment.immutable.ImmutableSegmentLoader;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.segment.local.segment.index.loader.IndexLoadingConfig;
import org.apache.pinot.segment.local.segment.readers.PinotSegmentRecordReader;
import org.apache.pinot.segment.spi.ColumnMetadata;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
import org.apache.pinot.segment.spi.index.metadata.SegmentMetadataImpl;
import org.apache.pinot.segment.spi.loader.SegmentDirectoryLoaderContext;
import org.apache.pinot.segment.spi.loader.SegmentDirectoryLoaderRegistry;
import org.apache.pinot.segment.spi.store.SegmentDirectory;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class RefreshSegmentTaskExecutor extends BaseSingleSegmentConversionExecutor {
  private static final Logger LOGGER = LoggerFactory.getLogger(RefreshSegmentTaskExecutor.class);

  private long _taskStartTime;

  /**
   * The code here currently covers segment refresh for the following cases:
   * 1. Process newly added columns.
   * 2. Addition/removal of indexes.
   * 3. Compatible datatype change for existing columns
   */
  @Override
  protected SegmentConversionResult convert(PinotTaskConfig pinotTaskConfig, File indexDir, File workingDir)
      throws Exception {
    _eventObserver.notifyProgress(pinotTaskConfig, "Refreshing segment: " + indexDir);

    // We set _taskStartTime before fetching the tableConfig. Task Generation relies on tableConfig/Schema updates
    // happening after the last processed time. So we explicity use the timestamp before fetching tableConfig as the
    // processedTime.
    _taskStartTime = System.currentTimeMillis();
    Map<String, String> configs = pinotTaskConfig.getConfigs();
    String tableNameWithType = configs.get(MinionConstants.TABLE_NAME_KEY);
    String segmentName = configs.get(MinionConstants.SEGMENT_NAME_KEY);
    String taskType = pinotTaskConfig.getTaskType();

    LOGGER.info("Starting task: {} with configs: {}", taskType, configs);

    TableConfig tableConfig = getTableConfig(tableNameWithType);
    Schema schema = getSchema(tableNameWithType);

    IndexLoadingConfig indexLoadingConfig = new IndexLoadingConfig(tableConfig, schema);
    SegmentMetadataImpl segmentMetadata = new SegmentMetadataImpl(indexDir);
    PinotConfiguration segmentDirectoryConfigs = indexLoadingConfig.getSegmentDirectoryConfigs();
    SegmentDirectoryLoaderContext segmentLoaderContext =
        new SegmentDirectoryLoaderContext.Builder().setTableConfig(indexLoadingConfig.getTableConfig())
            .setSchema(schema)
            .setInstanceId(indexLoadingConfig.getInstanceId())
            .setSegmentName(segmentMetadata.getName())
            .setSegmentCrc(segmentMetadata.getCrc())
            .setSegmentDirectoryConfigs(segmentDirectoryConfigs)
            .build();
    SegmentDirectory segmentDirectory =
        SegmentDirectoryLoaderRegistry.getDefaultSegmentDirectoryLoader().load(indexDir.toURI(), segmentLoaderContext);

    // TODO: Instead of relying on needPreprocess(), process segment metadata file to determine if refresh is needed.
    // BaseDefaultColumnHandler part of needPreprocess() does not process any changes to existing columns like datatype,
    // change from dimension to metric, etc.
    boolean needPreprocess = ImmutableSegmentLoader.needPreprocess(segmentDirectory, indexLoadingConfig);
    closeSegmentDirectoryQuietly(segmentDirectory);
    Set<String> refreshColumnSet = new HashSet<>();

    for (FieldSpec fieldSpecInSchema : schema.getAllFieldSpecs()) {
      // Virtual columns are constructed while loading the segment, thus do not exist in the record, nor should be
      // persisted to the disk.
      if (fieldSpecInSchema.isVirtualColumn()) {
        continue;
      }

      String column = fieldSpecInSchema.getName();
      ColumnMetadata columnMetadata = segmentMetadata.getColumnMetadataFor(column);
      if (columnMetadata != null) {
        FieldSpec fieldSpecInSegment = columnMetadata.getFieldSpec();

        // Check the data type and default value matches.
        FieldSpec.DataType dataTypeInSegment = fieldSpecInSegment.getDataType();
        FieldSpec.DataType dataTypeInSchema = fieldSpecInSchema.getDataType();

        // Column exists in segment.
        if (dataTypeInSegment != dataTypeInSchema) {
          // Check if we need to update the data-type. DataType change is dependent on segmentGeneration code converting
          // the object to the destination datatype. If the existing data is the column is not compatible with the
          // destination data-type, the refresh task will fail.
          refreshColumnSet.add(column);
        }

        // TODO: Maybe we can support singleValue to multi-value conversions are supproted and vice-versa.
      } else {
        refreshColumnSet.add(column);
      }
    }

    if (!needPreprocess && refreshColumnSet.isEmpty()) {
      LOGGER.info("Skipping segment={}, table={} as it is up-to-date with new table/schema", segmentName,
          tableNameWithType);
      // We just need to update the ZK metadata with the last refresh time to avoid getting picked up again. As the CRC
      // check will match, this will only end up being a ZK update.
      return new SegmentConversionResult.Builder().setTableNameWithType(tableNameWithType)
          .setFile(indexDir)
          .setSegmentName(segmentName)
          .build();
    }

    // Refresh the segment. Segment reload is achieved by generating a new segment from scratch using the updated schema
    // and table configs.
    try (PinotSegmentRecordReader recordReader = new PinotSegmentRecordReader()) {
      recordReader.init(indexDir, null, null);
      SegmentGeneratorConfig config = getSegmentGeneratorConfig(workingDir, tableConfig, segmentMetadata, segmentName,
          getSchema(tableNameWithType));
      SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
      driver.init(config, recordReader);
      driver.build();
    }

    File refreshedSegmentFile = new File(workingDir, segmentName);
    SegmentConversionResult result = new SegmentConversionResult.Builder().setFile(refreshedSegmentFile)
        .setTableNameWithType(tableNameWithType)
        .setSegmentName(segmentName)
        .build();

    long endMillis = System.currentTimeMillis();
    LOGGER.info("Finished task: {} with configs: {}. Total time: {}ms", taskType, configs,
        (endMillis - _taskStartTime));

    return result;
  }

  private static SegmentGeneratorConfig getSegmentGeneratorConfig(File workingDir, TableConfig tableConfig,
      SegmentMetadataImpl segmentMetadata, String segmentName, Schema schema) {
    // Inverted index creation is disabled by default during segment generation typically to reduce segment push times
    // from external sources like HDFS. Also, not creating an inverted index here, the segment will always be flagged as
    // needReload, causing the segment refresh to take place.
    tableConfig.getIndexingConfig().setCreateInvertedIndexDuringSegmentGeneration(true);
    SegmentGeneratorConfig config = new SegmentGeneratorConfig(tableConfig, schema);
    config.setOutDir(workingDir.getPath());
    config.setSegmentName(segmentName);

    // Keep index creation time the same as original segment because both segments use the same raw data.
    // This way, for REFRESH case, when new segment gets pushed to controller, we can use index creation time to
    // identify if the new pushed segment has newer data than the existing one.
    config.setCreationTime(String.valueOf(segmentMetadata.getIndexCreationTime()));

    // The time column type info is not stored in the segment metadata.
    // Keep segment start/end time to properly handle time column type other than EPOCH (e.g.SIMPLE_FORMAT).
    if (segmentMetadata.getTimeInterval() != null) {
      config.setTimeColumnName(tableConfig.getValidationConfig().getTimeColumnName());
      config.setStartTime(Long.toString(segmentMetadata.getStartTime()));
      config.setEndTime(Long.toString(segmentMetadata.getEndTime()));
      config.setSegmentTimeUnit(segmentMetadata.getTimeUnit());
    }
    return config;
  }

  private static void closeSegmentDirectoryQuietly(SegmentDirectory segmentDirectory) {
    if (segmentDirectory != null) {
      try {
        segmentDirectory.close();
      } catch (Exception e) {
        LOGGER.warn("Failed to close SegmentDirectory due to error: {}", e.getMessage());
      }
    }
  }

  @Override
  protected SegmentZKMetadataCustomMapModifier getSegmentZKMetadataCustomMapModifier(PinotTaskConfig pinotTaskConfig,
      SegmentConversionResult segmentConversionResult) {
    return new SegmentZKMetadataCustomMapModifier(SegmentZKMetadataCustomMapModifier.ModifyMode.UPDATE,
        Collections.singletonMap(MinionConstants.RefreshSegmentTask.TASK_TYPE + MinionConstants.TASK_TIME_SUFFIX,
            MinionTaskUtils.toUTCString(_taskStartTime)));
  }
}
