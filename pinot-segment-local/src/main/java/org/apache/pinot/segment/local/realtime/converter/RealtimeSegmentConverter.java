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
package org.apache.pinot.segment.local.realtime.converter;

import com.google.common.annotations.VisibleForTesting;
import java.util.List;
import java.util.Map;
import org.apache.pinot.common.metrics.ServerGauge;
import org.apache.pinot.common.metrics.ServerMeter;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.segment.local.indexsegment.mutable.MutableSegmentImpl;
import org.apache.pinot.segment.local.realtime.converter.stats.RealtimeSegmentSegmentCreationDataSource;
import org.apache.pinot.segment.local.segment.creator.TransformPipeline;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.segment.local.segment.readers.CompactedPinotSegmentRecordReader;
import org.apache.pinot.segment.local.segment.readers.PinotSegmentRecordReader;
import org.apache.pinot.segment.local.utils.TableConfigUtils;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
import org.apache.pinot.segment.spi.creator.SegmentVersion;
import org.apache.pinot.spi.config.table.ColumnPartitionConfig;
import org.apache.pinot.spi.config.table.SegmentPartitionConfig;
import org.apache.pinot.spi.config.table.SegmentZKPropsConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.Schema;


public class RealtimeSegmentConverter {
  private final MutableSegmentImpl _realtimeSegmentImpl;
  private final SegmentZKPropsConfig _segmentZKPropsConfig;
  private final String _outputPath;
  private final Schema _dataSchema;
  private final String _tableName;
  private final TableConfig _tableConfig;
  private final String _segmentName;
  private final boolean _nullHandlingEnabled;
  private final boolean _enableColumnMajor;

  public RealtimeSegmentConverter(MutableSegmentImpl realtimeSegment, SegmentZKPropsConfig segmentZKPropsConfig,
      String outputPath, Schema schema, String tableName, TableConfig tableConfig, String segmentName,
      boolean nullHandlingEnabled) {
    _realtimeSegmentImpl = realtimeSegment;
    _segmentZKPropsConfig = segmentZKPropsConfig;
    _outputPath = outputPath;
    _dataSchema = getUpdatedSchema(schema);
    _tableName = tableName;
    _tableConfig = tableConfig;
    _segmentName = segmentName;
    _nullHandlingEnabled = nullHandlingEnabled;
    if (_tableConfig.getIngestionConfig() != null
        && _tableConfig.getIngestionConfig().getStreamIngestionConfig() != null) {
      _enableColumnMajor =
          _tableConfig.getIngestionConfig().getStreamIngestionConfig().getColumnMajorSegmentBuilderEnabled();
    } else {
      _enableColumnMajor = _tableConfig.getIndexingConfig().isColumnMajorSegmentBuilderEnabled();
    }
  }

  public void build(SegmentVersion segmentVersion, ServerMetrics serverMetrics)
      throws Exception {
    SegmentGeneratorConfig genConfig = new SegmentGeneratorConfig(_tableConfig, _dataSchema, true);

    // The segment generation code in SegmentColumnarIndexCreator will throw
    // exception if start and end time in time column are not in acceptable
    // range. We don't want the realtime consumption to stop (if an exception
    // is thrown) and thus the time validity check is explicitly disabled for
    // realtime segment generation
    genConfig.setSegmentTimeValueCheck(false);

    if (segmentVersion != null) {
      genConfig.setSegmentVersion(segmentVersion);
    }
    genConfig.setTableName(_tableName);
    genConfig.setOutDir(_outputPath);
    genConfig.setSegmentName(_segmentName);
    SegmentPartitionConfig segmentPartitionConfig = _realtimeSegmentImpl.getSegmentPartitionConfig();
    genConfig.setSegmentPartitionConfig(segmentPartitionConfig);
    genConfig.setDefaultNullHandlingEnabled(_nullHandlingEnabled);
    genConfig.setSegmentZKPropsConfig(_segmentZKPropsConfig);

    // flush any artifacts to disk to improve mutable to immutable segment conversion
    _realtimeSegmentImpl.commit();

    SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();

    // Check if commit-time compaction is enabled for upsert tables
    boolean useCompactedReader = TableConfigUtils.isCommitTimeCompactionEnabled(_tableConfig);

    String sortedColumn = null;
    List<String> columnSortOrder = genConfig.getColumnSortOrder();
    if (columnSortOrder != null && !columnSortOrder.isEmpty()) {
      sortedColumn = columnSortOrder.get(0);
    }
    int[] sortedDocIds =
        sortedColumn != null ? _realtimeSegmentImpl.getSortedDocIdIterationOrderWithSortedColumn(sortedColumn) : null;

    if (useCompactedReader) {
      // Collect metrics for commit-time compaction
      long compactionStartTime = System.currentTimeMillis();
      int preCompactionRowCount = _realtimeSegmentImpl.getNumDocsIndexed();
      // Track that commit-time compaction is enabled for this segment
      if (serverMetrics != null) {
        serverMetrics.addMeteredTableValue(_tableName, ServerMeter.COMMIT_TIME_COMPACTION_ENABLED_SEGMENTS, 1L);
        serverMetrics.addMeteredTableValue(_tableName, ServerMeter.COMMIT_TIME_COMPACTION_ROWS_PRE_COMPACTION,
            preCompactionRowCount);
      }

      // Use CompactedPinotSegmentRecordReader to remove obsolete records
      try (CompactedPinotSegmentRecordReader recordReader = new CompactedPinotSegmentRecordReader(
          _realtimeSegmentImpl.getValidDocIds(), _realtimeSegmentImpl.getDeleteRecordColumn())) {
        recordReader.init(_realtimeSegmentImpl, sortedDocIds);
        RealtimeSegmentSegmentCreationDataSource dataSource =
            new RealtimeSegmentSegmentCreationDataSource(_realtimeSegmentImpl, recordReader, sortedDocIds);
        driver.init(genConfig, dataSource, TransformPipeline.getPassThroughPipeline(_tableName)); // initializes reader

        if (!_enableColumnMajor) {
          driver.build();
        } else {
          driver.buildByColumn(_realtimeSegmentImpl);
        }
      }

      // Collect and publish post-compaction metrics
      if (serverMetrics != null) {
        publishCompactionMetrics(serverMetrics, preCompactionRowCount, driver, compactionStartTime);
      }
    } else {
      // Use regular PinotSegmentRecordReader (existing behavior)
      try (PinotSegmentRecordReader recordReader = new PinotSegmentRecordReader()) {
        recordReader.init(_realtimeSegmentImpl, sortedDocIds);
        RealtimeSegmentSegmentCreationDataSource dataSource =
            new RealtimeSegmentSegmentCreationDataSource(_realtimeSegmentImpl, recordReader);
        driver.init(genConfig, dataSource, TransformPipeline.getPassThroughPipeline(_tableName)); // initializes reader

        if (!_enableColumnMajor) {
          driver.build();
        } else {
          driver.buildByColumn(_realtimeSegmentImpl);
        }
      }
    }

    if (segmentPartitionConfig != null && serverMetrics != null) {
      Map<String, ColumnPartitionConfig> columnPartitionMap = segmentPartitionConfig.getColumnPartitionMap();
      for (String columnName : columnPartitionMap.keySet()) {
        int numPartitions = driver.getSegmentStats().getColumnProfileFor(columnName).getPartitions().size();
        serverMetrics.addValueToTableGauge(_tableName, ServerGauge.REALTIME_SEGMENT_NUM_PARTITIONS, numPartitions);
      }
    }
  }

  /**
   * Publishes detailed commit-time compaction metrics
   */
  private void publishCompactionMetrics(ServerMetrics serverMetrics, int preCompactionRowCount,
      SegmentIndexCreationDriverImpl driver, long compactionStartTime) {
    try {
      int postCompactionRowCount = driver.getSegmentStats().getTotalDocCount();
      long compactionProcessingTime = System.currentTimeMillis() - compactionStartTime;
      int rowsRemoved = preCompactionRowCount - postCompactionRowCount;

      // Publish basic row count metrics
      serverMetrics.addMeteredTableValue(_tableName, ServerMeter.COMMIT_TIME_COMPACTION_ROWS_POST_COMPACTION,
          postCompactionRowCount);
      serverMetrics.addMeteredTableValue(_tableName, ServerMeter.COMMIT_TIME_COMPACTION_ROWS_REMOVED, rowsRemoved);
      serverMetrics.addMeteredTableValue(_tableName, ServerMeter.COMMIT_TIME_COMPACTION_BUILD_TIME_MS,
          compactionProcessingTime);

      // Calculate and publish compaction ratio percentage (only if we had rows to compact)
      if (preCompactionRowCount > 0) {
        double compactionRatioPercent = (double) rowsRemoved / preCompactionRowCount * 100.0;
        serverMetrics.setOrUpdateTableGauge(_tableName, ServerGauge.COMMIT_TIME_COMPACTION_RATIO_PERCENT,
            (long) compactionRatioPercent);
      }
    } catch (Exception e) {
      //no-op.
    }
  }

  /**
   * Returns a new schema containing only physical columns
   */
  @VisibleForTesting
  public static Schema getUpdatedSchema(Schema original) {
    return original.withoutVirtualColumns();
  }

  public boolean isColumnMajorEnabled() {
    return _enableColumnMajor;
  }
}
