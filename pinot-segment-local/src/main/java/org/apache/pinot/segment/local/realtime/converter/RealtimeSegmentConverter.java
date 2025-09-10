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
import javax.annotation.Nullable;
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
import org.apache.pinot.segment.spi.index.mutable.ThreadSafeMutableRoaringBitmap;
import org.apache.pinot.spi.config.table.ColumnPartitionConfig;
import org.apache.pinot.spi.config.table.SegmentPartitionConfig;
import org.apache.pinot.spi.config.table.SegmentZKPropsConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.RecordReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class RealtimeSegmentConverter {
  private static final Logger LOGGER = LoggerFactory.getLogger(RealtimeSegmentConverter.class);

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

  public void build(@Nullable SegmentVersion segmentVersion, @Nullable ServerMetrics serverMetrics)
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

    long compactionStartTime = System.currentTimeMillis();
    int preCommitRowCount = _realtimeSegmentImpl.getNumDocsIndexed();

    if (useCompactedReader) {
      // Take a snapshot of validDocIds at the beginning of conversion to ensure consistency
      ThreadSafeMutableRoaringBitmap validDocIdsSnapshot = getValidDocIdSnapshot();
      if (validDocIdsSnapshot == null) {
        throw new IllegalStateException("Cannot use CompactedPinotSegmentRecordReader without valid document IDs. "
            + "Segment may be corrupted.");
      }
      // Use CompactedPinotSegmentRecordReader to remove obsolete/invalidated records
      try (CompactedPinotSegmentRecordReader recordReader = new CompactedPinotSegmentRecordReader(
          validDocIdsSnapshot)) {
        recordReader.init(_realtimeSegmentImpl, sortedDocIds);
        buildSegmentWithReader(driver, genConfig, recordReader, sortedDocIds, useCompactedReader, validDocIdsSnapshot);
        publishCompactionMetrics(serverMetrics, preCommitRowCount, driver, compactionStartTime);
      }
    } else {
      // Use regular PinotSegmentRecordReader (existing behavior)
      try (PinotSegmentRecordReader recordReader = new PinotSegmentRecordReader()) {
        recordReader.init(_realtimeSegmentImpl, sortedDocIds);
        buildSegmentWithReader(driver, genConfig, recordReader, sortedDocIds, useCompactedReader, null);
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

  private @Nullable ThreadSafeMutableRoaringBitmap getValidDocIdSnapshot() {
    ThreadSafeMutableRoaringBitmap validDocIdsSnapshot = null;
    if (_realtimeSegmentImpl.getValidDocIds() != null) {
      validDocIdsSnapshot = new ThreadSafeMutableRoaringBitmap(
          _realtimeSegmentImpl.getValidDocIds().getMutableRoaringBitmap());
    }
    return validDocIdsSnapshot;
  }

  /**
   * Publishes segment build metrics including common metrics (always published) and compaction-specific metrics
   * (published only when compaction is enabled)
   */
  private void publishCompactionMetrics(@Nullable ServerMetrics serverMetrics,
      int preCommitRowCount, SegmentIndexCreationDriverImpl driver, long buildStartTime) {
    if (serverMetrics == null) {
      return;
    }
    try {
      int postCommitRowCount = driver.getSegmentStats().getTotalDocCount();
      long buildProcessingTime = System.currentTimeMillis() - buildStartTime;

      int rowsRemoved = preCommitRowCount - postCommitRowCount;

      // Only publish compaction-specific metrics when compaction is actually enabled
      serverMetrics.addMeteredTableValue(_tableName, ServerMeter.COMMIT_TIME_COMPACTION_ENABLED_SEGMENTS, 1L);
      serverMetrics.addMeteredTableValue(_tableName, ServerMeter.COMMIT_TIME_COMPACTION_ROWS_PRE_COMPACTION,
          preCommitRowCount);
      serverMetrics.addMeteredTableValue(_tableName, ServerMeter.COMMIT_TIME_COMPACTION_ROWS_POST_COMPACTION,
          postCommitRowCount);
      serverMetrics.addMeteredTableValue(_tableName, ServerMeter.COMMIT_TIME_COMPACTION_ROWS_REMOVED, rowsRemoved);
      serverMetrics.addMeteredTableValue(_tableName, ServerMeter.COMMIT_TIME_COMPACTION_BUILD_TIME_MS,
          buildProcessingTime);

      // Calculate and publish compaction ratio percentage (only if we had rows to compact)
      if (preCommitRowCount > 0) {
        double compactionRatioPercent = (double) rowsRemoved / preCommitRowCount * 100.0;
        serverMetrics.setOrUpdateTableGauge(_tableName, ServerGauge.COMMIT_TIME_COMPACTION_RATIO_PERCENT,
            (long) compactionRatioPercent);
      }
    } catch (Exception e) {
      LOGGER.warn("Failed to publish segment build metrics for table: {}, segment: {}", _tableName,
          _segmentName, e);
    }
  }

  /**
   * Common method to build segment with the provided record reader
   */
  private void buildSegmentWithReader(SegmentIndexCreationDriverImpl driver, SegmentGeneratorConfig genConfig,
      RecordReader recordReader, int[] sortedDocIds, boolean useCompactedReader,
      @Nullable ThreadSafeMutableRoaringBitmap validDocIdsSnapshot)
      throws Exception {
    RealtimeSegmentSegmentCreationDataSource dataSource;
    if (useCompactedReader) {
      // For compacted readers, use the constructor that takes sortedDocIds and pass the validDocIds snapshot
      dataSource = new RealtimeSegmentSegmentCreationDataSource(_realtimeSegmentImpl, recordReader, sortedDocIds,
          validDocIdsSnapshot);
    } else {
      // For regular readers, use the original constructor
      dataSource =
          new RealtimeSegmentSegmentCreationDataSource(_realtimeSegmentImpl, (PinotSegmentRecordReader) recordReader);
    }
    driver.init(genConfig, dataSource, TransformPipeline.getPassThroughPipeline(_tableName)); // initializes reader

    if (!_enableColumnMajor) {
      driver.build();
    } else {
      driver.buildByColumn(_realtimeSegmentImpl);
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
