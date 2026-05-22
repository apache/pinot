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
import org.apache.pinot.segment.local.realtime.converter.stats.MutableSegmentCreationDataSource;
import org.apache.pinot.segment.local.segment.creator.TransformPipeline;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.segment.local.segment.readers.CompactedPinotSegmentRecordReader;
import org.apache.pinot.segment.local.segment.readers.PinotSegmentRecordReader;
import org.apache.pinot.segment.local.utils.TableConfigUtils;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
import org.apache.pinot.segment.spi.creator.SegmentVersion;
import org.apache.pinot.segment.spi.index.mutable.ThreadSafeMutableRoaringBitmap;
import org.apache.pinot.spi.config.instance.InstanceType;
import org.apache.pinot.spi.config.table.ColumnPartitionConfig;
import org.apache.pinot.spi.config.table.SegmentPartitionConfig;
import org.apache.pinot.spi.config.table.SegmentZKPropsConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.RecordReader;
import org.roaringbitmap.RoaringBitmap;
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
  private final ServerMetrics _serverMetrics;

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
    _serverMetrics = ServerMetrics.get();
  }

  public void build(@Nullable SegmentVersion segmentVersion)
      throws Exception {
    SegmentGeneratorConfig genConfig = new SegmentGeneratorConfig(_tableConfig, _dataSchema);
    genConfig.setInstanceType(InstanceType.SERVER);
    genConfig.setRealtimeConversion(true);
    genConfig.setConsumerDir(_realtimeSegmentImpl.getConsumerDir());

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
      RoaringBitmap validDocIds = getValidDocIds();
      if (validDocIds == null) {
        throw new IllegalStateException("Cannot use CompactedPinotSegmentRecordReader without valid document IDs. "
            + "Segment may be corrupted.");
      }
      genConfig.setMutableSegmentCompacted(true);
      // Use CompactedPinotSegmentRecordReader to remove obsolete/invalidated records
      try (CompactedPinotSegmentRecordReader recordReader = new CompactedPinotSegmentRecordReader(validDocIds)) {
        recordReader.init(_realtimeSegmentImpl, sortedDocIds);
        buildSegmentWithReader(driver, genConfig, recordReader, sortedDocIds, sortedColumn, validDocIds);
        publishCompactionMetrics(preCommitRowCount, driver, compactionStartTime);
      }
    } else {
      // Use regular PinotSegmentRecordReader (existing behavior)
      try (PinotSegmentRecordReader recordReader = new PinotSegmentRecordReader()) {
        recordReader.init(_realtimeSegmentImpl, sortedDocIds);
        // Calculate a mapping from mutable docId to immutable docId when both sorting and reusing mutable text index
        // are enabled
        if (sortedDocIds != null && _realtimeSegmentImpl.hasColumnWithReuseMutableTextIndex()) {
          int numDocs = sortedDocIds.length;
          int[] mutableToImmutableDocIdMap = new int[numDocs];
          for (int i = 0; i < numDocs; i++) {
            mutableToImmutableDocIdMap[sortedDocIds[i]] = i;
          }
          genConfig.setMutableToImmutableDocIdMap(mutableToImmutableDocIdMap);
        }
        buildSegmentWithReader(driver, genConfig, recordReader, sortedDocIds, sortedColumn, null);
      }
    }

    if (segmentPartitionConfig != null) {
      Map<String, ColumnPartitionConfig> columnPartitionMap = segmentPartitionConfig.getColumnPartitionMap();
      for (String columnName : columnPartitionMap.keySet()) {
        int numPartitions = driver.getSegmentStats().getColumnProfileFor(columnName).getPartitions().size();
        _serverMetrics.addValueToTableGauge(_tableName, ServerGauge.REALTIME_SEGMENT_NUM_PARTITIONS, numPartitions);
      }
    }
  }

  @Nullable
  private RoaringBitmap getValidDocIds() {
    ThreadSafeMutableRoaringBitmap validDocIds = _realtimeSegmentImpl.getValidDocIds();
    return validDocIds != null ? validDocIds.getMutableRoaringBitmap().toRoaringBitmap() : null;
  }

  /**
   * Publishes segment build metrics including common metrics (always published) and compaction-specific metrics
   * (published only when compaction is enabled)
   */
  private void publishCompactionMetrics(int preCommitRowCount, SegmentIndexCreationDriverImpl driver,
      long buildStartTime) {
    try {
      int postCommitRowCount = driver.getSegmentStats().getTotalDocCount();
      long buildProcessingTime = System.currentTimeMillis() - buildStartTime;

      int rowsRemoved = preCommitRowCount - postCommitRowCount;

      // Only publish compaction-specific metrics when compaction is actually enabled
      _serverMetrics.addMeteredTableValue(_tableName, ServerMeter.COMMIT_TIME_COMPACTION_ENABLED_SEGMENTS, 1L);
      _serverMetrics.addMeteredTableValue(_tableName, ServerMeter.COMMIT_TIME_COMPACTION_ROWS_PRE_COMPACTION,
          preCommitRowCount);
      _serverMetrics.addMeteredTableValue(_tableName, ServerMeter.COMMIT_TIME_COMPACTION_ROWS_POST_COMPACTION,
          postCommitRowCount);
      _serverMetrics.addMeteredTableValue(_tableName, ServerMeter.COMMIT_TIME_COMPACTION_ROWS_REMOVED, rowsRemoved);
      _serverMetrics.addMeteredTableValue(_tableName, ServerMeter.COMMIT_TIME_COMPACTION_BUILD_TIME_MS,
          buildProcessingTime);

      // Calculate and publish compaction ratio percentage (only if we had rows to compact)
      if (preCommitRowCount > 0) {
        double compactionRatioPercent = (double) rowsRemoved / preCommitRowCount * 100.0;
        _serverMetrics.setOrUpdateTableGauge(_tableName, ServerGauge.COMMIT_TIME_COMPACTION_RATIO_PERCENT,
            (long) compactionRatioPercent);
      }
    } catch (Exception e) {
      LOGGER.warn("Failed to publish segment build metrics for table: {}, segment: {}", _tableName, _segmentName, e);
    }
  }

  /**
   * Common method to build segment with the provided record reader
   */
  private void buildSegmentWithReader(SegmentIndexCreationDriverImpl driver, SegmentGeneratorConfig genConfig,
      RecordReader recordReader, int[] sortedDocIds, @Nullable String sortedColumn, @Nullable RoaringBitmap validDocIds)
      throws Exception {
    MutableSegmentCreationDataSource dataSource =
        new MutableSegmentCreationDataSource(_realtimeSegmentImpl, recordReader, sortedDocIds, sortedColumn,
            validDocIds);
    driver.init(genConfig, dataSource, TransformPipeline.getPassThroughPipeline(_tableName));

    if (!_enableColumnMajor) {
      driver.build();
    } else {
      driver.buildByColumn(_realtimeSegmentImpl, validDocIds);
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
