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
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.pinot.common.metrics.ServerGauge;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.segment.local.indexsegment.mutable.MutableSegmentImpl;
import org.apache.pinot.segment.local.realtime.converter.stats.RealtimeSegmentSegmentCreationDataSource;
import org.apache.pinot.segment.local.segment.creator.TransformPipeline;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.segment.local.segment.readers.PinotSegmentRecordReader;
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
  private final ColumnIndicesForRealtimeTable _columnIndicesForRealtimeTable;
  private final boolean _nullHandlingEnabled;

  public RealtimeSegmentConverter(MutableSegmentImpl realtimeSegment, SegmentZKPropsConfig segmentZKPropsConfig,
      String outputPath, Schema schema, String tableName, TableConfig tableConfig, String segmentName,
      ColumnIndicesForRealtimeTable cdc, boolean nullHandlingEnabled) {
    _realtimeSegmentImpl = realtimeSegment;
    _segmentZKPropsConfig = segmentZKPropsConfig;
    _outputPath = outputPath;
    _columnIndicesForRealtimeTable = cdc;
    if (cdc.getSortedColumn() != null) {
      _columnIndicesForRealtimeTable.getInvertedIndexColumns().remove(cdc.getSortedColumn());
    }
    _dataSchema = getUpdatedSchema(schema);
    _tableName = tableName;
    _tableConfig = tableConfig;
    _segmentName = segmentName;
    _nullHandlingEnabled = nullHandlingEnabled;
  }

  public void build(@Nullable SegmentVersion segmentVersion, ServerMetrics serverMetrics)
      throws Exception {
    SegmentGeneratorConfig genConfig = new SegmentGeneratorConfig(_tableConfig, _dataSchema);
    // The segment generation code in SegmentColumnarIndexCreator will throw
    // exception if start and end time in time column are not in acceptable
    // range. We don't want the realtime consumption to stop (if an exception
    // is thrown) and thus the time validity check is explicitly disabled for
    // realtime segment generation
    genConfig.setSegmentTimeValueCheck(false);
    if (_columnIndicesForRealtimeTable.getInvertedIndexColumns() != null) {
      for (String column : _columnIndicesForRealtimeTable.getInvertedIndexColumns()) {
        genConfig.createInvertedIndexForColumn(column);
      }
    }

    if (_columnIndicesForRealtimeTable.getVarLengthDictionaryColumns() != null) {
      genConfig.setVarLengthDictionaryColumns(_columnIndicesForRealtimeTable.getVarLengthDictionaryColumns());
    }

    if (segmentVersion != null) {
      genConfig.setSegmentVersion(segmentVersion);
    }
    genConfig.setTableName(_tableName);
    genConfig.setOutDir(_outputPath);
    genConfig.setSegmentName(_segmentName);
    genConfig.setTextIndexCreationColumns(_columnIndicesForRealtimeTable.getTextIndexColumns());
    genConfig.setFSTIndexCreationColumns(_columnIndicesForRealtimeTable.getFstIndexColumns());
    SegmentPartitionConfig segmentPartitionConfig = _realtimeSegmentImpl.getSegmentPartitionConfig();
    genConfig.setSegmentPartitionConfig(segmentPartitionConfig);
    genConfig.setNullHandlingEnabled(_nullHandlingEnabled);
    genConfig.setSegmentZKPropsConfig(_segmentZKPropsConfig);

    SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
    try (PinotSegmentRecordReader recordReader = new PinotSegmentRecordReader()) {
      int[] sortedDocIds =
          _columnIndicesForRealtimeTable.getSortedColumn() != null
              ? _realtimeSegmentImpl.getSortedDocIdIterationOrderWithSortedColumn(
                  _columnIndicesForRealtimeTable.getSortedColumn()) : null;
      recordReader.init(_realtimeSegmentImpl, sortedDocIds);
      RealtimeSegmentSegmentCreationDataSource dataSource =
          new RealtimeSegmentSegmentCreationDataSource(_realtimeSegmentImpl, recordReader);
      driver.init(genConfig, dataSource, TransformPipeline.getPassThroughPipeline());
      driver.build();
    }

    if (segmentPartitionConfig != null) {
      Map<String, ColumnPartitionConfig> columnPartitionMap = segmentPartitionConfig.getColumnPartitionMap();
      for (String columnName : columnPartitionMap.keySet()) {
        int numPartitions = driver.getSegmentStats().getColumnProfileFor(columnName).getPartitions().size();
        serverMetrics.addValueToTableGauge(_tableName, ServerGauge.REALTIME_SEGMENT_NUM_PARTITIONS, numPartitions);
      }
    }
  }

  /**
   * Returns a new schema containing only physical columns
   */
  @VisibleForTesting
  public static Schema getUpdatedSchema(Schema original) {
    Schema newSchema = new Schema();
    for (String col : original.getPhysicalColumnNames()) {
      newSchema.addField(original.getFieldSpecFor(col));
    }
    return newSchema;
  }
}
