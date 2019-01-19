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
package org.apache.pinot.core.realtime.converter;

import com.google.common.annotations.VisibleForTesting;
import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.pinot.common.config.ColumnPartitionConfig;
import org.apache.pinot.common.config.SegmentPartitionConfig;
import org.apache.pinot.common.data.FieldSpec;
import org.apache.pinot.common.data.Schema;
import org.apache.pinot.common.data.StarTreeIndexSpec;
import org.apache.pinot.common.data.TimeFieldSpec;
import org.apache.pinot.common.data.TimeGranularitySpec;
import org.apache.pinot.common.metrics.ServerGauge;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.core.data.recordtransformer.CompoundTransformer;
import org.apache.pinot.core.indexsegment.generator.SegmentGeneratorConfig;
import org.apache.pinot.core.indexsegment.generator.SegmentVersion;
import org.apache.pinot.core.indexsegment.mutable.MutableSegmentImpl;
import org.apache.pinot.core.io.compression.ChunkCompressorFactory;
import org.apache.pinot.core.realtime.converter.stats.RealtimeSegmentSegmentCreationDataSource;
import org.apache.pinot.core.segment.creator.impl.SegmentIndexCreationDriverImpl;


public class RealtimeSegmentConverter {
  private MutableSegmentImpl realtimeSegmentImpl;
  private String outputPath;
  private Schema dataSchema;
  private String tableName;
  private String timeColumnName;
  private String segmentName;
  private String sortedColumn;
  private List<String> invertedIndexColumns;
  private List<String> noDictionaryColumns;
  private StarTreeIndexSpec starTreeIndexSpec;

  public RealtimeSegmentConverter(MutableSegmentImpl realtimeSegment, String outputPath, Schema schema,
      String tableName, String timeColumnName, String segmentName, String sortedColumn,
      List<String> invertedIndexColumns, List<String> noDictionaryColumns, StarTreeIndexSpec starTreeIndexSpec) {
    if (new File(outputPath).exists()) {
      throw new IllegalAccessError("path already exists:" + outputPath);
    }

    this.realtimeSegmentImpl = realtimeSegment;
    this.outputPath = outputPath;
    this.invertedIndexColumns = new ArrayList<>(invertedIndexColumns);
    if (sortedColumn != null && this.invertedIndexColumns.contains(sortedColumn)) {
      this.invertedIndexColumns.remove(sortedColumn);
    }
    this.dataSchema = getUpdatedSchema(schema);
    this.sortedColumn = sortedColumn;
    this.tableName = tableName;
    this.segmentName = segmentName;
    this.noDictionaryColumns = noDictionaryColumns;
    this.starTreeIndexSpec = starTreeIndexSpec;
  }

  public RealtimeSegmentConverter(MutableSegmentImpl realtimeSegment, String outputPath, Schema schema,
      String tableName, String timeColumnName, String segmentName, String sortedColumn) {
    this(realtimeSegment, outputPath, schema, tableName, timeColumnName, segmentName, sortedColumn, new ArrayList<>(),
        new ArrayList<>(), null/*StarTreeIndexSpec*/);
  }

  public void build(@Nullable SegmentVersion segmentVersion, ServerMetrics serverMetrics) throws Exception {
    // lets create a record reader
    RealtimeSegmentRecordReader reader;
    if (sortedColumn == null) {
      reader = new RealtimeSegmentRecordReader(realtimeSegmentImpl, dataSchema);
    } else {
      reader = new RealtimeSegmentRecordReader(realtimeSegmentImpl, dataSchema, sortedColumn);
    }
    SegmentGeneratorConfig genConfig = new SegmentGeneratorConfig(dataSchema);
    if (invertedIndexColumns != null && !invertedIndexColumns.isEmpty()) {
      for (String column : invertedIndexColumns) {
        genConfig.createInvertedIndexForColumn(column);
      }
    }
    if (noDictionaryColumns != null) {
      genConfig.setRawIndexCreationColumns(noDictionaryColumns);
      Map<String, ChunkCompressorFactory.CompressionType> columnToCompressionType = new HashMap<>();
      for (String column : noDictionaryColumns) {
        FieldSpec fieldSpec = dataSchema.getFieldSpecFor(column);
        if (fieldSpec.getFieldType().equals(FieldSpec.FieldType.METRIC)) {
          columnToCompressionType.put(column, ChunkCompressorFactory.CompressionType.PASS_THROUGH);
        }
      }
      genConfig.setRawIndexCompressionType(columnToCompressionType);
    }

    // Presence of the spec enables star tree generation.
    if (starTreeIndexSpec != null) {
      genConfig.enableStarTreeIndex(starTreeIndexSpec);
    }

    // TODO: use timeColumnName field
    genConfig.setTimeColumnName(dataSchema.getTimeFieldSpec().getOutgoingTimeColumnName());
    // TODO: find timeColumnName in schema.getDateTimeFieldSpec, in order to get the timeUnit
    genConfig.setSegmentTimeUnit(dataSchema.getTimeFieldSpec().getOutgoingGranularitySpec().getTimeType());
    if (segmentVersion != null) {
      genConfig.setSegmentVersion(segmentVersion);
    }
    genConfig.setTableName(tableName);
    genConfig.setOutDir(outputPath);
    genConfig.setSegmentName(segmentName);
    SegmentPartitionConfig segmentPartitionConfig = realtimeSegmentImpl.getSegmentPartitionConfig();
    genConfig.setSegmentPartitionConfig(segmentPartitionConfig);
    final SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
    RealtimeSegmentSegmentCreationDataSource dataSource =
        new RealtimeSegmentSegmentCreationDataSource(realtimeSegmentImpl, reader, dataSchema);
    driver.init(genConfig, dataSource, CompoundTransformer.getPassThroughTransformer());
    driver.build();

    if (segmentPartitionConfig != null && segmentPartitionConfig.getColumnPartitionMap() != null) {
      Map<String, ColumnPartitionConfig> columnPartitionMap = segmentPartitionConfig.getColumnPartitionMap();
      for (String columnName : columnPartitionMap.keySet()) {
        int numPartitions = driver.getSegmentStats().getColumnProfileFor(columnName).getPartitions().size();
        serverMetrics.addValueToTableGauge(tableName, ServerGauge.REALTIME_SEGMENT_NUM_PARTITIONS, numPartitions);
      }
    }
  }

  /**
   * Returns a new schema based on the original one. The new schema removes columns as needed (for ex, virtual cols)
   * and adds the new timespec to the schema.
   */
  @VisibleForTesting
  public
  Schema getUpdatedSchema(Schema original) {

    TimeFieldSpec tfs = original.getTimeFieldSpec();
    // Use outgoing granularity for creating segment
    TimeGranularitySpec outgoing = tfs.getOutgoingGranularitySpec();
    TimeFieldSpec newTimeSpec = new TimeFieldSpec(outgoing);
    Schema newSchema = new Schema();
    newSchema.addField(newTimeSpec);

    for (String col : original.getPhysicalColumnNames()) {
      if (!col.equals(tfs.getName())) {
        newSchema.addField(original.getFieldSpecFor(col));
      }
    }
    return newSchema;
  }
}
