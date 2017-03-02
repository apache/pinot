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
package com.linkedin.pinot.core.realtime.converter;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import com.linkedin.pinot.common.data.Schema;
import com.linkedin.pinot.common.data.TimeFieldSpec;
import com.linkedin.pinot.common.data.TimeGranularitySpec;
import com.linkedin.pinot.core.data.readers.RecordReader;
import com.linkedin.pinot.core.indexsegment.generator.SegmentGeneratorConfig;
import com.linkedin.pinot.core.indexsegment.generator.SegmentVersion;
import com.linkedin.pinot.core.realtime.impl.RealtimeSegmentImpl;
import com.linkedin.pinot.core.segment.creator.impl.SegmentIndexCreationDriverImpl;


public class RealtimeSegmentConverter {

  private RealtimeSegmentImpl realtimeSegmentImpl;
  private String outputPath;
  private Schema dataSchema;
  private String tableName;
  private String segmentName;
  private String sortedColumn;
  private List<String> invertedIndexColumns;
  private List<String> noDictionaryColumns;

  public RealtimeSegmentConverter(RealtimeSegmentImpl realtimeSegment, String outputPath, Schema schema,
      String tableName, String segmentName, String sortedColumn, List<String> invertedIndexColumns,
      List<String> noDictionaryColumns) {
    if (new File(outputPath).exists()) {
      throw new IllegalAccessError("path already exists:" + outputPath);
    }
    TimeFieldSpec original = schema.getTimeFieldSpec();
    TimeGranularitySpec incoming = original.getIncomingGranularitySpec();
    // incoming.setDataType(DataType.LONG);

    TimeFieldSpec newTimeSpec = new TimeFieldSpec(incoming);

    Schema newSchema = new Schema();
    for (String dimension : schema.getDimensionNames()) {
      newSchema.addField(schema.getFieldSpecFor(dimension));
    }
    for (String metric : schema.getMetricNames()) {
      newSchema.addField(schema.getFieldSpecFor(metric));
    }

    newSchema.addField(newTimeSpec);
    this.realtimeSegmentImpl = realtimeSegment;
    this.outputPath = outputPath;
    this.invertedIndexColumns = new ArrayList<>(invertedIndexColumns);
    if (sortedColumn != null && this.invertedIndexColumns.contains(sortedColumn)) {
      this.invertedIndexColumns.remove(sortedColumn);
    }
    this.dataSchema = newSchema;
    this.sortedColumn = sortedColumn;
    this.tableName = tableName;
    this.segmentName = segmentName;
    this.noDictionaryColumns = noDictionaryColumns;
  }

  public RealtimeSegmentConverter(RealtimeSegmentImpl realtimeSegment, String outputPath, Schema schema,
      String tableName, String segmentName, String sortedColumn) {
    this(realtimeSegment, outputPath, schema, tableName, segmentName, sortedColumn, new ArrayList<String>(),
        new ArrayList<String>());
  }

  public void build(SegmentVersion segmentVersion) throws Exception {
    // lets create a record reader
    RecordReader reader;
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
    }
    genConfig.setTimeColumnName(dataSchema.getTimeFieldSpec().getOutgoingTimeColumnName());
    genConfig.setSegmentTimeUnit(dataSchema.getTimeFieldSpec().getOutgoingGranularitySpec().getTimeType());
    genConfig.setSegmentVersion(segmentVersion);
    genConfig.setTableName(tableName);
    genConfig.setOutDir(outputPath);
    genConfig.setSegmentName(segmentName);
    final SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
    driver.init(genConfig, reader);
    driver.build();
  }
}
