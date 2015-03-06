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
package com.linkedin.pinot.core.realtime.converter;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import com.linkedin.pinot.common.data.FieldSpec.DataType;
import com.linkedin.pinot.common.data.TimeGranularitySpec;
import com.linkedin.pinot.common.data.Schema;
import com.linkedin.pinot.common.data.TimeFieldSpec;
import com.linkedin.pinot.core.data.extractors.FieldExtractor;
import com.linkedin.pinot.core.data.extractors.FieldExtractorFactory;
import com.linkedin.pinot.core.data.readers.FileFormat;
import com.linkedin.pinot.core.data.readers.RecordReader;
import com.linkedin.pinot.core.indexsegment.generator.SegmentGeneratorConfig;
import com.linkedin.pinot.core.indexsegment.generator.SegmentVersion;
import com.linkedin.pinot.core.realtime.impl.RealtimeSegmentImpl;
import com.linkedin.pinot.core.segment.creator.impl.SegmentIndexCreationDriverImpl;


public class RealtimeSegmentConverter {

  private RealtimeSegmentImpl realtimeSegmentImpl;
  private String outputPath;
  private Schema dataSchema;
  private String resourceName;
  private String tableName;

  public RealtimeSegmentConverter(RealtimeSegmentImpl realtimeSegment, String outputPath, Schema schema,
      String resourceName, String tableName) {
    realtimeSegmentImpl = realtimeSegment;
    this.outputPath = outputPath;
    if (new File(outputPath).exists())
      throw new IllegalAccessError("path already exists");
    this.resourceName = resourceName;
    this.tableName = tableName;

    TimeFieldSpec original = schema.getTimeSpec();
    TimeGranularitySpec incoming = original.getIncominGranularutySpec();
    incoming.setDataType(DataType.LONG);

    TimeFieldSpec newTimeSpec = new TimeFieldSpec(incoming);

    Schema newSchema = new Schema();
    for (String dimension : schema.getDimensionNames())
      newSchema.addSchema(dimension, schema.getFieldSpecFor(dimension));
    for (String metic : schema.getMetricNames())
      newSchema.addSchema(metic, schema.getFieldSpecFor(metic));

    newSchema.addSchema(newTimeSpec.getName(), newTimeSpec);
    this.dataSchema = newSchema;
  }

  public void build() throws Exception {
    // lets create a record reader
    RecordReader reader = new RealtimeSegmentRecordReader(realtimeSegmentImpl, dataSchema);
    FieldExtractor extractor = FieldExtractorFactory.getPlainFieldExtractor(dataSchema);

    SegmentGeneratorConfig genConfig = new SegmentGeneratorConfig(dataSchema);
    genConfig.setInputFilePath(null);

    genConfig.setTimeColumnName(dataSchema.getTimeSpec().getOutGoingTimeColumnName());

    genConfig.setTimeUnitForSegment(dataSchema.getTimeSpec().getOutgoingGranularitySpec().getTimeType());
    genConfig.setSegmentVersion(SegmentVersion.v1);
    genConfig.setResourceName(resourceName);
    genConfig.setTableName(tableName);
    genConfig.setIndexOutputDir(outputPath);

    final SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
    driver.init(genConfig, reader);
    driver.build();
  }
}
