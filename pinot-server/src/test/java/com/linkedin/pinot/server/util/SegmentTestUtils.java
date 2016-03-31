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
package com.linkedin.pinot.server.util;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

import com.linkedin.pinot.common.data.Schema;
import com.linkedin.pinot.core.data.readers.FileFormat;
import com.linkedin.pinot.core.indexsegment.generator.SegmentGeneratorConfig;
import com.linkedin.pinot.core.indexsegment.generator.SegmentVersion;
import com.linkedin.pinot.core.indexsegment.utils.AvroUtils;


public class SegmentTestUtils {

  public static SegmentGeneratorConfig getSegmentGenSpecWithSchemAndProjectedColumns(File inputAvro, File outputDir,
      TimeUnit timeUnit, String clusterName) throws IOException {
    Schema schema = AvroUtils.extractSchemaFromAvro(inputAvro);
    SegmentGeneratorConfig segmentGenSpec = new SegmentGeneratorConfig(schema);
    segmentGenSpec.setInputFilePath(inputAvro.getAbsolutePath());
    segmentGenSpec.setSegmentTimeUnit(timeUnit);
    if (inputAvro.getName().endsWith("gz")) {
      segmentGenSpec.setFormat(FileFormat.GZIPPED_AVRO);
    } else {
      segmentGenSpec.setFormat(FileFormat.AVRO);
    }
    segmentGenSpec.setSegmentVersion(SegmentVersion.v1);
    segmentGenSpec.setTableName(clusterName);
    segmentGenSpec.setOutDir(outputDir.getAbsolutePath());
    return segmentGenSpec;
  }
}
