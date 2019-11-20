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
package org.apache.pinot.druid.tools;

import java.io.File;
import org.apache.pinot.common.data.Schema;
import org.apache.pinot.core.indexsegment.generator.SegmentGeneratorConfig;
import org.apache.pinot.core.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.druid.data.readers.DruidSegmentRecordReader;


/**
 * The DruidToPinotSegmentConverter is a CLI tool that converts a Druid segment to a Pinot segment.
 */
public class DruidToPinotSegmentConverter {
  // TODO: Change implementation to use the Command framework like the CreateSegmentCommand
  public static void convertSegment(String pinotTableName, String pinotSegmentName, String pinotSchemaFilePath, String druidSegmentPath, String outputPath)
      throws Exception {
    File segment = new File(druidSegmentPath);
    Schema schema = Schema.fromFile(new File(pinotSchemaFilePath));

    final SegmentGeneratorConfig segmentGeneratorConfig = new SegmentGeneratorConfig();
    segmentGeneratorConfig.setDataDir(druidSegmentPath);
    segmentGeneratorConfig.setOutDir(outputPath);
    segmentGeneratorConfig.setOverwrite(true);
    segmentGeneratorConfig.setTableName(pinotTableName);
    segmentGeneratorConfig.setSegmentName(pinotSegmentName);
    segmentGeneratorConfig.setSchema(schema);

    final SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
    SegmentGeneratorConfig config = new SegmentGeneratorConfig(segmentGeneratorConfig);
    DruidSegmentRecordReader recordReader = new DruidSegmentRecordReader(segment, segmentGeneratorConfig.getSchema());
    driver.init(config, recordReader);
    driver.build();
  }

  public static void main(String[] args)
      throws Exception {
    if (args.length != 5) {
      System.out.println("Usage:");
      System.out.println("java -jar pinot-druid-migration-jar-with-dependencies.jar <pinot_table_name> <pinot_segment_name> <pinot_schema_path> <druid_segment_path> <output_path>");
    } else {
      convertSegment(args[0], args[1], args[2], args[3], args[4]);
    }
  }
}
