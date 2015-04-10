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
package com.linkedin.pinot.tools.admin.command;

import java.io.File;

import org.codehaus.jackson.map.ObjectMapper;
import org.kohsuke.args4j.Option;

import com.linkedin.pinot.common.data.Schema;
import com.linkedin.pinot.core.data.readers.FileFormat;
import com.linkedin.pinot.core.indexsegment.generator.SegmentGeneratorConfig;
import com.linkedin.pinot.core.indexsegment.generator.SegmentVersion;
import com.linkedin.pinot.core.segment.creator.impl.SegmentIndexCreationDriverImpl;

/**
 * Class to implement CreateSegment command.
 *
 * @author Mayank Shrivastava <mshrivastava@linkedin.com>
 */
public class CreateSegmentCommand implements Command {
  @Option(name="-schemaFile", required=true, metaVar="<schemaFile>", usage="file containing schema for data")
  String _schemaFile;

  @Option(name="-dataDir", required=true, metaVar="<dataDir>", usage="directory containing the data")
  String _dataDir;

  @Option(name="-resourceName", required=true, metaVar="<resourceName>", usage="name of the resource")
  String _resourceName;

  @Option(name="-tableName", required=true, metaVar="<tableName>", usage="name of the table")
  String _tableName;

  @Option(name="-segmentName", required=true, metaVar="<segmentName>", usage="name of the segment")
  String _segmentName;

  @Option(name="-outDir", required=true, metaVar="<outputDir>")
  String _outDir;

  public void init(String schemaFile, String dataDir, String resourceName,
      String tableName, String segmentName, String outDir) {
    _schemaFile = schemaFile;
    _dataDir = dataDir;

    _resourceName = resourceName;
    _tableName = tableName;
    _segmentName = segmentName;

    _outDir = outDir;
  }

  @Override
  public String toString() {
    return ("CreateSegment " + _schemaFile + " " + _resourceName + " " + _tableName + " " + _segmentName + _outDir);
  }

  @Override
  public boolean execute() throws Exception {
    File schemaFile = new File(_schemaFile);
    Schema schema = new ObjectMapper().readValue(schemaFile, Schema.class);
    final SegmentGeneratorConfig config = new SegmentGeneratorConfig(schema);

    config.setInputFileFormat(FileFormat.AVRO);
    config.setSegmentVersion(SegmentVersion.v1);
    config.setIndexOutputDir(_outDir);

    config.setResourceName(_resourceName);
    config.setTableName(_tableName);

    File dir = new File(_dataDir);
    File [] files = dir.listFiles();

    int cnt = 0;
    for (File file : files) {
      final SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
      config.setInputFilePath(file.getAbsolutePath());
      config.setSegmentName(_segmentName + cnt);

      driver.init(config);
      driver.build();
      cnt = cnt + 1;
    }

    return true;
  }
}
