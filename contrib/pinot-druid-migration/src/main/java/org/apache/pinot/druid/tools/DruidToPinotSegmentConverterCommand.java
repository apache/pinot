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

import com.fasterxml.jackson.databind.JsonNode;
import java.io.File;
import org.apache.pinot.common.config.TableConfig;
import org.apache.pinot.common.data.Schema;
import org.apache.pinot.common.utils.JsonUtils;
import org.apache.pinot.core.indexsegment.generator.SegmentGeneratorConfig;
import org.apache.pinot.core.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.druid.data.readers.DruidSegmentRecordReader;
import org.apache.pinot.tools.Command;
import org.apache.pinot.tools.admin.command.AbstractBaseAdminCommand;
import org.kohsuke.args4j.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * The DruidToPinotSegmentConverter is a CLI tool that converts a Druid segment to a Pinot segment.
 */
public class DruidToPinotSegmentConverterCommand extends AbstractBaseAdminCommand implements Command {
  private static final Logger LOGGER = LoggerFactory.getLogger(DruidToPinotSegmentConverterCommand.class);

  @Option(name = "-pinotTableName", metaVar = "<string>", usage = "Name of the table.", required = true)
  private String _pinotTableName;

  @Option(name = "-pinotSegmentName", metaVar = "<string>", usage = "Name of the segment.", required = true)
  private String _pinotSegmentName;

  @Option(name = "-pinotTableConfigPath", metaVar = "<string>", usage = "Path to the Pinot table config.", required = true)
  private String _pinotTableConfigPath;

  @Option(name = "-pinotSchemaPath", metaVar = "<string>", usage = "Path to the Pinot schema.")
  private String _pinotSchemaPath;

  @Option(name = "-druidSegmentPath", metaVar = "<string>", usage = "Path to the Druid segment.", required = true)
  private String _druidSegmentPath;

  @Option(name = "-outputPath", metaVar = "<string>", usage = "Output path for the generated Pinot segment.", required = true)
  private String _outputPath;

  @Option(name = "-help", help = true, aliases = {"-h", "--h", "--help"}, usage = "Print this message.")
  private boolean _help = false;

  public DruidToPinotSegmentConverterCommand setPinotTableName(String pinotTableName) {
    _pinotTableName = pinotTableName;
    return this;
  }

  public DruidToPinotSegmentConverterCommand setPinotSegmentName(String pinotSegmentName) {
    _pinotSegmentName = pinotSegmentName;
    return this;
  }

  public DruidToPinotSegmentConverterCommand setPinotTableConfigPath(String pinotTableConfigPath) {
    _pinotTableConfigPath = pinotTableConfigPath;
    return this;
  }

  public DruidToPinotSegmentConverterCommand setPinotSchemaPath(String pinotSchemaPath) {
    _pinotSchemaPath = pinotSchemaPath;
    return this;
  }

  public DruidToPinotSegmentConverterCommand setDruidSegmentPath(String druidSegmentPath) {
    _druidSegmentPath = druidSegmentPath;
    return this;
  }

  public DruidToPinotSegmentConverterCommand setOutputPath(String outputPath) {
    _outputPath = outputPath;
    return this;
  }

  @Override
  public String toString() {
    return ("ConvertSegment  -pinotTableName " + _pinotTableName + " -pinotSegmentName " + _pinotSegmentName
        + " -pinotSchemaPath " + _pinotSchemaPath + "-pinotTableConfigPath " + _pinotTableConfigPath + " -druidSegmentPath "
        + _druidSegmentPath + " -outputPath " + _outputPath);
  }

  @Override
  public final String getName() {
    return "ConvertSegment";
  }

  @Override
  public String description() {
    return "Create a Pinot segment from the provided Druid segment.";
  }

  @Override
  public boolean getHelp() {
    return _help;
  }

  @Override
  public boolean execute()
      throws Exception {
    LOGGER.info("Executing command: {}", toString());
    File segment = new File(_druidSegmentPath);
    boolean compressed = false;
    if (segment.getName().endsWith(".zip") || segment.getName().endsWith(".tar.gz")) {
      compressed = true;
      segment = DruidSegmentUtils.uncompressSegmentFile(segment);
      _druidSegmentPath = segment.getPath();
    }

    File tableConfigFile = new File(_pinotTableConfigPath);
    JsonNode tableConfigJsonNode = JsonUtils.fileToJsonNode(tableConfigFile);
    TableConfig tableConfig = TableConfig.fromJsonConfig(tableConfigJsonNode);

    Schema schema;
    if (_pinotSchemaPath == null) {
      schema = DruidToPinotSchemaConverter.createSchema(_pinotTableName, _druidSegmentPath);
    } else {
      schema = Schema.fromFile(new File(_pinotSchemaPath));
    }

    final SegmentGeneratorConfig segmentGeneratorConfig = new SegmentGeneratorConfig(tableConfig, schema);
    segmentGeneratorConfig.setDataDir(_druidSegmentPath);
    segmentGeneratorConfig.setOutDir(_outputPath);
    segmentGeneratorConfig.setOverwrite(true);
    segmentGeneratorConfig.setTableName(_pinotTableName);
    segmentGeneratorConfig.setSegmentName(_pinotSegmentName);

    final SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
    SegmentGeneratorConfig config = new SegmentGeneratorConfig(segmentGeneratorConfig);
    DruidSegmentRecordReader recordReader = new DruidSegmentRecordReader(segment, segmentGeneratorConfig.getSchema());
    driver.init(config, recordReader);
    driver.build();

    // Delete uncompressed segment from the input directory
    if (compressed) {
      String[] filenames = segment.list();
      for (String f: filenames) {
        File currentFile = new File(_druidSegmentPath, f);
        currentFile.delete();
      }
      segment.delete();
    }
    return true;
  }
}
