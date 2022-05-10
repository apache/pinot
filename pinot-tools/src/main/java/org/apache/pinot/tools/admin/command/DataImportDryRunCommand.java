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
package org.apache.pinot.tools.admin.command;

import java.io.File;
import org.apache.pinot.plugin.inputformat.json.JSONRecordReader;
import org.apache.pinot.segment.local.segment.creator.RecordReaderSegmentCreationDataSource;
import org.apache.pinot.segment.local.segment.creator.TransformPipeline;
import org.apache.pinot.segment.local.segment.creator.impl.stats.SegmentPreIndexStatsCollectorImpl;
import org.apache.pinot.segment.spi.creator.SegmentPreIndexStatsCollector;
import org.apache.pinot.segment.spi.creator.StatsCollectorConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.apache.pinot.tools.Command;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;


/**
 * Class for command to infer pinot schema from Json data. Given that it is not always possible to
 * automatically do this, the intention is to get most of the work done by this class, and require any
 * manual editing on top.
 */
@CommandLine.Command(name = "DataImportDryRun")
public class DataImportDryRunCommand extends AbstractBaseAdminCommand implements Command {
  private static final Logger LOGGER = LoggerFactory.getLogger(DataImportDryRunCommand.class);

  @CommandLine.Option(names = {"-jsonFile"}, required = true, description = "Path to json file.")
  String _jsonFile;

  @CommandLine.Option(names = {"-tableConfigFile"}, required = true, description = "Path to table config file.")
  String _tableConfigFile;

  @CommandLine.Option(names = {"-schemaFile"}, required = true, description = "Path to schema file.")
  String _schemaFile;


  @SuppressWarnings("FieldCanBeLocal")
  @CommandLine.Option(names = {"-help", "-h", "--h", "--help"}, help = true, description = "Print this message.")
  private boolean _help = false;

  @Override
  public boolean execute() throws Exception {
    JSONRecordReader jsonRecordReader = new JSONRecordReader();
    jsonRecordReader.init(new File(_jsonFile), null, null);

    RecordReaderSegmentCreationDataSource dataSource =
        new RecordReaderSegmentCreationDataSource(jsonRecordReader);

    TableConfig tableConfig = JsonUtils.fileToObject(new File(_tableConfigFile), TableConfig.class);
    Schema schema = Schema.fromFile(new File(_schemaFile));

    StatsCollectorConfig statsCollectorConfig = new StatsCollectorConfig(tableConfig, schema, null);

    TransformPipeline transformPipeline =
        new TransformPipeline(statsCollectorConfig.getTableConfig(), statsCollectorConfig.getSchema());


    // Gather the stats
    GenericRow reuse = new GenericRow();
    TransformPipeline.Result reusedResult = new TransformPipeline.Result();
    while (jsonRecordReader.hasNext()) {
      reuse.clear();

      reuse = jsonRecordReader.next(reuse);
      System.out.println("reuse = " + reuse);
      transformPipeline.processRow(reuse, reusedResult);
      for (GenericRow row : reusedResult.getTransformedRows()) {
        System.out.println("row = " + JsonUtils.objectToPrettyString(row.getFieldToValueMap()));
      }
    }

    return true;
  }

  @Override
  public String description() {
    return "Extracting Pinot schema file from JSON data file.";
  }

  @Override
  public boolean getHelp() {
    return _help;
  }

}
