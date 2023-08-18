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
import java.util.TreeMap;
import org.apache.pinot.plugin.inputformat.json.JSONRecordReader;
import org.apache.pinot.segment.local.segment.creator.TransformPipeline;
import org.apache.pinot.segment.spi.creator.StatsCollectorConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.tools.Command;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;


/**
 * Class for command to do a dry run of data ingestion so that we can see how transformation functions and
 * complex config will be applied.
 */
@CommandLine.Command(name = "DataImportDryRun", description = "Dry run of data import", mixinStandardHelpOptions = true)
public class DataImportDryRunCommand extends AbstractBaseAdminCommand implements Command {
  private static final Logger LOGGER = LoggerFactory.getLogger(DataImportDryRunCommand.class);

  @CommandLine.Option(names = {"-jsonFile"}, required = true, description = "Path to json file.")
  String _jsonFile;

  @CommandLine.Option(names = {"-tableConfigFile"}, required = true, description = "Path to table config file.")
  String _tableConfigFile;

  @Override
  public boolean execute() throws Exception {
    JSONRecordReader jsonRecordReader = new JSONRecordReader();
    jsonRecordReader.init(new File(_jsonFile), null, null);

    TableConfig tableConfig = JsonUtils.fileToObject(new File(_tableConfigFile), TableConfig.class);
    StatsCollectorConfig statsCollectorConfig = new StatsCollectorConfig(tableConfig, new Schema(), null);

    TransformPipeline transformPipeline =
        new TransformPipeline(statsCollectorConfig.getTableConfig(), statsCollectorConfig.getSchema());

    // Gather the stats
    GenericRow reuse = new GenericRow();
    TransformPipeline.Result reusedResult = new TransformPipeline.Result();
    while (jsonRecordReader.hasNext()) {
      reuse.clear();

      reuse = jsonRecordReader.next(reuse);
      transformPipeline.processRow(reuse, reusedResult);
      for (GenericRow row : reusedResult.getTransformedRows()) {
        System.out.println("Available Fields: " + JsonUtils.objectToPrettyString(
            new TreeMap<>(row.getFieldToValueMap())));
      }
    }

    return true;
  }
}
