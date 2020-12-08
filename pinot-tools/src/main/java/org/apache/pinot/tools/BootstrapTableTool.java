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
package org.apache.pinot.tools;

import com.google.common.base.Preconditions;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.Reader;
import java.net.URL;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.spi.ingestion.batch.IngestionJobLauncher;
import org.apache.pinot.spi.ingestion.batch.spec.SegmentGenerationJobSpec;
import org.apache.pinot.tools.admin.command.AddTableCommand;
import org.apache.pinot.tools.utils.JarUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;


public class BootstrapTableTool {
  private static final Logger LOGGER = LoggerFactory.getLogger(BootstrapTableTool.class);
  private final String _controllerHost;
  private final int _controllerPort;
  private final String _tableDir;

  public BootstrapTableTool(String controllerHost, int controllerPort, String tableDir) {
    Preconditions.checkNotNull(controllerHost);
    Preconditions.checkNotNull(tableDir);
    _controllerHost = controllerHost;
    _controllerPort = controllerPort;
    _tableDir = tableDir;
  }

  public boolean execute()
      throws Exception {
    File setupTableTmpDir = new File(FileUtils.getTempDirectory(), String.valueOf(System.currentTimeMillis()));

    File tableDir = new File(_tableDir);
    String tableName = tableDir.getName();
    File schemaFile = new File(tableDir, String.format("%s_schema.json", tableName));
    if (!schemaFile.exists()) {
      throw new RuntimeException(
          "Unable to find schema file for table - " + tableName + ", at " + schemaFile.getAbsolutePath());
    }
    boolean tableCreationResult = false;
    File offlineTableConfigFile = new File(tableDir, String.format("%s_offline_table_config.json", tableName));
    if (offlineTableConfigFile.exists()) {
      File ingestionJobSpecFile = new File(tableDir, "ingestionJobSpec.yaml");
      tableCreationResult =
          bootstrapOfflineTable(setupTableTmpDir, tableName, schemaFile, offlineTableConfigFile, ingestionJobSpecFile);
    }
    File realtimeTableConfigFile = new File(tableDir, String.format("%s_realtime_table_config.json", tableName));
    if (realtimeTableConfigFile.exists()) {
      tableCreationResult = bootstrapRealtimeTable(tableName, schemaFile, realtimeTableConfigFile);
    }
    if (!tableCreationResult) {
      throw new RuntimeException(String
          .format("Unable to find config files for table - %s, at location [%s] or [%s].", tableName,
              offlineTableConfigFile.getAbsolutePath(), realtimeTableConfigFile.getAbsolutePath()));
    }
    return true;
  }

  private boolean bootstrapRealtimeTable(String tableName, File schemaFile, File realtimeTableConfigFile)
      throws Exception {
    LOGGER.info("Adding realtime table {}", tableName);
    if (!createTable(schemaFile, realtimeTableConfigFile)) {
      throw new RuntimeException(String
          .format("Unable to create realtime table - %s from schema file [%s] and table conf file [%s].", tableName,
              schemaFile, realtimeTableConfigFile));
    }
    return true;
  }

  private boolean createTable(File schemaFile, File tableConfigFile)
      throws Exception {
    return new AddTableCommand().setSchemaFile(schemaFile.getAbsolutePath())
        .setTableConfigFile(tableConfigFile.getAbsolutePath()).setControllerHost(_controllerHost)
        .setControllerPort(String.valueOf(_controllerPort)).setExecute(true).execute();
  }
  private boolean bootstrapOfflineTable(File setupTableTmpDir, String tableName, File schemaFile,
      File offlineTableConfigFile, File ingestionJobSpecFile)
      throws Exception {
    LOGGER.info("Adding offline table: {}", tableName);
    boolean tableCreationResult = createTable(schemaFile, offlineTableConfigFile);

    if (!tableCreationResult) {
      throw new RuntimeException(String
          .format("Unable to create offline table - %s from schema file [%s] and table conf file [%s].", tableName,
              schemaFile, offlineTableConfigFile));
    }

    if (ingestionJobSpecFile.exists()) {
      LOGGER.info("Launch data ingestion job to build index segment for table {} and push to controller [{}:{}]",
          tableName, _controllerHost, _controllerPort);
      try (Reader reader = new BufferedReader(new FileReader(ingestionJobSpecFile.getAbsolutePath()))) {
        SegmentGenerationJobSpec spec = new Yaml().loadAs(reader, SegmentGenerationJobSpec.class);
        String inputDirURI = spec.getInputDirURI();
        if (!new File(inputDirURI).exists()) {
          URL resolvedInputDirURI = BootstrapTableTool.class.getClassLoader().getResource(inputDirURI);
          if (resolvedInputDirURI.getProtocol().equals("jar")) {
            String[] splits = resolvedInputDirURI.getFile().split("!");
            String inputDir = new File(setupTableTmpDir, "inputData").toString();
            JarUtils.copyResourcesToDirectory(splits[0], splits[1].substring(1), inputDir);
            spec.setInputDirURI(inputDir);
          } else {
            spec.setInputDirURI(resolvedInputDirURI.toString());
          }
        }
        IngestionJobLauncher.runIngestionJob(spec);
      }
    } else {
      LOGGER.info("Not found ingestionJobSpec.yaml at location [{}], skipping data ingestion",
          ingestionJobSpecFile.getAbsolutePath());
    }
    return true;
  }
}
