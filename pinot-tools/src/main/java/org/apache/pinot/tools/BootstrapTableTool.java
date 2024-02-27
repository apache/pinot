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
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.net.URI;
import java.net.URL;
import java.util.List;
import java.util.Map;
import org.apache.commons.io.FileUtils;
import org.apache.http.HttpException;
import org.apache.pinot.common.auth.AuthProviderUtils;
import org.apache.pinot.common.minion.MinionClient;
import org.apache.pinot.common.utils.tls.TlsUtils;
import org.apache.pinot.core.common.MinionConstants;
import org.apache.pinot.spi.auth.AuthProvider;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.ingestion.batch.BatchConfigProperties;
import org.apache.pinot.spi.ingestion.batch.IngestionJobLauncher;
import org.apache.pinot.spi.ingestion.batch.spec.PinotClusterSpec;
import org.apache.pinot.spi.ingestion.batch.spec.SegmentGenerationJobSpec;
import org.apache.pinot.spi.ingestion.batch.spec.TableSpec;
import org.apache.pinot.spi.ingestion.batch.spec.TlsSpec;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.apache.pinot.tools.admin.command.AddTableCommand;
import org.apache.pinot.tools.utils.JarUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;


public class BootstrapTableTool {
  private static final Logger LOGGER = LoggerFactory.getLogger(BootstrapTableTool.class);
  private static final String COMPLETED = "COMPLETED";
  private final String _controllerProtocol;
  private final String _controllerHost;
  private final int _controllerPort;
  private final AuthProvider _authProvider;
  private final String _tableDir;
  private final MinionClient _minionClient;

  public BootstrapTableTool(String controllerProtocol, String controllerHost, int controllerPort, String tableDir,
      AuthProvider authProvider) {
    Preconditions.checkNotNull(controllerProtocol);
    Preconditions.checkNotNull(controllerHost);
    Preconditions.checkNotNull(tableDir);
    _controllerProtocol = controllerProtocol;
    _controllerHost = controllerHost;
    _controllerPort = controllerPort;
    _tableDir = tableDir;
    _minionClient =
        new MinionClient(String.format("%s://%s:%s", controllerProtocol, controllerHost, controllerPort), authProvider);
    _authProvider = authProvider;
  }

  public boolean execute()
      throws Exception {
    File setupTableTmpDir = new File(FileUtils.getTempDirectory(), String.valueOf(System.currentTimeMillis()));
    setupTableTmpDir.mkdirs();

    File tableDir = new File(_tableDir);
    String tableName = tableDir.getName();
    File schemaFile = new File(tableDir, String.format("%s_schema.json", tableName));
    if (!schemaFile.exists()) {
      throw new RuntimeException(
          "Unable to find schema file for table - " + tableName + ", at " + schemaFile.getAbsolutePath());
    }
    boolean tableCreationResult = false;
    File offlineTableConfigFile = new File(tableDir, String.format("%s_offline_table_config.json", tableName));
    File realtimeTableConfigFile = new File(tableDir, String.format("%s_realtime_table_config.json", tableName));

    if (offlineTableConfigFile.exists() && realtimeTableConfigFile.exists()) {
      File ingestionJobSpecFile = new File(tableDir, "ingestionJobSpec.yaml");
      tableCreationResult =
          bootstrapHybridTable(setupTableTmpDir, tableName, schemaFile, offlineTableConfigFile, ingestionJobSpecFile,
              realtimeTableConfigFile);
    } else if (offlineTableConfigFile.exists()) {
      File ingestionJobSpecFile = new File(tableDir, "ingestionJobSpec.yaml");
      tableCreationResult =
          bootstrapOfflineTable(setupTableTmpDir, tableName, schemaFile, offlineTableConfigFile, ingestionJobSpecFile);
    } else if (realtimeTableConfigFile.exists()) {
      tableCreationResult = bootstrapRealtimeTable(tableName, schemaFile, realtimeTableConfigFile);
    }
    if (!tableCreationResult) {
      throw new RuntimeException(
          String.format("Unable to find config files for table - %s, at location [%s] or [%s].", tableName,
              offlineTableConfigFile.getAbsolutePath(), realtimeTableConfigFile.getAbsolutePath()));
    }
    return true;
  }

  private boolean bootstrapRealtimeTable(String tableName, File schemaFile, File realtimeTableConfigFile)
      throws Exception {
    LOGGER.info("Adding realtime table {}", tableName);
    if (!createTable(schemaFile, realtimeTableConfigFile)) {
      throw new RuntimeException(
          String.format("Unable to create realtime table - %s from schema file [%s] and table conf file [%s].",
              tableName, schemaFile, realtimeTableConfigFile));
    }
    return true;
  }

  private boolean createTable(File schemaFile, File tableConfigFile)
      throws Exception {
    return new AddTableCommand().setSchemaFile(schemaFile.getAbsolutePath())
        .setTableConfigFile(tableConfigFile.getAbsolutePath()).setControllerProtocol(_controllerProtocol)
        .setControllerHost(_controllerHost).setControllerPort(String.valueOf(_controllerPort)).setExecute(true)
        .setAuthProvider(_authProvider).execute();
  }

  private boolean createTable(File schemaFile, File offlineTableConfigFile, File realtimeTableConfigFile)
      throws Exception {
    return new AddTableCommand().setSchemaFile(schemaFile.getAbsolutePath())
        .setOfflineTableConfigFile(offlineTableConfigFile.getAbsolutePath())
        .setRealtimeTableConfigFile(realtimeTableConfigFile.getAbsolutePath())
        .setControllerProtocol(_controllerProtocol).setControllerHost(_controllerHost)
        .setControllerPort(String.valueOf(_controllerPort)).setExecute(true).setAuthProvider(_authProvider).execute();
  }

  private boolean bootstrapHybridTable(File setupTableTmpDir, String tableName, File schemaFile,
      File offlineTableConfigFile, File ingestionJobSpecFile, File realtimeTableConfig)
      throws Exception {
    TableConfig tableConfig =
        JsonUtils.inputStreamToObject(new FileInputStream(offlineTableConfigFile), TableConfig.class);
    if (tableConfig.getIngestionConfig() != null
        && tableConfig.getIngestionConfig().getBatchIngestionConfig() != null) {
      updatedTableConfig(tableConfig, setupTableTmpDir);
    }

    LOGGER.info("Adding offline table: {}", tableName);
    File updatedTableConfigFile =
        new File(setupTableTmpDir, String.format("%s_%d.config", tableName, System.currentTimeMillis()));
    FileOutputStream outputStream = new FileOutputStream(updatedTableConfigFile);
    outputStream.write(JsonUtils.objectToPrettyString(tableConfig).getBytes());
    outputStream.close();
    // this function is separated from bootstrap offline/realtime to create both tables at the same time in
    // order to avoid a "table already exists error"
    // TODO: it's unclear why this broke as it's been working, but using the PUT API would solve this
    boolean tableCreationResult = createTable(schemaFile, updatedTableConfigFile, realtimeTableConfig);
    if (!tableCreationResult) {
      throw new RuntimeException(
          String.format("Unable to create offline table - %s from schema file [%s] and table conf file [%s].",
              tableName, schemaFile, offlineTableConfigFile));
    }
    return setupOfflineData(setupTableTmpDir, tableConfig, tableName, ingestionJobSpecFile);
  }

  private boolean bootstrapOfflineTable(File setupTableTmpDir, String tableName, File schemaFile,
      File offlineTableConfigFile, File ingestionJobSpecFile)
      throws Exception {
    TableConfig tableConfig =
        JsonUtils.inputStreamToObject(new FileInputStream(offlineTableConfigFile), TableConfig.class);
    if (tableConfig.getIngestionConfig() != null
        && tableConfig.getIngestionConfig().getBatchIngestionConfig() != null) {
      updatedTableConfig(tableConfig, setupTableTmpDir);
    }

    LOGGER.info("Adding offline table: {}", tableName);
    File updatedTableConfigFile =
        new File(setupTableTmpDir, String.format("%s_%d.config", tableName, System.currentTimeMillis()));
    FileOutputStream outputStream = new FileOutputStream(updatedTableConfigFile);
    outputStream.write(JsonUtils.objectToPrettyString(tableConfig).getBytes());
    outputStream.close();
    boolean tableCreationResult = createTable(schemaFile, updatedTableConfigFile);
    if (!tableCreationResult) {
      throw new RuntimeException(
          String.format("Unable to create offline table - %s from schema file [%s] and table conf file [%s].",
              tableName, schemaFile, offlineTableConfigFile));
    }
    return setupOfflineData(setupTableTmpDir, tableConfig, tableName, ingestionJobSpecFile);
  }

  private boolean setupOfflineData(File setupTableTmpDir, TableConfig tableConfig, String tableName,
      File ingestionJobSpecFile)
      throws IOException, HttpException {
    if (tableConfig.getTaskConfig() != null && tableConfig.getTaskConfig()
        .isTaskTypeEnabled(MinionConstants.SegmentGenerationAndPushTask.TASK_TYPE)) {
      final Map<String, String> scheduledTasks =
          _minionClient.scheduleMinionTasks(MinionConstants.SegmentGenerationAndPushTask.TASK_TYPE,
              TableNameBuilder.OFFLINE.tableNameWithType(tableName));
      if (scheduledTasks.isEmpty()) {
        LOGGER.info("No scheduled tasks.");
        return true;
      }
      waitForMinionTaskToFinish(scheduledTasks, 30_000L);
    }
    if (ingestionJobSpecFile != null) {
      if (ingestionJobSpecFile.exists()) {
        String controllerAuthority = String.format("%s://%s:%s", _controllerProtocol, _controllerHost, _controllerPort);
        LOGGER.info("Launch data ingestion job to build index segment for table {} and push to controller [{}]",
            tableName, controllerAuthority);
        try (Reader reader = new BufferedReader(new FileReader(ingestionJobSpecFile.getAbsolutePath()))) {
          SegmentGenerationJobSpec spec = new Yaml().loadAs(reader, SegmentGenerationJobSpec.class);

          // rewrite table and schema URI spec if the controller URI is not the same as the bootstrap tool setting.
          TableSpec tableSpec = spec.getTableSpec();
          URI tableConfigURI = URI.create(tableSpec.getTableConfigURI());
          tableSpec.setTableConfigURI(controllerAuthority + tableConfigURI.getPath());
          URI schemaURI = URI.create(tableSpec.getSchemaURI());
          tableSpec.setSchemaURI(controllerAuthority + schemaURI.getPath());
          PinotClusterSpec[] pinotClusterSpecs = spec.getPinotClusterSpecs();
          for (PinotClusterSpec pinotClusterSpec : pinotClusterSpecs) {
            pinotClusterSpec.setControllerURI(controllerAuthority);
          }

          String inputDirURI = spec.getInputDirURI();
          if (!new File(inputDirURI).exists()) {
            URL resolvedInputDirURI = BootstrapTableTool.class.getClassLoader().getResource(inputDirURI);
            if (resolvedInputDirURI != null && "jar".equals(resolvedInputDirURI.getProtocol())) {
              String[] splits = resolvedInputDirURI.getFile().split("!");
              String inputDir = new File(setupTableTmpDir, "inputData").toString();
              JarUtils.copyResourcesToDirectory(splits[0], splits[1].substring(1), inputDir);
              spec.setInputDirURI(inputDir);
            } else {
              spec.setInputDirURI(resolvedInputDirURI.toString());
            }
          }

          TlsSpec tlsSpec = spec.getTlsSpec();
          if (tlsSpec != null) {
            TlsUtils.installDefaultSSLSocketFactory(tlsSpec.getKeyStoreType(), tlsSpec.getKeyStorePath(),
                tlsSpec.getKeyStorePassword(), tlsSpec.getTrustStoreType(), tlsSpec.getTrustStorePath(),
                tlsSpec.getTrustStorePassword());
          }

          // url-based token needs to be resolved before job run
          spec.setAuthToken(AuthProviderUtils.toStaticToken(_authProvider));

          IngestionJobLauncher.runIngestionJob(spec);
        }
      } else {
        LOGGER.info("Not found ingestionJobSpec.yaml at location [{}], skipping data ingestion",
            ingestionJobSpecFile.getAbsolutePath());
      }
    }
    return true;
  }

  private void updatedTableConfig(TableConfig tableConfig, File setupTableTmpDir)
      throws Exception {
    final List<Map<String, String>> batchConfigsMaps =
        tableConfig.getIngestionConfig().getBatchIngestionConfig().getBatchConfigMaps();
    for (Map<String, String> batchConfigsMap : batchConfigsMaps) {
      String inputDirURI = batchConfigsMap.get(BatchConfigProperties.INPUT_DIR_URI);
      if (!new File(inputDirURI).exists()) {
        URL resolvedInputDirURI = BootstrapTableTool.class.getClassLoader().getResource(inputDirURI);
        if (resolvedInputDirURI != null) {
          if ("jar".equals(resolvedInputDirURI.getProtocol())) {
            String[] splits = resolvedInputDirURI.getFile().split("!");
            File inputDir = new File(setupTableTmpDir, "inputData");
            JarUtils.copyResourcesToDirectory(splits[0], splits[1].substring(1), inputDir.toString());
            batchConfigsMap.put(BatchConfigProperties.INPUT_DIR_URI, inputDir.toURI().toString());
            batchConfigsMap.put(BatchConfigProperties.OUTPUT_DIR_URI,
                new File(inputDir.getParent(), "segments").toURI().toString());
          } else {
            final URI inputURI = resolvedInputDirURI.toURI();
            batchConfigsMap.put(BatchConfigProperties.INPUT_DIR_URI, inputURI.toString());
            URI outputURI =
                inputURI.getPath().endsWith("/") ? inputURI.resolve("../segments") : inputURI.resolve("./segments");
            batchConfigsMap.put(BatchConfigProperties.OUTPUT_DIR_URI, outputURI.toString());
          }
        }
      }
    }
  }

  private boolean waitForMinionTaskToFinish(Map<String, String> scheduledTasks, long timeoutInMillis) {
    long startTime = System.currentTimeMillis();
    while (System.currentTimeMillis() - startTime < timeoutInMillis) {
      try {
        Thread.sleep(500L);
      } catch (InterruptedException e) {
        // Swallow the exception
      }
      try {
        boolean allCompleted = true;
        for (String taskType : scheduledTasks.keySet()) {
          String taskName = scheduledTasks.get(taskType);
          String taskState = _minionClient.getTaskState(taskName);
          if (!COMPLETED.equalsIgnoreCase(taskState)) {
            allCompleted = false;
            break;
          } else {
            scheduledTasks.remove(taskType);
          }
        }
        if (allCompleted) {
          LOGGER.info("All minion tasks are completed.");
          return true;
        }
      } catch (Exception e) {
        LOGGER.error("Failed to query task endpoint", e);
        continue;
      }
    }
    return false;
  }
}
