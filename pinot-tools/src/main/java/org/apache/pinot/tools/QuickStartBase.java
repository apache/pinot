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

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.pinot.tools.admin.command.QuickstartRunner;
import org.apache.pinot.tools.utils.JarUtils;
import org.apache.pinot.tools.utils.PinotConfigUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Assuming that database name is DBNAME, bootstrap path must have the file structure specified below to properly
 * load the table:
 *  DBNAME
 *  ├── ingestionJobSpec.yaml
 *  ├── rawdata
 *  │   └── DBNAME_data.csv
 *  ├── DBNAME_offline_table_config.json
 *  └── DBNAME_schema.json
 *
 */
public abstract class QuickStartBase {
  private static final Logger LOGGER = LoggerFactory.getLogger(QuickStartBase.class);
  private static final String TAB = "\t\t";
  private static final String NEW_LINE = "\n";

  protected static final String[] DEFAULT_OFFLINE_TABLE_DIRECTORIES = new String[]{
      "examples/batch/airlineStats",
      "examples/minions/batch/baseballStats",
      "examples/batch/dimBaseballTeams",
      "examples/batch/starbucksStores",
      "examples/batch/githubEvents",
      "examples/batch/githubComplexTypeEvents"
  };

  protected File _dataDir = FileUtils.getTempDirectory();
  protected String[] _bootstrapDataDirs;
  protected String _zkExternalAddress;
  protected String _configFilePath;

  public QuickStartBase setDataDir(String dataDir) {
    _dataDir = new File(dataDir);
    return this;
  }

  public QuickStartBase setBootstrapDataDirs(String[] bootstrapDataDirs) {
    _bootstrapDataDirs = bootstrapDataDirs;
    return this;
  }

  /** @return Table name if specified by command line argument -bootstrapTableDir; otherwise, default. */
  public String getTableName(String bootstrapDataDir) {
    return Paths.get(bootstrapDataDir).getFileName().toString();
  }

  /** @return true if bootstrapTableDir is not specified by command line argument -bootstrapTableDir, else false.*/
  public boolean useDefaultBootstrapTableDir() {
    return _bootstrapDataDirs == null;
  }

  public QuickStartBase setZkExternalAddress(String zkExternalAddress) {
    _zkExternalAddress = zkExternalAddress;
    return this;
  }

  public QuickStartBase setConfigFilePath(String configFilePath) {
    _configFilePath = configFilePath;
    return this;
  }

  public abstract List<String> types();

  public void runSampleQueries(QuickstartRunner runner)
      throws Exception {
  }

  protected void waitForBootstrapToComplete(QuickstartRunner runner)
      throws Exception {
    QuickStartBase.printStatus(Quickstart.Color.CYAN,
        "***** Waiting for 5 seconds for the server to fetch the assigned segment *****");
    Thread.sleep(5000);
  }

  public static void printStatus(Quickstart.Color color, String message) {
    System.out.println(color.getCode() + message + Quickstart.Color.RESET.getCode());
  }

  public abstract void execute()
      throws Exception;

  protected List<QuickstartTableRequest> bootstrapOfflineTableDirectories(File quickstartTmpDir)
      throws IOException {
    List<QuickstartTableRequest> quickstartTableRequests = new ArrayList<>();
    for (String directory : getDefaultBatchTableDirectories()) {
      String tableName = getTableName(directory);
      File baseDir = new File(quickstartTmpDir, tableName);
      File dataDir = new File(baseDir, "rawdata");
      Preconditions.checkState(dataDir.mkdirs());
      if (useDefaultBootstrapTableDir()) {
        copyResourceTableToTmpDirectory(directory, tableName, baseDir, dataDir, false);
      } else {
        copyFilesystemTableToTmpDirectory(directory, tableName, baseDir);
      }
      quickstartTableRequests.add(new QuickstartTableRequest(baseDir.getAbsolutePath()));
    }
    return quickstartTableRequests;
  }

  private static void copyResourceTableToTmpDirectory(String sourcePath, String tableName, File baseDir, File dataDir,
      boolean isStreamTable)
      throws IOException {
    ClassLoader classLoader = Quickstart.class.getClassLoader();
    // Copy schema
    URL resource = classLoader.getResource(sourcePath + File.separator + tableName + "_schema.json");
    Preconditions.checkNotNull(resource, "Missing schema json file for table - " + tableName);
    File schemaFile = new File(baseDir, tableName + "_schema.json");
    FileUtils.copyURLToFile(resource, schemaFile);

    // Copy table config
    String tableConfigFileSuffix = isStreamTable ? "_realtime_table_config.json" : "_offline_table_config.json";
    File tableConfigFile = new File(baseDir, tableName + tableConfigFileSuffix);
    String sourceTableConfig = sourcePath + File.separator + tableName + tableConfigFileSuffix;
    resource = classLoader.getResource(sourceTableConfig);
    Preconditions.checkNotNull(resource, "Missing table config file for table - " + tableName);
    FileUtils.copyURLToFile(resource, tableConfigFile);

    // Copy raw data
    String sourceRawDataPath = sourcePath + File.separator + "rawdata";
    resource = classLoader.getResource(sourceRawDataPath);
    if (resource != null) {
      File rawDataDir = new File(resource.getFile());
      if (rawDataDir.isDirectory()) {
        // Copy the directory from `pinot-tools/src/main/resources/examples` directory. This code path is used for
        // running Quickstart inside IDE, `ClassLoader.getResource()` should source it at build directory,
        // e.g. `/pinot-tools/target/classes/examples/batch/airlineStats/rawdata`
        FileUtils.copyDirectory(rawDataDir, dataDir);
      } else {
        // Copy the directory recursively from a jar file. This code path is used for running Quickstart using
        // pinot-admin script. The `ClassLoader.getResource()` should found the resources in the jar file then
        // decompress it, e.g. `lib/pinot-all-jar-with-dependencies.jar!/examples/batch/airlineStats/rawdata`
        String[] jarPathSplits = resource.toString().split("!/", 2);
        JarUtils.copyResourcesToDirectory(jarPathSplits[0], jarPathSplits[1], dataDir.getAbsolutePath());
      }
    } else {
      LOGGER.warn("Not found rawdata directory for table {} from {}", tableName, sourceRawDataPath);
    }

    if (!isStreamTable) {
      // Copy ingestion job spec file
      resource = classLoader.getResource(sourcePath + File.separator + "ingestionJobSpec.yaml");
      if (resource != null) {
        File ingestionJobSpecFile = new File(baseDir, "ingestionJobSpec.yaml");
        FileUtils.copyURLToFile(resource, ingestionJobSpecFile);
      }
    }
  }

  private static void copyFilesystemTableToTmpDirectory(String sourcePath, String tableName, File baseDir)
      throws IOException {
    File fileDb = new File(sourcePath);

    if (!fileDb.exists() || !fileDb.isDirectory()) {
      throw new RuntimeException("Directory " + fileDb.getAbsolutePath() + " not found.");
    }

    File schemaFile = new File(fileDb, tableName + "_schema.json");
    if (!schemaFile.exists()) {
      throw new RuntimeException("Schema file " + schemaFile.getAbsolutePath() + " not found.");
    }

    File tableFile = new File(fileDb, tableName + "_offline_table_config.json");
    if (!tableFile.exists()) {
      throw new RuntimeException("Table table " + tableFile.getAbsolutePath() + " not found.");
    }

    File data = new File(fileDb, "rawdata" + File.separator + tableName + "_data.csv");
    if (!data.exists()) {
      throw new RuntimeException(("Data file " + data.getAbsolutePath() + " not found. "));
    }

    FileUtils.copyDirectory(fileDb, baseDir);
  }

  protected Map<String, Object> getConfigOverrides() {
    try {
      return StringUtils.isEmpty(_configFilePath) ? ImmutableMap.of()
          : PinotConfigUtils.readConfigFromFile(_configFilePath);
    } catch (ConfigurationException e) {
      throw new RuntimeException(e);
    }
  }

  protected String[] getDefaultBatchTableDirectories() {
    return DEFAULT_OFFLINE_TABLE_DIRECTORIES;
  }

  public static String prettyPrintResponse(JsonNode response) {
    StringBuilder responseBuilder = new StringBuilder();

    // Sql Results
    if (response.has("resultTable")) {
      JsonNode columns = response.get("resultTable").get("dataSchema").get("columnNames");
      int numColumns = columns.size();
      for (int i = 0; i < numColumns; i++) {
        responseBuilder.append(columns.get(i).asText()).append(TAB);
      }
      responseBuilder.append(NEW_LINE);
      JsonNode rows = response.get("resultTable").get("rows");
      for (int i = 0; i < rows.size(); i++) {
        JsonNode row = rows.get(i);
        for (int j = 0; j < numColumns; j++) {
          responseBuilder.append(row.get(j).asText()).append(TAB);
        }
        responseBuilder.append(NEW_LINE);
      }
    }
    return responseBuilder.toString();
  }
}
