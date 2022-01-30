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

import com.google.common.collect.ImmutableMap;
import java.io.File;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.pinot.tools.admin.command.QuickstartRunner;
import org.apache.pinot.tools.utils.PinotConfigUtils;


public abstract class QuickStartBase {
  protected File _dataDir = FileUtils.getTempDirectory();
  protected String _bootstrapDataDir;
  protected String _zkExternalAddress;
  protected String _configFilePath;

  public QuickStartBase setDataDir(String dataDir) {
    _dataDir = new File(dataDir);
    return this;
  }

  public QuickStartBase setBootstrapDataDir(String bootstrapDataDir) {
    _bootstrapDataDir = bootstrapDataDir;
    return this;
  }

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
   * @return bootstrap path if specified by command line argument -bootstrapTableDir; otherwise, default.
   */
  public String getBootstrapDataDir(String bootstrapDataDir) {
    return _bootstrapDataDir != null ? _bootstrapDataDir : bootstrapDataDir;
  }

  /** @return Table name if specified by command line argument -bootstrapTableDir; otherwise, default. */
  public String getTableName(String bootstrapDataDir) {
    return Paths.get(getBootstrapDataDir(bootstrapDataDir)).getFileName().toString();
  }

  /** @return true if bootstrapTableDir is not specified by command line argument -bootstrapTableDir, else false.*/
  public boolean useDefaultBootstrapTableDir() {
    return _bootstrapDataDir == null;
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

  protected Map<String, Object> getConfigOverrides() {
    try {
      return StringUtils.isEmpty(_configFilePath) ? ImmutableMap.of()
          : PinotConfigUtils.readConfigFromFile(_configFilePath);
    } catch (ConfigurationException e) {
      throw new RuntimeException(e);
    }
  }
}
