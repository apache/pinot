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

import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import org.apache.pinot.spi.plugin.PluginManager;
import org.apache.pinot.tools.Command;
import org.apache.pinot.tools.QuickStartBase;
import org.reflections.Reflections;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;


@CommandLine.Command(name = "QuickStart", description = "Launch a complete Pinot cluster within one single process "
                                                        + "and import pre-built datasets.", mixinStandardHelpOptions
    = true)
public class QuickStartCommand extends AbstractBaseAdminCommand implements Command {
  private static final Logger LOGGER = LoggerFactory.getLogger(QuickStartCommand.class.getName());

  @CommandLine.Option(names = "-type", required = false,
      description = "Type of quickstart, supported: STREAM/BATCH/HYBRID")
  private String _type;

  @CommandLine.Option(names = {"-bootstrapTableDir"}, required = false, arity = "1..*",
      description = "A list of Directories, each directory containing table schema, config, and data.")
  private String[] _bootstrapTableDirs;

  @CommandLine.Option(names = {"-tmpDir", "-quickstartDir", "-dataDir"}, required = false,
      description = "Temp Directory to host quickstart data")
  private String _tmpDir;

  @CommandLine.Option(names = {"-zkAddress", "-zkUrl", "-zkExternalAddress"}, required = false,
      description = "URL for an external Zookeeper instance instead of using the default embedded instance")
  private String _zkExternalAddress;

  @CommandLine.Option(names = {"-configFile", "-configFilePath"}, required = false,
      description = "Config file path to override default pinot configs")
  private String _configFilePath;

  @Override
  public String getName() {
    return "QuickStart";
  }

  public QuickStartCommand setType(String type) {
    _type = type;
    return this;
  }

  public String getType() {
    return _type;
  }

  public String getTmpDir() {
    return _tmpDir;
  }

  public void setTmpDir(String tmpDir) {
    _tmpDir = tmpDir;
  }

  public String getBootstrapDataDir() {
    return (_bootstrapTableDirs != null && _bootstrapTableDirs.length > 0) ? _bootstrapTableDirs[0] : null;
  }

  public String[] getBootstrapDataDirs() {
    return _bootstrapTableDirs;
  }

  public void setBootstrapTableDir(String bootstrapTableDir) {
    _bootstrapTableDirs = new String[]{bootstrapTableDir};
  }

  public void setBootstrapTableDirs(String[] bootstrapTableDirs) {
    _bootstrapTableDirs = bootstrapTableDirs;
  }

  public String getZkExternalAddress() {
    return _zkExternalAddress;
  }

  public void setZkExternalAddress(String zkExternalAddress) {
    _zkExternalAddress = zkExternalAddress;
  }

  public String getConfigFilePath() {
    return _configFilePath;
  }

  public void setConfigFilePath(String configFilePath) {
    _configFilePath = configFilePath;
  }

  @Override
  public String toString() {
    return ("QuickStart -type " + _type);
  }

  @Override
  public void cleanup() {
  }

  public QuickStartBase selectQuickStart(String type)
      throws NoSuchMethodException, InvocationTargetException, InstantiationException, IllegalAccessException {
    Set<Class<? extends QuickStartBase>> quickStarts = allQuickStarts();
    for (Class<? extends QuickStartBase> quickStart : quickStarts) {
      QuickStartBase quickStartBase = quickStart.getDeclaredConstructor().newInstance();
      if (quickStartBase.types().contains(type.toUpperCase())) {
        return quickStartBase;
      }
    }
    throw new UnsupportedOperationException(
        "Unsupported QuickStart type: " + type + ". " + "Valid types are: " + errroMessageFor(quickStarts));
  }

  @Override
  public boolean execute()
      throws Exception {
    PluginManager.get().init();

    if (_type == null) {
      Set<Class<? extends QuickStartBase>> quickStarts = allQuickStarts();

      throw new UnsupportedOperationException("No QuickStart type provided. "
          + "Valid types are: " + errroMessageFor(quickStarts));
    }

    QuickStartBase quickstart = selectQuickStart(_type);

    if (_tmpDir != null) {
      quickstart.setDataDir(_tmpDir);
    }

    if (_bootstrapTableDirs != null) {
      quickstart.setBootstrapDataDirs(_bootstrapTableDirs);
    }

    if (_zkExternalAddress != null) {
      quickstart.setZkExternalAddress(_zkExternalAddress);
    }

    if (_configFilePath != null) {
      quickstart.setConfigFilePath(_configFilePath);
    }

    quickstart.execute();
    return true;
  }

  private static List<String> errroMessageFor(Set<Class<? extends QuickStartBase>> quickStarts)
      throws InstantiationException, IllegalAccessException, InvocationTargetException, NoSuchMethodException {
    List<String> validTypes = new ArrayList<>();
    for (Class<? extends QuickStartBase> quickStart : quickStarts) {
      validTypes.addAll(quickStart.getDeclaredConstructor().newInstance().types());
    }
    return validTypes;
  }

  protected Set<Class<? extends QuickStartBase>> allQuickStarts() {
    Reflections reflections = new Reflections("org.apache.pinot.tools");
    return reflections.getSubTypesOf(QuickStartBase.class);
  }
}
