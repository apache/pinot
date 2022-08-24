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

import com.google.common.base.Joiner;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.pinot.spi.auth.AuthProvider;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.filesystem.PinotFS;
import org.apache.pinot.spi.filesystem.PinotFSFactory;
import org.apache.pinot.spi.ingestion.batch.IngestionJobLauncher;
import org.apache.pinot.spi.ingestion.batch.spec.PinotFSSpec;
import org.apache.pinot.spi.ingestion.batch.spec.SegmentGenerationJobSpec;
import org.apache.pinot.spi.plugin.PluginManager;
import org.apache.pinot.spi.utils.GroovyTemplateUtils;
import org.apache.pinot.tools.Command;
import org.apache.spark.launcher.SparkAppHandle;
import org.apache.spark.launcher.SparkLauncher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;


/**
 * Class to implement LaunchDataIngestionJob command.
 *
 */
@CommandLine.Command(name = "LaunchSparkDataIngestionJob")
public class LaunchSparkDataIngestionJobCommand extends AbstractBaseAdminCommand implements Command {
  private static final Logger LOGGER = LoggerFactory.getLogger(LaunchSparkDataIngestionJobCommand.class);
  @CommandLine.Option(names = {"-help", "-h", "--h", "--help"}, required = false, help = true,
      description = "Print this message.")
  private boolean _help = false;
  @CommandLine.Option(names = {"-jobSpecFile", "-jobSpec"}, required = true,
      description = "Ingestion job spec file")
  private String _jobSpecFile;
  @CommandLine.Option(names = {"-values"}, required = false, arity = "1..*",
      description = "Context values set to the job spec template")
  private List<String> _values;
  @CommandLine.Option(names = {"-propertyFile"}, required = false,
      description = "A property file contains context values to set the job spec template")
  private String _propertyFile;
  @CommandLine.Option(names = {"-user"}, required = false, description = "Username for basic auth.")
  private String _user;
  @CommandLine.Option(names = {"-password"}, required = false, description = "Password for basic auth.")
  private String _password;
  @CommandLine.Option(names = {"-authToken"}, required = false, description = "Http auth token.")
  private String _authToken;
  @CommandLine.Option(names = {"-authTokenUrl"}, required = false, description = "Http auth token url.")
  private String _authTokenUrl;
  @CommandLine.Option(names = {"-pluginsToLoad"}, required = false, arity = "1..*", description = "Plugins to Load")
  private List<String> _pluginsToLoad;
  @CommandLine.Option(names = {"-pinotJarsDir"}, required = true, description = "Pinot binary installation directory")
  private String _pinotJarDir;
  @CommandLine.Option(names = {"-deployMode"}, required = false, description = "Spark Deploy Mode")
  private String _deployMode;
  @CommandLine.Option(names = {"-master"}, required = false, defaultValue = "local", description = "Spark Master")
  private String _sparkMaster;

  private AuthProvider _authProvider;

  public String getJobSpecFile() {
    return _jobSpecFile;
  }

  public void setJobSpecFile(String jobSpecFile) {
    _jobSpecFile = jobSpecFile;
  }

  public List<String> getValues() {
    return _values;
  }

  public void setValues(List<String> values) {
    _values = values;
  }

  public String getPropertyFile() {
    return _propertyFile;
  }

  public void setPropertyFile(String propertyFile) {
    _propertyFile = propertyFile;
  }

  public void setAuthProvider(AuthProvider authProvider) {
    _authProvider = authProvider;
  }

  @Override
  public boolean getHelp() {
    return _help;
  }

  public void setHelp(boolean help) {
    _help = help;
  }


  @Override
  public boolean execute()
      throws Exception {
    SparkLauncher sparkLauncher = new SparkLauncher();
    sparkLauncher.setMaster(_sparkMaster);
    if(_deployMode != null) {
      sparkLauncher.setDeployMode(_deployMode);
    }
    sparkLauncher.setMainClass("org.apache.pinot.tools.admin.command.LaunchDataIngestionJobCommand");
    SegmentGenerationJobSpec spec;
    try {
      spec = IngestionJobLauncher.getSegmentGenerationJobSpec(_jobSpecFile, _propertyFile,
          GroovyTemplateUtils.getTemplateContext(_values), System.getenv());
    } catch (Exception e) {
      LOGGER.error("Got exception to generate IngestionJobSpec for data ingestion job - ", e);
      throw e;
    }

    List<PinotFSSpec> pinotFSSpecs = spec.getPinotFSSpecs();
    for (PinotFSSpec pinotFSSpec : pinotFSSpecs) {
      PinotFSFactory.register(pinotFSSpec.getScheme(), pinotFSSpec.getClassName(), new PinotConfiguration(pinotFSSpec));
    }

    addDepsJarToDistributedCache(sparkLauncher, _pinotJarDir);
    addAppResource(sparkLauncher, _pinotJarDir);

    //TODO: Add rest of the args
    sparkLauncher.addAppArgs("-jobSpecFile", _jobSpecFile);

    sparkLauncher.setAppName("Pinot Spark Ingestion Job");
    sparkLauncher.setVerbose(true);
    sparkLauncher.redirectToLog(LOGGER.getName());
    SparkAppListener listener = new SparkAppListener();
//    SparkAppHandle sparkAppHandle = sparkLauncher.startApplication(listener);
//    sparkAppHandle.addListener(listener);
    Process process = sparkLauncher.launch();
    process.waitFor();
    return true;
  }

  class SparkAppListener implements SparkAppHandle.Listener {
    @Override
    public void stateChanged(SparkAppHandle sparkAppHandle) {
      LOGGER.info("Current app State: {} for appId: {}", sparkAppHandle.getState(), sparkAppHandle.getAppId());
      while(sparkAppHandle.getState() != SparkAppHandle.State.FINISHED) {
        try {
          Thread.sleep(1000L);
        }catch (Exception e) {

        }
      }
    }

    @Override
    public void infoChanged(SparkAppHandle sparkAppHandle) {
      LOGGER.info("Current app info State: {} for appId: {}", sparkAppHandle.getState(), sparkAppHandle.getAppId());
    }
  }
  private void addAppResource(SparkLauncher sparkLauncher, String depsJarDir)
      throws IOException {
    if (depsJarDir != null) {
      URI depsJarDirURI = URI.create(depsJarDir + "/lib");
      if (depsJarDirURI.getScheme() == null) {
        depsJarDirURI = new File(depsJarDir+ "/lib").toURI();
      }
      PinotFS pinotFS = PinotFSFactory.create(depsJarDirURI.getScheme());
      String[] files = pinotFS.listFiles(depsJarDirURI, true);
      for (String file : files) {
        if (!pinotFS.isDirectory(URI.create(file))) {
          if (file.endsWith(".jar")) {
            LOGGER.info("Adding deps jar: {} to appResource", file);
              sparkLauncher.addJar(file);
//              Path path = Paths.get(URI.create(file));
              //TODO: Handle local vs s3 correctly
              sparkLauncher.setAppResource(file);
          }
        }
      }
    }
  }

  private void addDepsJarToDistributedCache(SparkLauncher sparkLauncher, String depsJarDir)
      throws IOException {
    if (depsJarDir != null) {
      URI depsJarDirURI = URI.create(depsJarDir);
      if (depsJarDirURI.getScheme() == null) {
        depsJarDirURI = new File(depsJarDir).toURI();
      }
      PinotFS pinotFS = PinotFSFactory.create(depsJarDirURI.getScheme());
      String[] files = pinotFS.listFiles(depsJarDirURI, true);
      List<String> jarFiles = new ArrayList<>();
      for (String file : files) {
        if (!pinotFS.isDirectory(URI.create(file))) {
          if (file.endsWith(".jar")) {
            LOGGER.info("Adding deps jar: {} to distributed cache", file);
            if (_pluginsToLoad == null || _pluginsToLoad.isEmpty()) {
              URI fileUri = URI.create(file);
              if (fileUri.getScheme() == null) {
                fileUri = new File(file).toURI();
              }
              Path path = Paths.get(fileUri);
              if(_deployMode != null && _deployMode.contentEquals("cluster")) {
                sparkLauncher.addJar(path.getFileName().toString());
                jarFiles.add(path.getFileName().toString());
              } else {
                sparkLauncher.addJar(file);
                jarFiles.add(file);
              }
            } else {

              URI fileUri = URI.create(file);
              if (fileUri.getScheme() == null) {
                fileUri = new File(file).toURI();
              }
              Path path = Paths.get(fileUri);
              String fileName = path.getFileName().toString();
              String parentDir = path.getParent().getFileName().toString();
              if (_pluginsToLoad.contains(parentDir)) {
                if(_deployMode != null && _deployMode.contentEquals("cluster")) {
                  sparkLauncher.addJar(fileName);
                  jarFiles.add(fileName);
                } else {
                  sparkLauncher.addJar(file);
                  jarFiles.add(file);
                }
              }
            }
          }
        }
      }

      String extraClassPaths = Joiner.on(":").join(jarFiles);
      sparkLauncher.setConf("spark.driver.extraClassPath", extraClassPaths);
      sparkLauncher.setConf("spark.executor.extraClassPath", extraClassPaths);
    }
  }

  @Override
  public String getName() {
    return "LaunchDataIngestionJob";
  }

  @Override
  public String toString() {
    String results = "LaunchDataIngestionJob -jobSpecFile " + _jobSpecFile;
    if (_propertyFile != null) {
      results += " -propertyFile " + _propertyFile;
    }
    if (_values != null) {
      results += " -values " + Arrays.toString(_values.toArray());
    }
    return results;
  }


  @Override
  public String description() {
    return "Launch a data ingestion job.";
  }

  public static void main(String[] args) {
    PluginManager.get().init();
    new CommandLine(new LaunchSparkDataIngestionJobCommand()).execute(args);
  }
}
