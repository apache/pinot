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
  @CommandLine.Option(names = {"-help", "-h", "--h", "--help"}, required = false, help = true, description = "Print "
      + "this message.")
  private boolean _help = false;
  @CommandLine.Option(names = {"-jobSpecFile", "-jobSpec"}, required = true, description = "Ingestion job spec file")
  private String _jobSpecFile;
  @CommandLine.Option(names = {"-values"}, required = false, arity = "1..*", description = "Context values set to the"
      + " job spec template")
  private List<String> _values;
  @CommandLine.Option(names = {"-propertyFile"}, required = false, description = "A property file contains context "
      + "values to set the job spec template")
  private String _propertyFile;
  @CommandLine.Option(names = {"-pluginsToLoad"}, required = false, arity = "1..*", description = "Plugins to Load")
  private List<String> _pluginsToLoad;
  @CommandLine.Option(names = {"-pinotJarsDir"}, required = false, description = "Pinot binary installation directory")
  private String _pinotJarDir;
  @CommandLine.Option(names = {"-deployMode"}, required = false, description = "Spark Deploy Mode")
  private String _deployMode;
  @CommandLine.Option(names = {"-master"}, required = false, defaultValue = "local", description = "Spark Master")
  private String _sparkMaster;
  @CommandLine.Unmatched
  private String[] _unmatchedArgs;

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
    if (_pinotJarDir == null) {
      if (System.getenv("BASEDIR") != null) {
        _pinotJarDir = System.getenv("BASEDIR");
      } else {
        throw new RuntimeException("Either option -pinotJarDir or env BASEDIR must be set. "
            + "Currently null");
      }
    }

    SparkLauncher sparkLauncher = new SparkLauncher();
    sparkLauncher.setMaster(_sparkMaster);
    if (_deployMode != null) {
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
    if (_propertyFile != null) {
      sparkLauncher.addAppArgs("-propertyFile", _propertyFile);
    }

    if (_values != null) {
      sparkLauncher.addAppArgs("-values", Joiner.on(",").join(_values));
    }

    if (_unmatchedArgs != null) {
      sparkLauncher.addAppArgs(_unmatchedArgs);
    }

    sparkLauncher.setAppName("Pinot Spark Ingestion Job");
    sparkLauncher.setVerbose(true);
    sparkLauncher.redirectToLog(LOGGER.getName());
    Process process = sparkLauncher.launch();
    process.waitFor();
    return true;
  }

  private void addAppResource(SparkLauncher sparkLauncher, String depsJarDir)
      throws IOException {
    if (depsJarDir != null) {
      URI depsJarDirURI = URI.create(depsJarDir);
      if (depsJarDirURI.getScheme() == null) {
        depsJarDirURI = new File(depsJarDir).toURI();
      }
      PinotFS pinotFS = PinotFSFactory.create(depsJarDirURI.getScheme());
      String[] files = pinotFS.listFiles(depsJarDirURI, true);
      for (String file : files) {
        if (!pinotFS.isDirectory(URI.create(file))) {
          if (file.endsWith(".jar") && file.contains("pinot-all")) {
            LOGGER.info("Adding jar: {} to appResource", file);
            URI fileUri = URI.create(file);
            if (fileUri.getScheme() == null) {
              fileUri = new File(file).toURI();
            }
            Path path = Paths.get(fileUri);
            String fileName = path.getFileName().toString();
            if (_deployMode != null && _deployMode.contentEquals("cluster")) {
              sparkLauncher.setAppResource("local://" + fileName);
            } else {
              sparkLauncher.setAppResource(file);
            }
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
            URI fileUri = URI.create(file);
            if (fileUri.getScheme() == null) {
              fileUri = new File(file).toURI();
            }
            Path path = Paths.get(fileUri);
            String fileName = path.getFileName().toString();
            String parentDir = path.getParent().getFileName().toString();
            if (_pluginsToLoad != null && _pluginsToLoad.contains(parentDir)) {
              addJarFilePath(sparkLauncher, fileName, jarFiles, file);
            } else {
              addJarFilePath(sparkLauncher, fileName, jarFiles, file);
            }
          }
        }
      }

      String extraClassPaths = Joiner.on(":").join(jarFiles);
      sparkLauncher.setConf("spark.driver.extraClassPath", extraClassPaths);
      sparkLauncher.setConf("spark.executor.extraClassPath", extraClassPaths);
    }
  }

  private void addJarFilePath(SparkLauncher sparkLauncher, String fileName, List<String> jarFiles, String file) {
    if (_deployMode != null && _deployMode.contentEquals("cluster")) {
      sparkLauncher.addJar(fileName);
      jarFiles.add(fileName);
    } else {
      sparkLauncher.addJar(file);
      jarFiles.add(file);
    }
  }

  @Override
  public String getName() {
    return "LaunchSparkDataIngestionJob";
  }

  @Override
  public String toString() {
    String results = "LaunchSparkDataIngestionJob -jobSpecFile " + _jobSpecFile;
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
