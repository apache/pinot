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
import com.google.common.base.Preconditions;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.apache.calcite.rel.core.Join;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.JavaVersion;
import org.apache.commons.lang3.SystemUtils;
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
//TODO: Fix Scala version causing NoSuchMethodError at runtime with a few plugins
//TODO: Cleanup descriptions for options
//TODO: Add options for most popular spark confs such as numExecutors
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
  @CommandLine.Option(names = {"-pluginsToLoad"}, required = false, arity = "1..*", split = ":", description = "Plugins to Load")
  private List<String> _pluginsToLoad;
  @CommandLine.Option(names = {"-pinotBaseDir"}, required = false, description = "Pinot binary installation directory")
  private String _pinotBaseDir;
  @CommandLine.Option(names = {"-deployMode"}, required = false, description = "Spark Deploy Mode")
  private String _deployMode;
  @CommandLine.Option(names = {"-master"}, required = false, defaultValue = "local", description = "Spark Master")
  private String _sparkMaster;
  @CommandLine.Option(names = {"-sparkVersion"}, required = false, defaultValue = "SPARK_3", description = "Spark "
      + "Type - can be one of Spark_2 or Spark_3")
  private SparkType _sparkVersion;
  @CommandLine.Option(names = {"-verbose"}, required = false, defaultValue = "true", description = "Enable verbose logging")
  private boolean _verbose;
  @CommandLine.Option(names = {"-sparkConf"}, required = false, split = ":", mapFallbackValue = "", description = "Spark Conf")
  private Map<String, String> _sparkConf;
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
    if (_pinotBaseDir == null) {
      String baseDir = System.getProperty("basedir");
      if (baseDir != null) {
        _pinotBaseDir = baseDir;
      } else {
        throw new RuntimeException("Either option -pinotBaseDir or env BASEDIR must be set. "
            + "Currently null");
      }
    }

    Preconditions.checkNotNull(System.getenv("SPARK_HOME"),
        "SPARK_HOME environment variable should be set to Spark installation path");

    boolean isAppropriateJavaVersion = SystemUtils.isJavaVersionAtMost(_sparkVersion.getJavaVersion());
    if (!isAppropriateJavaVersion) {
      LOGGER.warn(
          "Platform java version should be at most: {}, found: {}. "
              + "Ignore this warning if you are running from different environment than your spark cluster",
          _sparkVersion.getSparkVersion(), SystemUtils.JAVA_SPECIFICATION_VERSION);
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

    List<String> extraClassPaths = new ArrayList<>();
    addDepsJarToDistributedCache(sparkLauncher, _pinotBaseDir, extraClassPaths);
    addAppResource(sparkLauncher, _pinotBaseDir, extraClassPaths);

    String extraClassPathsString = Joiner.on(":").join(extraClassPaths);
    sparkLauncher.setConf(SparkLauncher.DRIVER_EXTRA_CLASSPATH, extraClassPathsString);
    sparkLauncher.setConf(SparkLauncher.EXECUTOR_EXTRA_CLASSPATH, extraClassPathsString);

    if(_deployMode != null && _deployMode.contentEquals("cluster")) {
      sparkLauncher.addFile(_jobSpecFile);
      sparkLauncher.addAppArgs("-jobSpecFile", FilenameUtils.getName(_jobSpecFile));
    } else {
      sparkLauncher.addAppArgs("-jobSpecFile", _jobSpecFile);
    }

    if (_propertyFile != null) {
      sparkLauncher.addAppArgs("-propertyFile", _propertyFile);
    }

    if (_values != null) {
      sparkLauncher.addAppArgs("-values", Joiner.on(",").join(_values));
    }

    if (_sparkConf != null) {
      for (Map.Entry<String, String> conf : _sparkConf.entrySet()) {
        sparkLauncher.setConf(conf.getKey(), conf.getValue());
      }
    }

    if (_unmatchedArgs != null) {
      sparkLauncher.addAppArgs(_unmatchedArgs);
    }
    sparkLauncher.setAppName("Pinot Spark Ingestion Job");
    sparkLauncher.setVerbose(_verbose);
    sparkLauncher.redirectToLog(LOGGER.getName());
    Process process = sparkLauncher.launch();
    process.waitFor();
    return true;
  }

  class SparkAppListener implements SparkAppHandle.Listener {
    @Override
    public void stateChanged(SparkAppHandle sparkAppHandle) {
      LOGGER.info("Spark Application State changed: {}", sparkAppHandle.getState().toString());
    }

    @Override
    public void infoChanged(SparkAppHandle sparkAppHandle) {
      LOGGER.info("Spark Info changed: {}", sparkAppHandle.getState().toString());
    }
  }

  //TODO: Handle DFS paths correctly
  private void addAppResource(SparkLauncher sparkLauncher, String depsJarDir, List<String> extraClassPath)
      throws IOException {
    if (depsJarDir != null) {
      URI depsJarDirURI = URI.create(depsJarDir);
      if (depsJarDirURI.getScheme() == null) {
        depsJarDirURI = new File(depsJarDir).toURI();
      }
      PinotFS pinotFS = PinotFSFactory.create(depsJarDirURI.getScheme());
      String[] files = pinotFS.listFiles(depsJarDirURI, true);
      for (String file : files) {
        URI fileUri = URI.create(file);
        if (!pinotFS.isDirectory(fileUri)) {
          if (file.endsWith(".jar") && file.contains("pinot-all")) {
            LOGGER.info("Adding jar: {} to appResource", file);
            String fileName = FilenameUtils.getName(file);
            if(_deployMode != null && _deployMode.contentEquals("cluster")) {
              sparkLauncher.setAppResource("local://" + fileName);
              sparkLauncher.addJar(file);
              extraClassPath.add(fileName);
            } else  if (fileUri.getScheme() == null || fileUri.getScheme().contentEquals("file")) {
              sparkLauncher.setAppResource("local://" + file);
              sparkLauncher.addJar(file);
              extraClassPath.add(file);
            } else {
              sparkLauncher.setAppResource("local://" + fileName);
              sparkLauncher.addJar(file);
              extraClassPath.add(fileName);
            }
          }
        }
      }
    }
  }

  private void addDepsJarToDistributedCache(SparkLauncher sparkLauncher, String depsJarDir, List<String> extraClassPath)
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
          if (file.endsWith(".jar")) {
            String parentDir = FilenameUtils.getName(file.substring(0, file.lastIndexOf('/')));

            if (_pluginsToLoad == null || _pluginsToLoad.contains(parentDir) ||
                _sparkVersion.getPluginName().contentEquals(parentDir)) {
              addJarFilePath(sparkLauncher, extraClassPath, file);
            }
          }
        }
      }
    }
  }

  private void addJarFilePath(SparkLauncher sparkLauncher, List<String> extraClassPath, String file) {
    URI fileUri = URI.create(file);
    sparkLauncher.addJar(file);
    if(_deployMode != null && _deployMode.contentEquals("cluster")) {
      LOGGER.info("Adding deps jar: {} to distributed cache", file);
      String fileName = FilenameUtils.getName(file);
      if(!fileName.isEmpty()) {
        extraClassPath.add(fileName);
      }
    } else if (fileUri.getScheme() == null || fileUri.getScheme().contentEquals("file")) {
      LOGGER.info("Adding deps jar: {} to distributed cache", file);
      extraClassPath.add(file);
    } else {
      LOGGER.info("Adding deps jar: {} to distributed cache", file);
      String fileName = FilenameUtils.getName(file);
      if(!fileName.isEmpty()) {
        extraClassPath.add(fileName);
      }
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

  enum SparkType {
    SPARK_2("2.4.0", "pinot-batch-ingestion-spark-2.4", JavaVersion.JAVA_1_8),
    SPARK_3("3.2.1", "pinot-batch-ingestion-spark-3.2", JavaVersion.JAVA_11);

    private final String _sparkVersion;
    private final String _pluginName;
    private final JavaVersion _javaVersion;

    SparkType(String sparkVersion, String pluginName, JavaVersion javaVersion) {
      _sparkVersion = sparkVersion;
      _pluginName = pluginName;
      _javaVersion = javaVersion;
    }

    public String getSparkVersion() {
      return _sparkVersion;
    }

    public String getPluginName() {
      return _pluginName;
    }

    public JavaVersion getJavaVersion() {
      return _javaVersion;
    }
  }
}
