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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
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
import org.apache.spark.launcher.SparkLauncher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;


/**
 * Class to implement LaunchDataIngestionJob command.
 *
 */
@CommandLine.Command(name = "LaunchSparkDataIngestionJob", description = "Launch a data ingestion job.",
    mixinStandardHelpOptions = true)
public class LaunchSparkDataIngestionJobCommand extends AbstractBaseAdminCommand implements Command {
  private static final Logger LOGGER = LoggerFactory.getLogger(LaunchSparkDataIngestionJobCommand.class);
  public static final String MAIN_CLASS = "org.apache.pinot.tools.admin.command.LaunchDataIngestionJobCommand";
  public static final String SPARK_HOME = "SPARK_HOME";
  public static final String BASEDIR = "basedir";
  public static final String LOCAL_FILE_PREFIX = "local://";
  public static final String PINOT_MAIN_JAR_PREFIX = "pinot-all";

  @CommandLine.Option(names = {"-jobSpecFile", "-jobSpec"}, required = true, description = "Ingestion job spec file")
  private String _jobSpecFile;
  @CommandLine.Option(names = {"-values"}, required = false, arity = "1..*", description = "Context values set to the"
      + " job spec template")
  private List<String> _values;
  @CommandLine.Option(names = {"-propertyFile"}, required = false, description = "A property file contains context "
      + "values to set the job spec template")
  private String _propertyFile;
  @CommandLine.Option(names = {"-pluginsToLoad"}, required = false, arity = "1..*", split = ":", description = "List "
      + "of plugin name separated by : to load at runtime. e.g. pinto-s3:pinot-parquet")
  private List<String> _pluginsToLoad;

  // Kafka plugins need to be excluded as they contain scala dependencies which cause
  // NoSuchMethodErrors with runtime spark.
  // It is also fine to exclude Kafka plugins as they are not going to be used in batch ingestion in any case
  @CommandLine.Option(names = {"-pluginsToExclude"}, defaultValue = "pinot-kafka-0.9:pinot-kafka-2.0", required =
      false, arity = "1..*", split = ":", description =
      "List " + "of plugin name separated by : to not load at runtime. e.g. pinto-s3:pinot-parquet")
  private List<String> _pluginsToExclude;
  @CommandLine.Option(names = {"-pinotBaseDir"}, required = false, description = "Pinot binary installation directory")
  private String _pinotBaseDir;
  @CommandLine.Option(names = {"-deployMode"}, required = false, description = "Spark Deploy Mode")
  private String _deployMode;
  @CommandLine.Option(names = {"-master"}, required = false, defaultValue = "local", description = "Spark Master")
  private String _sparkMaster;
  @CommandLine.Option(names = {"-sparkVersion"}, required = false, defaultValue = "SPARK_3", description = "Spark "
      + "plugin to use - can be one of Spark_2 or Spark_3")
  private SparkType _sparkVersion;
  @CommandLine.Option(names = {"-verbose"}, required = false, defaultValue = "true", description = "Enable verbose "
      + "logging from launcher")
  private boolean _verbose;
  @CommandLine.Option(names = {"-sparkConf"}, required = false, split = ":", mapFallbackValue = "", description =
      "Additional Spark configuration values as key value pairs separated by : e.g. -sparkConf spark.executor"
          + ".cores=2:num-executors=3")
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
  public boolean execute()
      throws Exception {
    if (_pinotBaseDir == null) {
      String baseDir = System.getProperty(BASEDIR);
      if (baseDir != null) {
        _pinotBaseDir = baseDir;
      } else {
        throw new RuntimeException(
            String.format("Either option -pinotBaseDir or env %s must be set. " + "Currently null", BASEDIR));
      }
    }

    Preconditions.checkNotNull(System.getenv(SPARK_HOME),
        "SPARK_HOME environment variable should be set to Spark installation path");

    boolean isAppropriateJavaVersion = SystemUtils.isJavaVersionAtMost(_sparkVersion.getJavaVersion());
    if (!isAppropriateJavaVersion) {
      LOGGER.warn("Platform java version should be at most: {}, found: {}. "
              + "Ignore this warning if you are running from different environment than your spark cluster",
          _sparkVersion.getJavaVersion(), SystemUtils.JAVA_SPECIFICATION_VERSION);
    }

    SparkLauncher sparkLauncher = new SparkLauncher();
    sparkLauncher.setMaster(_sparkMaster);
    if (_deployMode != null) {
      sparkLauncher.setDeployMode(_deployMode);
    }

    sparkLauncher.setMainClass(MAIN_CLASS);

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

    if (isClusterDeployMode()) {
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
        if (conf.getKey().startsWith("spark")) {
          sparkLauncher.setConf(conf.getKey(), conf.getValue());
        } else {
          sparkLauncher.addSparkArg("--" + conf.getKey(), conf.getValue());
        }
      }
    }

    if (_unmatchedArgs != null) {
      sparkLauncher.addAppArgs(_unmatchedArgs);
    }
    sparkLauncher.setAppName("Pinot Spark Ingestion Job");
    sparkLauncher.setVerbose(_verbose);
    sparkLauncher.redirectOutput(ProcessBuilder.Redirect.INHERIT);
    sparkLauncher.redirectError(ProcessBuilder.Redirect.INHERIT);
    Process process = sparkLauncher.launch();
    process.waitFor();
    return true;
  }

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
          if (file.endsWith(".jar") && file.contains(PINOT_MAIN_JAR_PREFIX)) {
            LOGGER.info("Adding jar: {} to appResource", file);
            String fileName = FilenameUtils.getName(file);
            if (isClusterDeployMode() || !isLocalFileUri(fileUri)) {
              sparkLauncher.setAppResource(LOCAL_FILE_PREFIX + fileName);
              sparkLauncher.addJar(file);
              extraClassPath.add(fileName);
            } else {
              sparkLauncher.setAppResource(LOCAL_FILE_PREFIX + file);
              sparkLauncher.addJar(file);
              extraClassPath.add(file);
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

            if (shouldLoadPlugin(parentDir)) {
              addJarFilePath(sparkLauncher, extraClassPath, file);
            }
          }
        }
      }
    }
  }

  private boolean shouldLoadPlugin(String parentDir) {
    boolean shouldLoadPlugin = _pluginsToLoad == null && !_pluginsToExclude.contains(parentDir)
        && !parentDir.contains("spark");
    shouldLoadPlugin = shouldLoadPlugin || (_pluginsToLoad != null && _pluginsToLoad.contains(parentDir));
    shouldLoadPlugin = shouldLoadPlugin || _sparkVersion.getPluginName().contentEquals(parentDir);
    return shouldLoadPlugin;
  }

  private void addJarFilePath(SparkLauncher sparkLauncher, List<String> extraClassPath, String file) {
    URI fileUri = URI.create(file);
    sparkLauncher.addJar(file);
    if (isClusterDeployMode() || !isLocalFileUri(fileUri)) {
      LOGGER.info("Adding deps jar: {} to distributed cache", file);
      String fileName = FilenameUtils.getName(file);
      if (!fileName.isEmpty()) {
        extraClassPath.add(fileName);
      }
    } else {
      LOGGER.info("Adding deps jar: {} to distributed cache", file);
      extraClassPath.add(file);
    }
  }

  private boolean isLocalFileUri(URI fileUri) {
    return fileUri.getScheme() == null || fileUri.getScheme().contentEquals("file");
  }

  private boolean isClusterDeployMode() {
    return _deployMode != null && _deployMode.contentEquals("cluster");
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

  public static void main(String[] args) {
    PluginManager.get().init();
    int exitCode = new CommandLine(new LaunchSparkDataIngestionJobCommand()).execute(args);
    System.exit(exitCode);
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
