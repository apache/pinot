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

import java.util.Arrays;
import java.util.List;
import org.apache.pinot.core.util.TlsUtils;
import org.apache.pinot.spi.ingestion.batch.IngestionJobLauncher;
import org.apache.pinot.spi.ingestion.batch.spec.SegmentGenerationJobSpec;
import org.apache.pinot.spi.ingestion.batch.spec.TlsSpec;
import org.apache.pinot.spi.plugin.PluginManager;
import org.apache.pinot.spi.utils.GroovyTemplateUtils;
import org.apache.pinot.tools.Command;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.kohsuke.args4j.spi.StringArrayOptionHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Class to implement LaunchDataIngestionJob command.
 *
 */
public class LaunchDataIngestionJobCommand extends AbstractBaseAdminCommand implements Command {
  private static final Logger LOGGER = LoggerFactory.getLogger(LaunchDataIngestionJobCommand.class);
  @Option(name = "-help", required = false, help = true, aliases = {"-h", "--h", "--help"}, usage = "Print this message.")
  private boolean _help = false;
  @Option(name = "-jobSpecFile", required = true, metaVar = "<string>", aliases = {"-jobSpec"}, usage = "Ingestion job spec file")
  private String _jobSpecFile;
  @Option(name = "-values", required = false, metaVar = "<template context>", handler = StringArrayOptionHandler.class, usage = "Context values set to the job spec template")
  private List<String> _values;
  @Option(name = "-propertyFile", required = false, metaVar = "<template context file>", usage = "A property file contains context values to set the job spec template")
  private String _propertyFile;

  public static void main(String[] args) {
    PluginManager.get().init();
    LaunchDataIngestionJobCommand cmd = new LaunchDataIngestionJobCommand();
    CmdLineParser parser = new CmdLineParser(cmd);
    if (args.length == 0) {
      cmd.printUsage();
      return;
    }
    try {
      parser.parseArgument(args);
      if (cmd.getHelp()) {
        cmd.printUsage();
        return;
      }
      boolean status = cmd.execute();
      if (System.getProperties().getProperty("pinot.admin.system.exit", "false").equalsIgnoreCase("true")) {
        System.exit(status ? 0 : 1);
      }
    } catch (CmdLineException e) {
      LOGGER.error("Error: {}", e.getMessage());
    } catch (Exception e) {
      LOGGER.error("Exception caught: ", e);
    }
  }

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
    String jobSpecFilePath = _jobSpecFile;
    String propertyFilePath = _propertyFile;
    SegmentGenerationJobSpec spec;
    try {
      spec = IngestionJobLauncher.getSegmentGenerationJobSpec(jobSpecFilePath, propertyFilePath,
          GroovyTemplateUtils.getTemplateContext(_values));
    } catch (Exception e) {
      LOGGER.error("Got exception to generate IngestionJobSpec for data ingestion job - ", e);
      throw e;
    }

    TlsSpec tlsSpec = spec.getTlsSpec();
    if (tlsSpec != null) {
      TlsUtils.installDefaultSSLSocketFactory(tlsSpec.getKeyStorePath(), tlsSpec.getKeyStorePassword(),
          tlsSpec.getTrustStorePath(), tlsSpec.getTrustStorePassword());
    }

    try {
      IngestionJobLauncher.runIngestionJob(spec);
    } catch (Exception e) {
      LOGGER.error("Got exception to kick off standalone data ingestion job - ", e);
      throw e;
    }
    return true;
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
}
