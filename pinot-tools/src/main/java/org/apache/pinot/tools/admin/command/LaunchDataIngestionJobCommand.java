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
import org.apache.commons.lang3.StringUtils;
import org.apache.pinot.common.auth.AuthProviderUtils;
import org.apache.pinot.common.tls.TlsUtils;
import org.apache.pinot.spi.auth.AuthProvider;
import org.apache.pinot.spi.ingestion.batch.IngestionJobLauncher;
import org.apache.pinot.spi.ingestion.batch.spec.SegmentGenerationJobSpec;
import org.apache.pinot.spi.ingestion.batch.spec.TlsSpec;
import org.apache.pinot.spi.plugin.PluginManager;
import org.apache.pinot.spi.utils.GroovyTemplateUtils;
import org.apache.pinot.tools.Command;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;


/**
 * Class to implement LaunchDataIngestionJob command.
 *
 */
@CommandLine.Command(name = "LaunchDataIngestionJob")
public class LaunchDataIngestionJobCommand extends AbstractBaseAdminCommand implements Command {
  private static final Logger LOGGER = LoggerFactory.getLogger(LaunchDataIngestionJobCommand.class);
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
    String jobSpecFilePath = _jobSpecFile;
    String propertyFilePath = _propertyFile;
    SegmentGenerationJobSpec spec;
    try {
      spec = IngestionJobLauncher.getSegmentGenerationJobSpec(jobSpecFilePath, propertyFilePath,
          GroovyTemplateUtils.getTemplateContext(_values), System.getenv());
    } catch (Exception e) {
      LOGGER.error("Got exception to generate IngestionJobSpec for data ingestion job - ", e);
      throw e;
    }

    TlsSpec tlsSpec = spec.getTlsSpec();
    if (tlsSpec != null) {
      TlsUtils.installDefaultSSLSocketFactory(tlsSpec.getKeyStoreType(), tlsSpec.getKeyStorePath(),
          tlsSpec.getKeyStorePassword(), tlsSpec.getTrustStoreType(), tlsSpec.getTrustStorePath(),
          tlsSpec.getTrustStorePassword());
    }

    if (StringUtils.isBlank(spec.getAuthToken())) {
      spec.setAuthToken(AuthProviderUtils.makeAuthProvider(_authProvider, _authTokenUrl, _authToken, _user, _password)
          .getTaskToken());
    }

    try {
      IngestionJobLauncher.runIngestionJob(spec);
    } catch (Exception e) {
      LOGGER.error("Got exception to kick off {} data ingestion job - ", spec.getExecutionFrameworkSpec().getName(), e);
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

  public static void main(String[] args) {
    PluginManager.get().init();
    int exitCode = new CommandLine(new LaunchDataIngestionJobCommand()).execute(args);
    if ((exitCode != 0)
        || Boolean.parseBoolean(System.getProperties().getProperty("pinot.admin.system.exit"))) {
      System.exit(exitCode);
    }
  }
}
