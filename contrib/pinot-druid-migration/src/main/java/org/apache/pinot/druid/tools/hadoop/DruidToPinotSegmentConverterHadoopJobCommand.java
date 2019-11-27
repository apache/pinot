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
package org.apache.pinot.druid.tools.hadoop;

import java.io.FileInputStream;
import java.util.Properties;
import org.apache.pinot.hadoop.job.SegmentTarPushJob;
import org.apache.pinot.tools.Command;
import org.apache.pinot.tools.admin.command.AbstractBaseAdminCommand;
import org.kohsuke.args4j.Option;

/**
 * The DruidToPinotSegmentConverterHadoopJob is a tool that converts a Druid segment to a Pinot segment on Hadoop
 * and pushes the resulting segment to the specified cluster, all of which is configured in the given job properties file.
 */
public class DruidToPinotSegmentConverterHadoopJobCommand extends AbstractBaseAdminCommand implements Command {

  @Option(name = "-jobProperties", metaVar = "<string>", usage = "Path to the job properties configuration file.", required = true)
  private String _jobProperties;

  @Option(name = "-help", help = true, aliases = {"-h", "--h", "--help"}, usage = "Print this message.")
  private boolean _help = false;

  public DruidToPinotSegmentConverterHadoopJobCommand setJobProperties(String jobProperties) {
    _jobProperties = jobProperties;
    return this;
  }

  @Override
  public String toString() {
    return ("DruidToPinotSegmentConverterHadoopJob  -jobProperties " + _jobProperties);
  }

  @Override
  public final String getName() {
    return "DruidToPinotSegmentConverterHadoopJob";
  }

  @Override
  public String description() {
    return "Create and push pinot segments from the Druid segment and configurations specified in the given job.properties file.";
  }

  @Override
  public boolean getHelp() {
    return _help;
  }

  @Override
  public boolean execute() {
    if (_jobProperties == null) {
      printUsage();
    }
    Properties jobConf;
    try {
      jobConf = new Properties();
      jobConf.load(new FileInputStream(_jobProperties));
      DruidToPinotSegmentConverterHadoopJob job = new DruidToPinotSegmentConverterHadoopJob(jobConf);
      job.run();

      // Run segment push job
      SegmentTarPushJob pushJob = new SegmentTarPushJob(jobConf);
      pushJob.run();
    } catch (Exception e) {
      e.printStackTrace();
      printUsage();
      System.exit(1);
    }
    return false;
  }
}
