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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.pinot.spi.ingestion.batch.IngestionJobLauncher;
import org.apache.pinot.tools.Command;
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

  @Option(name = "-jobSpecFile", required = true, metaVar = "<string>", usage = "Ingestion job spec file")
  private String _jobSpecFile;

  @Option(name = "-values", required = false, metaVar = "<template context>", handler = StringArrayOptionHandler.class, usage = "Context values set to the job spec template")
  private List<String> _values;

  @Option(name = "-help", required = false, help = true, aliases = {"-h", "--h", "--help"}, usage = "Print this message.")
  private boolean _help = false;

  @Override
  public boolean getHelp() {
    return _help;
  }

  @Override
  public boolean execute()
      throws Exception {
    try {
      List<String> arguments = new ArrayList();
      arguments.add(_jobSpecFile);
      arguments.addAll(_values);
      IngestionJobLauncher.main(arguments.toArray(new String[0]));
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
    return ("LaunchDataIngestionJob -jobSpecFile " + _jobSpecFile + " -values " + Arrays.toString(_values.toArray()));
  }

  @Override
  public String description() {
    return "Launch a data ingestion job.";
  }
}
