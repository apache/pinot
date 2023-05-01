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
package org.apache.pinot.tools.filesystem;

import org.apache.pinot.tools.AbstractBaseCommand;
import org.apache.pinot.tools.Command;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;


@CommandLine.Command(name = "PinotFSBenchmarkRunner", description = "Run Filesystem benchmark",
    mixinStandardHelpOptions = true)
public class PinotFSBenchmarkRunner extends AbstractBaseCommand implements Command {

  private static final Logger LOGGER = LoggerFactory.getLogger(PinotFSBenchmarkRunner.class);

  @CommandLine.Option(names = {"-mode"}, required = true,
      description = "Test mode. (ALL|LISTFILES|READWRITE|DELETE|RENAME)")
  private String _mode;

  @CommandLine.Option(names = {"-pinotFSConfigFile"}, required = true,
      description = "Path for PinotFS configuration file")
  private String _pinotFSConfigFile;

  @CommandLine.Option(names = {"-baseDirectoryUri"}, required = true,
      description = "Temp dir path for running benchmark against. e.g. file:///path/to/test, abfss://host/path...")
  private String _baseDirectoryUri;

  @CommandLine.Option(names = {"-localTempDir"}, required = false, description = "Local temp directory for benchmark.")
  private String _localTempDir;

  @CommandLine.Option(names = {"-numSegmentsForListTest"}, required = false,
      description = "The number of segments to create before running listFiles test.")
  private Integer _numSegmentsForListTest;

  @CommandLine.Option(names = {"-dataSizeInMBsForCopyTest"}, required = false,
      description = "Data size in MB for copy test. (e.g. 1024 = 1GB)")
  private Integer _dataSizeInMBsForCopyTest;

  @CommandLine.Option(names = {"-numOps"}, required = false,
      description = "The number of trials of operations when running a benchmark.")
  private Integer _numOps;

  @Override
  public boolean execute()
      throws Exception {
    try {
      LOGGER.info("Run filesystem benchmark...");
      PinotFSBenchmarkDriver driver =
          new PinotFSBenchmarkDriver(_mode, _pinotFSConfigFile, _baseDirectoryUri, _localTempDir,
              _numSegmentsForListTest, _dataSizeInMBsForCopyTest, _numOps);
      driver.run();
    } catch (Exception e) {
      LOGGER.error("Error while running benchmark: ", e);
    }
    return true;
  }
}
