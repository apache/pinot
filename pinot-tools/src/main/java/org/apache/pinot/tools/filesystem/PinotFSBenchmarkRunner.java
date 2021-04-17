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
import org.kohsuke.args4j.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class PinotFSBenchmarkRunner extends AbstractBaseCommand implements Command {

  private static final Logger LOGGER = LoggerFactory.getLogger(PinotFSBenchmarkRunner.class);

  @Option(name = "-mode", required = true, metaVar = "<String>",
      usage = "Test mode. (ALL|LISTFILES|READWRITE|DELETE|RENAME)")
  private String _mode;

  @Option(name = "-pinotFSConfigFile", required = true, metaVar = "<String>",
      usage = "Path for PinotFS configuration file")
  private String _pinotFSConfigFile;

  @Option(name = "-baseDirectoryUri", required = true, metaVar = "<String>",
      usage = "Temp directory path for running benchmark against. e.g. file:///path/to/test, abfss://host/path...")
  private String _baseDirectoryUri;

  @Option(name = "-localTempDir", required = false, metaVar = "<String>", usage = "Local temp directory for benchmark.")
  private String _localTempDir;

  @Option(name = "-numSegmentsForListTest", required = false, metaVar = "<Integer>",
      usage = "The number of segments to create before running listFiles test.")
  private Integer _numSegmentsForListTest;

  @Option(name = "-dataSizeInMBsForCopyTest", required = false, metaVar = "<Integer>",
      usage = "Data size in MB for copy test. (e.g. 1024 = 1GB)")
  private Integer _dataSizeInMBsForCopyTest;

  @Option(name = "-numOps", required = false, metaVar = "<Integer>",
      usage = "The number of trials of operations when running a benchmark.")
  private Integer _numOps;

  @Option(name = "-help", required = false, help = true, aliases = {"-h", "--h", "--help"},
      usage = "Print this message.")
  private boolean _help = false;

  @Override
  public boolean execute() throws Exception {
    try {
      LOGGER.info("Run filesystem benchmark...");
      PinotFSBenchmarkDriver driver = new PinotFSBenchmarkDriver(_mode, _pinotFSConfigFile, _baseDirectoryUri,
          _localTempDir, _numSegmentsForListTest, _dataSizeInMBsForCopyTest, _numOps);
      driver.run();
    } catch (Exception e) {
      LOGGER.error("Error while running benchmark: ", e);
    }
    return true;
  }

  @Override
  public String description() {
    return "Run Filesystem benchmark";
  }

  @Override
  public boolean getHelp() {
    return _help;
  }
}
