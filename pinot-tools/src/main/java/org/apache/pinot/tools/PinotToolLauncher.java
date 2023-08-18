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
package org.apache.pinot.tools;

import java.util.HashMap;
import java.util.Map;
import org.apache.pinot.spi.plugin.PluginManager;
import org.apache.pinot.tools.filesystem.PinotFSBenchmarkRunner;
import org.apache.pinot.tools.perf.PerfBenchmarkRunner;
import org.apache.pinot.tools.perf.QueryRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;


public class PinotToolLauncher {
  private static final Logger LOGGER = LoggerFactory.getLogger(PinotToolLauncher.class);
  private static final Map<String, Command> SUBCOMMAND_MAP = new HashMap<>();

  static {
    SUBCOMMAND_MAP.put("UpdateSegmentState", new UpdateSegmentState());
    SUBCOMMAND_MAP.put("AutoAddInvertedIndex", new AutoAddInvertedIndexTool());
    SUBCOMMAND_MAP.put("ValidateTableRetention", new ValidateTableRetention());
    SUBCOMMAND_MAP.put("PerfBenchmarkRunner", new PerfBenchmarkRunner());
    SUBCOMMAND_MAP.put("QueryRunner", new QueryRunner());
    SUBCOMMAND_MAP.put("PinotFSBenchmarkRunner", new PinotFSBenchmarkRunner());
    SUBCOMMAND_MAP.put("SegmentDump", new SegmentDumpTool());
  }

  @CommandLine.Option(names = {"-help", "-h", "--h", "--help"}, required = false, usageHelp = true, description =
      "Print this message.")
  boolean _help = false;

  public void execute(String[] args)
      throws Exception {
    try {
      picocli.CommandLine commandLine = new picocli.CommandLine(this);
      for (Map.Entry<String, Command> subCommand : this.getSubCommands().entrySet()) {
        commandLine.addSubcommand(subCommand.getKey(), subCommand.getValue());
      }
      commandLine.execute(args);
    } catch (Exception e) {
      LOGGER.error("Exception caught: ", e);
    }
  }

  public Map<String, Command> getSubCommands() {
    return SUBCOMMAND_MAP;
  }

  public static void main(String[] args)
      throws Exception {
    PluginManager.get().init();
    PinotToolLauncher pinotToolLauncher = new PinotToolLauncher();
    pinotToolLauncher.execute(args);
  }
}
