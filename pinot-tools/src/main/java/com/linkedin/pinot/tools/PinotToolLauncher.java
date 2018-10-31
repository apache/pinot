/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.tools;

import com.linkedin.pinot.tools.perf.PerfBenchmarkRunner;
import com.linkedin.pinot.tools.perf.QueryRunner;
import java.lang.reflect.Field;
import org.kohsuke.args4j.Argument;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.kohsuke.args4j.spi.SubCommand;
import org.kohsuke.args4j.spi.SubCommandHandler;
import org.kohsuke.args4j.spi.SubCommands;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class PinotToolLauncher {
  private static final Logger LOGGER = LoggerFactory.getLogger(PinotToolLauncher.class);

  // @formatter:off
  @Argument(handler = SubCommandHandler.class, metaVar = "<subCommand>")
  @SubCommands({
      @SubCommand(name = "UpdateSegmentState", impl = UpdateSegmentState.class),
      @SubCommand(name = "AutoAddInvertedIndex", impl = AutoAddInvertedIndexTool.class),
      @SubCommand(name = "ValidateTableRetention", impl = ValidateTableRetention.class),
      @SubCommand(name = "PerfBenchmarkRunner", impl = PerfBenchmarkRunner.class),
      @SubCommand(name = "QueryRunner", impl = QueryRunner.class)
  })
  Command _subCommand;
  // @formatter:on

  @Option(name = "-help", required = false, help = true, aliases={"-h", "--h", "--help"}, usage="Print this message.")
  boolean _help = false;

  public void execute(String[] args) throws Exception {
    try {
      CmdLineParser parser = new CmdLineParser(this);
      parser.parseArgument(args);

      if ((_subCommand == null) || _help) {
        printUsage();
      } else if (_subCommand.getHelp()) {
        _subCommand.printUsage();
      } else {
        _subCommand.execute();
      }

    } catch (CmdLineException e) {
      LOGGER.error("Error: {}", e.getMessage());
    } catch (Exception e) {
      LOGGER.error("Exception caught: ", e);
    }
  }

  public static void main(String[] args) throws Exception {
    new PinotToolLauncher().execute(args);
  }

  public void printUsage() {
    LOGGER.info("Usage: pinot-tools.sh <subCommand>");
    LOGGER.info("Valid subCommands are:");

    Class<PinotToolLauncher> obj = PinotToolLauncher.class;

    for (Field f : obj.getDeclaredFields()) {
      if (f.isAnnotationPresent(SubCommands.class)) {
        SubCommands subCommands = f.getAnnotation(SubCommands.class);

        for (SubCommand subCommand : subCommands.value()) {
          Class<?> subCommandClass = subCommand.impl();
          Command command = null;

          try {
            command = (Command) subCommandClass.newInstance();
            LOGGER.info("\t" + subCommand.name() + "\t<" + command.description() + ">");
          } catch (Exception e) {
            LOGGER.info("Internal Error: Error instantiating class.");
          }
        }
      }
    }
  }
}
