/**
 * Copyright (C) 2014-2015 LinkedIn Corp. (pinot-core@linkedin.com)
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
package com.linkedin.pinot.tools.admin;

import java.lang.reflect.Field;

import org.kohsuke.args4j.Argument;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.kohsuke.args4j.spi.SubCommand;
import org.kohsuke.args4j.spi.SubCommandHandler;
import org.kohsuke.args4j.spi.SubCommands;

import com.linkedin.pinot.tools.admin.command.Command;
import com.linkedin.pinot.tools.admin.command.CreateResourceCommand;
import com.linkedin.pinot.tools.admin.command.CreateSegmentCommand;
import com.linkedin.pinot.tools.admin.command.GenerateDataCommand;
import com.linkedin.pinot.tools.admin.command.PostQueryCommand;
import com.linkedin.pinot.tools.admin.command.StartBrokerCommand;
import com.linkedin.pinot.tools.admin.command.StartControllerCommand;
import com.linkedin.pinot.tools.admin.command.StartServerCommand;
import com.linkedin.pinot.tools.admin.command.StartZookeeperCommand;
import com.linkedin.pinot.tools.admin.command.StopProcessCommand;
import com.linkedin.pinot.tools.admin.command.UploadDataCommand;


/**
 * Class to implement Pinot Administrator, that provides the following commands:
 *
 * @author Mayank Shrivastava <mshrivastava@linkedin.com>
 */
public class PinotAdministrator {
  // @formatter:off
  @Argument(handler = SubCommandHandler.class, metaVar = "<subCommand>")
  @SubCommands({
      @SubCommand(name = "GenerateData", impl = GenerateDataCommand.class),
      @SubCommand(name = "CreateSegment", impl = CreateSegmentCommand.class),
      @SubCommand(name = "CreateResource", impl = CreateResourceCommand.class),
      @SubCommand(name = "StartServer", impl = StartServerCommand.class),
      @SubCommand(name = "StartBroker", impl = StartBrokerCommand.class),
      @SubCommand(name = "StartController", impl = StartControllerCommand.class),
      @SubCommand(name = "UploadData", impl = UploadDataCommand.class),
      @SubCommand(name = "PostQuery", impl = PostQueryCommand.class),
      @SubCommand(name = "StartZookeeper", impl = StartZookeeperCommand.class),
      @SubCommand(name = "StopProcess", impl = StopProcessCommand.class)
  })
  Command _subCommand;
  // @formatter:on

  @Option(name = "-help", required = false, help = true)
  boolean help = false;

  public void execute(String[] args) throws Exception {
    try {
      CmdLineParser parser = new CmdLineParser(this);
      parser.parseArgument(args);

      if ((_subCommand == null) || help) {
        printUsage();
      } else if (_subCommand.getHelp()) {
        _subCommand.printUsage();
      } else {
        System.out.println("Executing command: " + _subCommand);
        _subCommand.execute();
      }

    } catch (Exception e) {
      System.err.println(e.getMessage());
    }
  }

  public static void main(String[] args) throws Exception {
    new PinotAdministrator().execute(args);
  }

  public void printUsage() {
    System.out.println("Usage: pinot-admin.sh <subCommand>");
    System.out.println("Valid subCommands are:");

    Class<PinotAdministrator> obj = PinotAdministrator.class;

    for (Field f : obj.getDeclaredFields()) {
      if (f.isAnnotationPresent(SubCommands.class)) {
        SubCommands subCommands = f.getAnnotation(SubCommands.class);

        for (SubCommand subCommand : subCommands.value()) {
          System.out.println("\t" + subCommand.name());
        }
      }
    }
  }
}
