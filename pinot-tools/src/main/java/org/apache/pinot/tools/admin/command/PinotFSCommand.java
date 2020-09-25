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

import org.apache.pinot.tools.Command;
import org.apache.pinot.tools.admin.command.fs.CopyCommand;
import org.apache.pinot.tools.admin.command.fs.CopyToLocalCommand;
import org.apache.pinot.tools.admin.command.fs.DeleteCommand;
import org.apache.pinot.tools.admin.command.fs.ExistsCommand;
import org.apache.pinot.tools.admin.command.fs.IsDirectoryCommand;
import org.apache.pinot.tools.admin.command.fs.LastModifiedCommand;
import org.apache.pinot.tools.admin.command.fs.LengthCommand;
import org.apache.pinot.tools.admin.command.fs.ListFilesCommand;
import org.apache.pinot.tools.admin.command.fs.MakeDirectoryCommand;
import org.apache.pinot.tools.admin.command.fs.MoveCommand;
import org.apache.pinot.tools.admin.command.fs.TouchCommand;
import org.kohsuke.args4j.Argument;
import org.kohsuke.args4j.Option;
import org.kohsuke.args4j.spi.SubCommand;
import org.kohsuke.args4j.spi.SubCommandHandler;
import org.kohsuke.args4j.spi.SubCommands;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;


/**
 * Class to implement PinotFS command.
 *
 */
public class PinotFSCommand extends AbstractBaseAdminCommand implements Command {
  private static final Logger LOGGER = LoggerFactory.getLogger(PinotFSCommand.class);

  //@formatter:off
  @Argument(handler = SubCommandHandler.class, metaVar = "<subCommand>")
  @SubCommands({
    @SubCommand(name = "copy", impl = CopyCommand.class),
    @SubCommand(name = "copyToLocal", impl = CopyToLocalCommand.class),
    @SubCommand(name = "delete", impl = DeleteCommand.class),
    @SubCommand(name = "exists", impl = ExistsCommand.class),
    @SubCommand(name = "isDirectoy", impl = IsDirectoryCommand.class),
    @SubCommand(name = "lastModified", impl = LastModifiedCommand.class),
    @SubCommand(name = "length", impl = LengthCommand.class),
    @SubCommand(name = "listFiles", impl = ListFilesCommand.class),
    @SubCommand(name = "makeDirectory", impl = MakeDirectoryCommand.class),
    @SubCommand(name = "move", impl = MoveCommand.class),
    @SubCommand(name = "touch", impl = TouchCommand.class),
  })
  Command _subCommand;
  //@formatter:on

  @Option(name = "-help", required = false, help = true, aliases = {"-h", "--h", "--help"}, usage = "Print this message.")
  private boolean _help = false;

  @Override
  public boolean getHelp() {
    return _help;
  }

  @Override
  public boolean execute()
          throws Exception {
    if ((_subCommand == null) || _help) {
      printUsage();
    } else if (_subCommand.getHelp()) {
      _subCommand.printUsage();
      return true;
    }
    return _subCommand.execute();
  }

  public void printUsage() {
    LOGGER.info("Usage: pinot-admin.sh PinotFS <subCommand>");
    LOGGER.info("Valid subCommands are:");

    Class<PinotFSCommand> obj = PinotFSCommand.class;

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

  @Override
  public String description() {
    return "Interact with a Pinot file system.";
  }

}
