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
import org.apache.pinot.spi.plugin.PluginManager;
import org.apache.pinot.tools.Command;
import org.apache.pinot.tools.admin.command.filesystem.CopyFiles;
import org.apache.pinot.tools.admin.command.filesystem.DeleteFiles;
import org.apache.pinot.tools.admin.command.filesystem.ListFiles;
import org.apache.pinot.tools.admin.command.filesystem.MoveFiles;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;


@CommandLine.Command(name = "FileSystem", subcommands = {
    ListFiles.class, CopyFiles.class, MoveFiles.class, DeleteFiles.class
}, description = "Pinot File system operation.", mixinStandardHelpOptions = true)
public class FileSystemCommand extends AbstractBaseAdminCommand implements Command {
  private static final Logger LOGGER = LoggerFactory.getLogger(FileSystemCommand.class.getName());

  @CommandLine.Option(names = {"-conf", "-config", "-configFile", "-config-file"}, scope =
      CommandLine.ScopeType.INHERIT, description = "PinotFS Config file.")
  private String _configFile;

  @CommandLine.Parameters
  private String[] _parameters;

  public FileSystemCommand setConfigFile(String configFile) {
    _configFile = configFile;
    return this;
  }

  public String getConfigFile() {
    return _configFile;
  }

  public FileSystemCommand setParameters(String[] parameters) {
    _parameters = parameters;
    return this;
  }

  @Override
  public String getName() {
    return "FileSystem";
  }

  @Override
  public String toString() {
    return "FileSystem -configFile " + _configFile;
  }

  @Override
  public void cleanup() {
  }

  @Override
  public boolean execute()
      throws Exception {
    // All FileSystem commands should go to SubCommand class.
    System.out.println("Unsupported subcommand: " + Arrays.toString(_parameters));
    return false;
  }

  public static void main(String[] args) {
    PluginManager.get().init();
    int exitCode = new CommandLine(new FileSystemCommand()).execute(args);
    if (exitCode != 0 && Boolean.parseBoolean(System.getProperties().getProperty("pinot.admin.system.exit", "true"))) {
      System.exit(exitCode);
    }
  }
}
