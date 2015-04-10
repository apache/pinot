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

import org.apache.log4j.BasicConfigurator;
import org.kohsuke.args4j.Argument;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.spi.SubCommand;
import org.kohsuke.args4j.spi.SubCommandHandler;
import org.kohsuke.args4j.spi.SubCommands;

import com.linkedin.pinot.tools.admin.command.Command;
import com.linkedin.pinot.tools.admin.command.CreateSegmentCommand;
import com.linkedin.pinot.tools.admin.command.GenerateDataCommand;
import com.linkedin.pinot.tools.admin.command.PostQueryCommand;
import com.linkedin.pinot.tools.admin.command.StartBrokerCommand;
import com.linkedin.pinot.tools.admin.command.StartControllerCommand;
import com.linkedin.pinot.tools.admin.command.StartServerCommand;
import com.linkedin.pinot.tools.admin.command.UploadDataCommand;

/**
 * Class to implement Pinot Administrator, that provides the following commands:
 *
 * - GenerateData    -numRecords <int> -numFiles <int> -schemaFile <inputDataSchema> -outDir <outputDir>
 * - CreateSegment   -schemaFile <inputDataSchema> -dataDir <inputData> -resourceName <resourceName>
 *                   -tableName <tableName> -segmentName <segmentName> -outDir <outputDir>
 * - StartServer     -cfgFile <configFile> -clusterName <name> -zooKeeperUrl <url> -dataDir <dir> -segmentDir <dir>
 * - StartBroker     -cfgFile <configFile> -clusterName <name> -zooKeeperUrl <url>
 * - StartController -cfgFile <configFile> -clusterName <name> -zooKeeperUrl <url> -controllerPort -dataDir <dir>
 *
 * @author Mayank Shrivastava <mshrivastava@linkedin.com>
 */
public class PinotAdministrator {
  @Argument(handler = SubCommandHandler.class)
  @SubCommands({
    @SubCommand(name = "GenerateDataCommand",    impl = GenerateDataCommand.class),
    @SubCommand(name = "CreateSegmentCommand",   impl = CreateSegmentCommand.class),
    @SubCommand(name = "StartServerCommand",     impl = StartServerCommand.class),
    @SubCommand(name = "StartBrokerCommand",     impl = StartBrokerCommand.class),
    @SubCommand(name = "StartControllerCommand", impl = StartControllerCommand.class),
    @SubCommand(name = "UploadDataCommand",      impl = UploadDataCommand.class),
    @SubCommand(name = "PostQueryCommand",       impl = PostQueryCommand.class)
  })
  Command cmd;

  public void execute(String[] args) throws Exception {
    CmdLineParser parser = new CmdLineParser(this);

    try {
      parser.parseArgument(args);
      System.out.println("Executing command: " + cmd);
      cmd.execute();
    } catch (CmdLineException e) {
      System.out.println(e.getMessage());
    }
  }

  public static void main(String[] args) throws Exception {
    // Configure the logger.
    BasicConfigurator.configure();

    new PinotAdministrator().execute(args);
  }
}
