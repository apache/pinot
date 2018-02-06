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
package com.linkedin.pinot.tools.admin;

import com.linkedin.pinot.tools.admin.command.MoveReplicaGroup;

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

import com.linkedin.pinot.tools.Command;
import com.linkedin.pinot.tools.admin.command.AddSchemaCommand;
import com.linkedin.pinot.tools.admin.command.AddTableCommand;
import com.linkedin.pinot.tools.admin.command.AddTenantCommand;
import com.linkedin.pinot.tools.admin.command.AvroSchemaToPinotSchema;
import com.linkedin.pinot.tools.admin.command.BackfillDateTimeColumnCommand;
import com.linkedin.pinot.tools.admin.command.ChangeNumReplicasCommand;
import com.linkedin.pinot.tools.admin.command.ChangeTableState;
import com.linkedin.pinot.tools.admin.command.CreateSegmentCommand;
import com.linkedin.pinot.tools.admin.command.DeleteClusterCommand;
import com.linkedin.pinot.tools.admin.command.GenerateDataCommand;
import com.linkedin.pinot.tools.admin.command.PostQueryCommand;
import com.linkedin.pinot.tools.admin.command.RebalanceTableCommand;
import com.linkedin.pinot.tools.admin.command.ShowClusterInfoCommand;
import com.linkedin.pinot.tools.admin.command.StartBrokerCommand;
import com.linkedin.pinot.tools.admin.command.StartControllerCommand;
import com.linkedin.pinot.tools.admin.command.StartKafkaCommand;
import com.linkedin.pinot.tools.admin.command.StartServerCommand;
import com.linkedin.pinot.tools.admin.command.StartZookeeperCommand;
import com.linkedin.pinot.tools.admin.command.StopProcessCommand;
import com.linkedin.pinot.tools.admin.command.StreamAvroIntoKafkaCommand;
import com.linkedin.pinot.tools.admin.command.UploadSegmentCommand;
import com.linkedin.pinot.tools.admin.command.ValidateConfigCommand;
import com.linkedin.pinot.tools.admin.command.VerifyClusterStateCommand;
import com.linkedin.pinot.tools.admin.command.VerifySegmentState;
import com.linkedin.pinot.tools.segment.converter.PinotSegmentConvertCommand;


/**
 * Class to implement Pinot Administrator, that provides the following commands:
 *
 */
public class PinotAdministrator {
  private static final Logger LOGGER = LoggerFactory.getLogger(PinotAdministrator.class);

  // @formatter:off
  @Argument(handler = SubCommandHandler.class, metaVar = "<subCommand>")
  @SubCommands({
      @SubCommand(name = "GenerateData", impl = GenerateDataCommand.class),
      @SubCommand(name = "CreateSegment", impl = CreateSegmentCommand.class),
      @SubCommand(name = "StartZookeeper", impl = StartZookeeperCommand.class),
      @SubCommand(name = "StartKafka", impl = StartKafkaCommand.class),
      @SubCommand(name = "StreamAvroIntoKafka", impl = StreamAvroIntoKafkaCommand.class),
      @SubCommand(name = "StartController", impl = StartControllerCommand.class),
      @SubCommand(name = "StartBroker", impl = StartBrokerCommand.class),
      @SubCommand(name = "StartServer", impl = StartServerCommand.class),
      @SubCommand(name = "AddTable", impl = AddTableCommand.class),
      @SubCommand(name = "ChangeTableState", impl = ChangeTableState.class),
      @SubCommand(name = "AddTenant", impl = AddTenantCommand.class),
      @SubCommand(name = "AddSchema", impl = AddSchemaCommand.class),
      @SubCommand(name = "UploadSegment", impl = UploadSegmentCommand.class),
      @SubCommand(name = "PostQuery", impl = PostQueryCommand.class),
      @SubCommand(name = "StopProcess", impl = StopProcessCommand.class),
      @SubCommand(name = "DeleteCluster", impl = DeleteClusterCommand.class),
      @SubCommand(name = "ShowClusterInfo", impl = ShowClusterInfoCommand.class),
      @SubCommand(name = "AvroSchemaToPinotSchema", impl = AvroSchemaToPinotSchema.class),
      @SubCommand(name = "RebalanceTable", impl = RebalanceTableCommand.class),
      @SubCommand(name = "ChangeNumReplicas", impl = ChangeNumReplicasCommand.class),
      @SubCommand(name = "ValidateConfig", impl = ValidateConfigCommand.class),
      @SubCommand(name = "VerifySegmentState", impl = VerifySegmentState.class),
      @SubCommand(name = "ConvertPinotSegment", impl = PinotSegmentConvertCommand.class),
      @SubCommand(name = "MoveReplicaGroup", impl = MoveReplicaGroup.class),
      @SubCommand(name = "BackfillSegmentColumn", impl = BackfillDateTimeColumnCommand.class),
      @SubCommand(name = "VerifyClusterState", impl = VerifyClusterStateCommand.class)
  })
  Command _subCommand;
  // @formatter:on

  @Option(name = "-help", required = false, help = true, aliases = {"-h", "--h", "--help"},
      usage = "Print this message.")
  boolean _help = false;
  boolean _status = false;

  private boolean getStatus() {
    return _status;
  }

  public void execute(String[] args) throws Exception {
    try {
      CmdLineParser parser = new CmdLineParser(this);
      parser.parseArgument(args);

      if ((_subCommand == null) || _help) {
        printUsage();
      } else if (_subCommand.getHelp()) {
        _subCommand.printUsage();
        _status = true;
      } else {
        _status = _subCommand.execute();
      }

    } catch (CmdLineException e) {
      LOGGER.error("Error: {}", e.getMessage());
    } catch (Exception e) {
      LOGGER.error("Exception caught: ", e);
    }
  }

  public static void main(String[] args) throws Exception {
    PinotAdministrator pinotAdministrator = new PinotAdministrator();
    pinotAdministrator.execute(args);
  }

  public void printUsage() {
    LOGGER.info("Usage: pinot-admin.sh <subCommand>");
    LOGGER.info("Valid subCommands are:");

    Class<PinotAdministrator> obj = PinotAdministrator.class;

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
    LOGGER.info("For other crud operations, please refer to ${ControllerAddress}/help.");
  }
}
