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
import com.linkedin.pinot.tools.segment.converter.PinotSegmentConvertCommand;
import com.linkedin.pinot.tools.admin.command.*;

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
      @SubCommand(name = "CreateDataAndSegment", impl = SegmentCreationCommand.class),
      @SubCommand(name = "UploadSegmentPeriodically", impl = SegmentUploaderCommand.class),
      @SubCommand(name = "PinotBenchmarkGenerateEventTableData", impl = PinotBenchmarkEventTableCreationCommand.class),
      @SubCommand(name = "PinotBenchmarkGenerateEventTableSegment", impl = PinotBenchmarkSegmentCreationCommand.class),
      @SubCommand(name = "PinotBenchmarkGenerateQueries", impl= PinotBenchmarkQueryGeneratorCommand.class)
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
