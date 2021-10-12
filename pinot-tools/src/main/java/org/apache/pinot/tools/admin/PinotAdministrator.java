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
package org.apache.pinot.tools.admin;

import java.lang.reflect.Field;
import java.util.Map;
import org.apache.pinot.common.Utils;
import org.apache.pinot.spi.plugin.PluginManager;
import org.apache.pinot.tools.Command;
import org.apache.pinot.tools.admin.command.AddSchemaCommand;
import org.apache.pinot.tools.admin.command.AddTableCommand;
import org.apache.pinot.tools.admin.command.AddTenantCommand;
import org.apache.pinot.tools.admin.command.AnonymizeDataCommand;
import org.apache.pinot.tools.admin.command.AvroSchemaToPinotSchema;
import org.apache.pinot.tools.admin.command.BootstrapTableCommand;
import org.apache.pinot.tools.admin.command.ChangeNumReplicasCommand;
import org.apache.pinot.tools.admin.command.ChangeTableState;
import org.apache.pinot.tools.admin.command.CreateSegmentCommand;
import org.apache.pinot.tools.admin.command.DeleteClusterCommand;
import org.apache.pinot.tools.admin.command.GenerateDataCommand;
import org.apache.pinot.tools.admin.command.GitHubEventsQuickStartCommand;
import org.apache.pinot.tools.admin.command.ImportDataCommand;
import org.apache.pinot.tools.admin.command.JsonToPinotSchema;
import org.apache.pinot.tools.admin.command.LaunchDataIngestionJobCommand;
import org.apache.pinot.tools.admin.command.MoveReplicaGroup;
import org.apache.pinot.tools.admin.command.OfflineSegmentIntervalCheckerCommand;
import org.apache.pinot.tools.admin.command.OperateClusterConfigCommand;
import org.apache.pinot.tools.admin.command.PostQueryCommand;
import org.apache.pinot.tools.admin.command.QuickStartCommand;
import org.apache.pinot.tools.admin.command.RealtimeProvisioningHelperCommand;
import org.apache.pinot.tools.admin.command.RebalanceTableCommand;
import org.apache.pinot.tools.admin.command.SegmentProcessorFrameworkCommand;
import org.apache.pinot.tools.admin.command.ShowClusterInfoCommand;
import org.apache.pinot.tools.admin.command.StartBrokerCommand;
import org.apache.pinot.tools.admin.command.StartControllerCommand;
import org.apache.pinot.tools.admin.command.StartKafkaCommand;
import org.apache.pinot.tools.admin.command.StartMinionCommand;
import org.apache.pinot.tools.admin.command.StartServerCommand;
import org.apache.pinot.tools.admin.command.StartServiceManagerCommand;
import org.apache.pinot.tools.admin.command.StartZookeeperCommand;
import org.apache.pinot.tools.admin.command.StopProcessCommand;
import org.apache.pinot.tools.admin.command.StreamAvroIntoKafkaCommand;
import org.apache.pinot.tools.admin.command.StreamGitHubEventsCommand;
import org.apache.pinot.tools.admin.command.UploadSegmentCommand;
import org.apache.pinot.tools.admin.command.ValidateConfigCommand;
import org.apache.pinot.tools.admin.command.VerifyClusterStateCommand;
import org.apache.pinot.tools.admin.command.VerifySegmentState;
import org.apache.pinot.tools.segment.converter.PinotSegmentConvertCommand;
import org.apache.pinot.tools.segment.converter.SegmentMergeCommand;
import org.kohsuke.args4j.Argument;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.kohsuke.args4j.spi.SubCommand;
import org.kohsuke.args4j.spi.SubCommandHandler;
import org.kohsuke.args4j.spi.SubCommands;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Class to implement Pinot Administrator, that provides the following commands:
 *
 * System property: `pinot.admin.system.exit`(default to false) is used to decide if System.exit(...) will be called
 * with exit code.
 *
 * Sample Usage in Commandline:
 *  JAVA_OPTS="-Xms4G -Xmx4G -Dpinot.admin.system.exit=true" \
 *  bin/pinot-admin.sh AddTable \
 *    -schemaFile /my/path/to/table/schema.json \
 *    -tableConfigFile /my/path/to/table/tableConfig.json \
 *    -controllerHost localhost \
 *    -controllerPort 9000 \
 *    -exec
 *
 */
public class PinotAdministrator {
  private static final Logger LOGGER = LoggerFactory.getLogger(PinotAdministrator.class);

  //@formatter:off
  @Argument(handler = SubCommandHandler.class, metaVar = "<subCommand>")
  @SubCommands({
      @SubCommand(name = "QuickStart", impl = QuickStartCommand.class),
      @SubCommand(name = "OperateClusterConfig", impl = OperateClusterConfigCommand.class),
      @SubCommand(name = "GenerateData", impl = GenerateDataCommand.class),
      @SubCommand(name = "LaunchDataIngestionJob", impl = LaunchDataIngestionJobCommand.class),
      @SubCommand(name = "CreateSegment", impl = CreateSegmentCommand.class),
      @SubCommand(name = "ImportData", impl = ImportDataCommand.class),
      @SubCommand(name = "StartZookeeper", impl = StartZookeeperCommand.class),
      @SubCommand(name = "StartKafka", impl = StartKafkaCommand.class),
      @SubCommand(name = "StreamAvroIntoKafka", impl = StreamAvroIntoKafkaCommand.class),
      @SubCommand(name = "StartController", impl = StartControllerCommand.class),
      @SubCommand(name = "StartBroker", impl = StartBrokerCommand.class),
      @SubCommand(name = "StartServer", impl = StartServerCommand.class),
      @SubCommand(name = "StartMinion", impl = StartMinionCommand.class),
      @SubCommand(name = "StartServiceManager", impl = StartServiceManagerCommand.class),
      @SubCommand(name = "AddTable", impl = AddTableCommand.class),
      @SubCommand(name = "ChangeTableState", impl = ChangeTableState.class),
      @SubCommand(name = "AddTenant", impl = AddTenantCommand.class),
      @SubCommand(name = "AddSchema", impl = AddSchemaCommand.class),
      @SubCommand(name = "UpdateSchema", impl = AddSchemaCommand.class),
      @SubCommand(name = "UploadSegment", impl = UploadSegmentCommand.class),
      @SubCommand(name = "PostQuery", impl = PostQueryCommand.class),
      @SubCommand(name = "StopProcess", impl = StopProcessCommand.class),
      @SubCommand(name = "DeleteCluster", impl = DeleteClusterCommand.class),
      @SubCommand(name = "ShowClusterInfo", impl = ShowClusterInfoCommand.class),
      @SubCommand(name = "AvroSchemaToPinotSchema", impl = AvroSchemaToPinotSchema.class),
      @SubCommand(name = "JsonToPinotSchema", impl = JsonToPinotSchema.class),
      @SubCommand(name = "RebalanceTable", impl = RebalanceTableCommand.class),
      @SubCommand(name = "ChangeNumReplicas", impl = ChangeNumReplicasCommand.class),
      @SubCommand(name = "ValidateConfig", impl = ValidateConfigCommand.class),
      @SubCommand(name = "VerifySegmentState", impl = VerifySegmentState.class),
      @SubCommand(name = "ConvertPinotSegment", impl = PinotSegmentConvertCommand.class),
      @SubCommand(name = "MoveReplicaGroup", impl = MoveReplicaGroup.class),
      @SubCommand(name = "VerifyClusterState", impl = VerifyClusterStateCommand.class),
      @SubCommand(name = "RealtimeProvisioningHelper", impl = RealtimeProvisioningHelperCommand.class),
      @SubCommand(name = "MergeSegments", impl = SegmentMergeCommand.class),
      @SubCommand(name = "CheckOfflineSegmentIntervals", impl = OfflineSegmentIntervalCheckerCommand.class),
      @SubCommand(name = "AnonymizeData", impl = AnonymizeDataCommand.class),
      @SubCommand(name = "GitHubEventsQuickStart", impl = GitHubEventsQuickStartCommand.class),
      @SubCommand(name = "StreamGitHubEvents", impl = StreamGitHubEventsCommand.class),
      @SubCommand(name = "BootstrapTable", impl = BootstrapTableCommand.class),
      @SubCommand(name = "SegmentProcessorFramework", impl = SegmentProcessorFrameworkCommand.class)
  })
  Command _subCommand;
  //@formatter:on

  @Option(name = "-help", required = false, help = true, aliases = {"-h", "--h", "--help"},
      usage = "Print this message.")
  boolean _help = false;

  @Option(name = "-version", required = false, help = true, aliases = {"-v", "--v", "--version"},
      usage = "Print the version of Pinot package.")
  boolean _version = false;
  boolean _status = false;

  private boolean getStatus() {
    return _status;
  }

  public void execute(String[] args) {
    try {
      CmdLineParser parser = new CmdLineParser(this);
      parser.parseArgument(args);
      if (_version) {
        printVersion();
        _status = true;
      } else if ((_subCommand == null) || _help) {
        printUsage();
        _status = true;
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

  private void printVersion() {
    LOGGER.info("List All Pinot Component Versions:");
    Map<String, String> componentVersions = Utils.getComponentVersions();
    for (Map.Entry<String, String> entry : componentVersions.entrySet()) {
      LOGGER.info("Package: {}, Version: {}", entry.getKey(), entry.getValue());
    }
  }

  public static void main(String[] args) {
    PluginManager.get().init();
    PinotAdministrator pinotAdministrator = new PinotAdministrator();
    pinotAdministrator.execute(args);
    if (System.getProperties().getProperty("pinot.admin.system.exit", "false").equalsIgnoreCase("true")) {
      // If status is true, cmd was successfully, so return 0 from process.
      System.exit(pinotAdministrator.getStatus() ? 0 : 1);
    }
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
