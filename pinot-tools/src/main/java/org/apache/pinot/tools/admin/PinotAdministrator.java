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

import java.io.PrintWriter;
import java.io.Writer;
import java.util.HashMap;
import java.util.Map;
import org.apache.pinot.common.Versions;
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
import org.apache.pinot.tools.admin.command.DataImportDryRunCommand;
import org.apache.pinot.tools.admin.command.DeleteClusterCommand;
import org.apache.pinot.tools.admin.command.DeleteSchemaCommand;
import org.apache.pinot.tools.admin.command.DeleteTableCommand;
import org.apache.pinot.tools.admin.command.FileSystemCommand;
import org.apache.pinot.tools.admin.command.GenerateDataCommand;
import org.apache.pinot.tools.admin.command.GitHubEventsQuickStartCommand;
import org.apache.pinot.tools.admin.command.ImportDataCommand;
import org.apache.pinot.tools.admin.command.JsonToPinotSchema;
import org.apache.pinot.tools.admin.command.LaunchDataIngestionJobCommand;
import org.apache.pinot.tools.admin.command.LaunchSparkDataIngestionJobCommand;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;


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
  private static final Map<String, Command> SUBCOMMAND_MAP = new HashMap<>();

  static {
    SUBCOMMAND_MAP.put("QuickStart", new QuickStartCommand());
    SUBCOMMAND_MAP.put("OperateClusterConfig", new OperateClusterConfigCommand());
    SUBCOMMAND_MAP.put("GenerateData", new GenerateDataCommand());
    SUBCOMMAND_MAP.put("LaunchDataIngestionJob", new LaunchDataIngestionJobCommand());
    SUBCOMMAND_MAP.put("LaunchSparkDataIngestionJob", new LaunchSparkDataIngestionJobCommand());
    SUBCOMMAND_MAP.put("CreateSegment", new CreateSegmentCommand());
    SUBCOMMAND_MAP.put("ImportData", new ImportDataCommand());
    SUBCOMMAND_MAP.put("StartZookeeper", new StartZookeeperCommand());
    SUBCOMMAND_MAP.put("StartKafka", new StartKafkaCommand());
    SUBCOMMAND_MAP.put("StreamAvroIntoKafka", new StreamAvroIntoKafkaCommand());
    SUBCOMMAND_MAP.put("StartController", new StartControllerCommand());
    SUBCOMMAND_MAP.put("StartBroker", new StartBrokerCommand());
    SUBCOMMAND_MAP.put("StartServer", new StartServerCommand());
    SUBCOMMAND_MAP.put("StartMinion", new StartMinionCommand());
    SUBCOMMAND_MAP.put("StartServiceManager", new StartServiceManagerCommand());
    SUBCOMMAND_MAP.put("AddTable", new AddTableCommand());
    SUBCOMMAND_MAP.put("DeleteTable", new DeleteTableCommand());
    SUBCOMMAND_MAP.put("ChangeTableState", new ChangeTableState());
    SUBCOMMAND_MAP.put("AddTenant", new AddTenantCommand());
    SUBCOMMAND_MAP.put("AddSchema", new AddSchemaCommand());
    SUBCOMMAND_MAP.put("DeleteSchema", new DeleteSchemaCommand());
    SUBCOMMAND_MAP.put("DataImportDryRun", new DataImportDryRunCommand());
    SUBCOMMAND_MAP.put("UpdateSchema", new AddSchemaCommand());
    SUBCOMMAND_MAP.put("UploadSegment", new UploadSegmentCommand());
    SUBCOMMAND_MAP.put("PostQuery", new PostQueryCommand());
    SUBCOMMAND_MAP.put("StopProcess", new StopProcessCommand());
    SUBCOMMAND_MAP.put("DeleteCluster", new DeleteClusterCommand());
    SUBCOMMAND_MAP.put("ShowClusterInfo", new ShowClusterInfoCommand());
    SUBCOMMAND_MAP.put("AvroSchemaToPinotSchema", new AvroSchemaToPinotSchema());
    SUBCOMMAND_MAP.put("JsonToPinotSchema", new JsonToPinotSchema());
    SUBCOMMAND_MAP.put("RebalanceTable", new RebalanceTableCommand());
    SUBCOMMAND_MAP.put("ChangeNumReplicas", new ChangeNumReplicasCommand());
    SUBCOMMAND_MAP.put("ValidateConfig", new ValidateConfigCommand());
    SUBCOMMAND_MAP.put("VerifySegmentState", new VerifySegmentState());
    SUBCOMMAND_MAP.put("ConvertPinotSegment", new PinotSegmentConvertCommand());
    SUBCOMMAND_MAP.put("MoveReplicaGroup", new MoveReplicaGroup());
    SUBCOMMAND_MAP.put("VerifyClusterState", new VerifyClusterStateCommand());
    SUBCOMMAND_MAP.put("RealtimeProvisioningHelper", new RealtimeProvisioningHelperCommand());
    SUBCOMMAND_MAP.put("CheckOfflineSegmentIntervals", new OfflineSegmentIntervalCheckerCommand());
    SUBCOMMAND_MAP.put("AnonymizeData", new AnonymizeDataCommand());
    SUBCOMMAND_MAP.put("GitHubEventsQuickStart", new GitHubEventsQuickStartCommand());
    SUBCOMMAND_MAP.put("StreamGitHubEvents", new StreamGitHubEventsCommand());
    SUBCOMMAND_MAP.put("BootstrapTable", new BootstrapTableCommand());
    SUBCOMMAND_MAP.put("SegmentProcessorFramework", new SegmentProcessorFrameworkCommand());
    SUBCOMMAND_MAP.put("FileSystem", new FileSystemCommand());
  }

  @CommandLine.Option(names = {"-help", "-h", "--h", "--help"}, required = false, description = "Print this message.")
  private boolean _help = false;

  @CommandLine.Option(names = {"-version", "-v", "--v", "--version"}, required = false, description = "Print the "
                                                                                                      + "version of "
                                                                                                      + "Pinot "
                                                                                                      + "package.")
  private boolean _version = false;

  private int _status = 1;

  private final Writer _outWriter;

  private final Versions _versions;

  public PinotAdministrator(Writer outWriter, Versions versions) {
    _outWriter = outWriter;
    _versions = versions;
  }

  public void execute(String[] args) {
    try {
      CommandLine commandLine = new CommandLine(this);
      this.getSubCommands().forEach(commandLine::addSubcommand);
      commandLine.setOut(new PrintWriter(_outWriter));
      CommandLine.ParseResult parseResult = commandLine.parseArgs(args);
      if (parseResult.hasSubcommand()) {
        _status = commandLine.execute(args);
      } else {
        handleVersionOrHelp(commandLine);
      }
    } catch (Exception e) {
      LOGGER.error("Exception caught: ", e);
    }
  }

  private void handleVersionOrHelp(CommandLine commandLine) {
    if (_version) {
      printVersion(commandLine.getOut());
      _status = 0;
    } else if (_help) {
      printUsage(commandLine.getOut());
      _status = 0;
    } else {
      _status = -1;
    }
  }

  private void printVersion(PrintWriter printWriter) {
    printWriter.println("List All Pinot Component Versions:");
    Map<String, String> componentVersions = _versions.getComponentVersions();
    for (Map.Entry<String, String> entry : componentVersions.entrySet()) {
      printWriter.println(String.format("Package: %s, Version: %s", entry.getKey(), entry.getValue()));
    }
  }

  public void printUsage(PrintWriter printWriter) {
    printWriter.println("Usage: pinot-admin.sh <subCommand>");
    printWriter.println("Valid subCommands are:");
    int maxCommandLength = this.getSubCommands().keySet().stream().mapToInt(String::length).max()
        .orElseThrow(() -> new IllegalArgumentException("Unable to print usage, missing SubCommand"));
    for (Map.Entry<String, Command> subCommand : this.getSubCommands().entrySet()) {
      printWriter.println(String.format("\t%" + maxCommandLength + "s\t:" + subCommand.getValue().getDescription(),
          subCommand.getKey()));
    }
    printWriter.println("For other crud operations, please refer to ${ControllerAddress}/help.");
  }

  public Map<String, Command> getSubCommands() {
    return SUBCOMMAND_MAP;
  }

  public static void main(String[] args) {
    PluginManager.get().init();
    PinotAdministrator pinotAdministrator = new PinotAdministrator(new PrintWriter(System.out, true), new Versions());
    pinotAdministrator.execute(args);
    if ((pinotAdministrator._status != 0) && Boolean.parseBoolean(
        System.getProperties().getProperty("pinot.admin.system.exit"))) {
      System.exit(pinotAdministrator._status);
    }
  }
}
