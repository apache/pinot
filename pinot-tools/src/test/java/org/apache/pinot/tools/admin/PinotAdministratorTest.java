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

import java.io.StringWriter;
import java.util.Map;
import java.util.TreeMap;
import org.apache.pinot.common.Versions;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;


public class PinotAdministratorTest {

  @Test
  public void testHelpParameterShouldOutputAllPossibleParametersAndTheirDescriptions() {
    // given
    StringWriter out = new StringWriter();
    PinotAdministrator pinotAdministrator = new PinotAdministrator(out, new Versions());

    // when
    pinotAdministrator.execute(new String[]{"AddSchema", "--help"});

    // then
    assertEquals(out.toString(), "Usage: <main class> AddSchema [-hV] [-exec] [-authToken=<_authToken>]\n"
                                 + "                              [-authTokenUrl=<_authTokenUrl>]\n"
                                 + "                              [-controllerHost=<_controllerHost>]\n"
                                 + "                              [-controllerPort=<_controllerPort>]\n"
                                 + "                              [-controllerProtocol=<_controllerProtocol>]\n"
                                 + "                              [-password=<_password>] -schemaFile=<_schemaFil"
                                 + "e>\n" + "                              [-user=<_user>]\n"
                                 + "Add schema specified in the schema file to the controller\n"
                                 + "      -authToken=<_authToken>\n" + "                      Http auth token.\n"
                                 + "      -authTokenUrl=<_authTokenUrl>\n"
                                 + "                      Http auth token url.\n"
                                 + "      -controllerHost=<_controllerHost>\n"
                                 + "                      host name for controller.\n"
                                 + "      -controllerPort=<_controllerPort>\n"
                                 + "                      port name for controller.\n"
                                 + "      -controllerProtocol=<_controllerProtocol>\n"
                                 + "                      protocol for controller.\n"
                                 + "      -exec           Execute the command.\n"
                                 + "  -h, --help          Show this help message and exit.\n"
                                 + "      -password=<_password>\n" + "                      Password for basic auth.\n"
                                 + "      -schemaFile=<_schemaFile>\n" + "                      Path to schema file.\n"
                                 + "      -user=<_user>   Username for basic auth.\n"
                                 + "  -V, --version       Print version information and exit.\n");
  }

  @Test
  public void testHelp() {
    StringWriter out = new StringWriter();
    PinotAdministrator pinotAdministrator = new PinotAdministrator(out, new Versions());

    pinotAdministrator.execute(new String[]{"--help"});

    assertEquals(out.toString(), "Usage: pinot-admin.sh <subCommand>\n" + "Valid subCommands are:\n"
                                 + "\t                    AddTable\t:Create a Pinot table\n"
                                 + "\t            ChangeTableState\t:Change the state (enable|disable|drop) of Pinot "
                                 + "table\n" + "\t                DeleteSchema\t:Delete schema specified via name\n"
                                 + "\t                   AddSchema\t:Add schema specified in the schema file to the "
                                 + "controller\n"
                                 + "\t               AnonymizeData\t:Tool to anonymize a given Pinot table data while"
                                 + " preserving data characteristics and query patterns\n"
                                 + "\t               DeleteCluster\t:Remove the Pinot Cluster from Helix.\n"
                                 + "\t                 StartMinion\t:Start the Pinot Minion process at the specified "
                                 + "port\n"
                                 + "\t          VerifyClusterState\t:Verify cluster's state after shutting down "
                                 + "several nodes randomly.\n"
                                 + "\t      LaunchDataIngestionJob\t:Launch a data ingestion job.\n"
                                 + "\t                 StopProcess\t:Stop the specified processes.\n"
                                 + "\t             ShowClusterInfo\t:Show Pinot Cluster information.\n"
                                 + "\t          VerifySegmentState\t:Compares helix IdealState and ExternalView for "
                                 + "specified table prefixes\n"
                                 + "\t                  QuickStart\t:Launch a complete Pinot cluster within one "
                                 + "single process and import pre-built datasets.\n"
                                 + "\t          StreamGitHubEvents\t:Streams GitHubEvents into a Kafka topic or "
                                 + "Kinesis Stream\n"
                                 + "\t                   AddTenant\t:Add tenant as per the specification provided.\n"
                                 + "\t         StreamAvroIntoKafka\t:Stream the specified Avro file into a Kafka "
                                 + "topic, which can be read by Pinot\n"
                                 + "by using org.apache.pinot.plugin.stream.kafka.KafkaJSONMessageDecoder as the\n"
                                 + "message decoder class name (stream.kafka.decoder.class.name).\n"
                                 + "\t           JsonToPinotSchema\t:Extracting Pinot schema file from JSON data file"
                                 + ".\n" + "\t                  FileSystem\t:Pinot File system operation.\n"
                                 + "\t  RealtimeProvisioningHelper\t:Given the table config, partitions, retention "
                                 + "and a sample completed segment for a realtime table to be setup, this tool will "
                                 + "provide memory used by each host and an optimal segment size for various "
                                 + "combinations of hours to consume and hosts. Instead of a completed segment, if "
                                 + "schema with characteristics of data is provided, a segment will be generated and "
                                 + "used for memory estimation.\n"
                                 + "\t              RebalanceTable\t:Reassign instances and segments for a table.\n"
                                 + "\tCheckOfflineSegmentIntervals\t:Prints out offline segments with invalid time "
                                 + "intervals\n"
                                 + "\t               CreateSegment\t:Create pinot segments from the provided data "
                                 + "files.\n"
                                 + "\t     AvroSchemaToPinotSchema\t:Extracting Pinot schema file from Avro schema or"
                                 + " data file.\n"
                                 + "\t              ValidateConfig\t:Validate configs on the specified Zookeeper.\n"
                                 + "\t               UploadSegment\t:Upload the Pinot segments.\n"
                                 + "\t   SegmentProcessorFramework\t:Runs the SegmentProcessorFramework\n"
                                 + "\t            MoveReplicaGroup\t:Move complete set of segment replica from source"
                                 + " servers to tagged servers in cluster\n"
                                 + "\t LaunchSparkDataIngestionJob\t:Launch a data ingestion job.\n"
                                 + "\t                UpdateSchema\t:Add schema specified in the schema file to the "
                                 + "controller\n"
                                 + "\t             StartController\t:Start the Pinot Controller Process at the "
                                 + "specified port.\n" + "\t                 DeleteTable\t:Delete a Pinot table\n"
                                 + "\t                GenerateData\t:Generate random data as per the provided schema\n"
                                 + "\t                  StartKafka\t:Start Kafka at the specified port.\n"
                                 + "\t            DataImportDryRun\t:Dry run of data import\n"
                                 + "\t              BootstrapTable\t:Bootstrap Table.\n"
                                 + "\t                 StartBroker\t:Start the Pinot Broker process at the specified "
                                 + "port\n" + "\t                  ImportData\t:Insert data into Pinot cluster.\n"
                                 + "\t                   PostQuery\t:Query the uploaded Pinot segments.\n"
                                 + "\t      GitHubEventsQuickStart\t:Runs the GitHubEventsQuickstart\n"
                                 + "\t              StartZookeeper\t:Start the Zookeeper process at the specified "
                                 + "port.\n"
                                 + "\t         ConvertPinotSegment\t:Convert Pinot segments to another format such as"
                                 + " AVRO/CSV/JSON.\n"
                                 + "\t        OperateClusterConfig\t:Operate Pinot Cluster Config. Sample usage: "
                                 + "`pinot-admin.sh OperateClusterConfig -operation DELETE -config pinot.broker"
                                 + ".enable.query.limit.override`\n"
                                 + "\t                 StartServer\t:Start the Pinot Server process at the specified "
                                 + "port.\n"
                                 + "\t         StartServiceManager\t:Start the Pinot Service Process at the specified"
                                 + " port.\n"
                                 + "\t           ChangeNumReplicas\t:Re-writes idealState to reflect the value of "
                                 + "numReplicas in table config\n"
                                 + "For other crud operations, please refer to ${ControllerAddress}/help.\n");
  }

  @Test
  public void testVersion() {
    // given
    StringWriter out = new StringWriter();
    PinotAdministrator pinotAdministrator = new PinotAdministrator(out, new VersionsStub());

    // when
    pinotAdministrator.execute(new String[]{"--version"});

    // then
    assertEquals(out.toString(), "List All Pinot Component Versions:\n" + "Package: AddSchema, Version: 1.2.3\n"
                                 + "Package: AddTable, Version: 1.2.4\n");
  }

  static class VersionsStub extends Versions {
    @Override
    public Map<String, String> getComponentVersions() {
      return new TreeMap<>(Map.of("AddSchema", "1.2.3", "AddTable", "1.2.4"));
    }
  }
}
