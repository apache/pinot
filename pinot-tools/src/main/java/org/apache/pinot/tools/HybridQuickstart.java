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
package org.apache.pinot.tools;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.common.utils.ZkStarter;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.stream.StreamDataProvider;
import org.apache.pinot.spi.stream.StreamDataServerStartable;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.tools.Quickstart.Color;
import org.apache.pinot.tools.admin.PinotAdministrator;
import org.apache.pinot.tools.admin.command.QuickstartRunner;
import org.apache.pinot.tools.streams.AirlineDataStream;
import org.apache.pinot.tools.utils.KafkaStarterUtils;

import static org.apache.pinot.tools.Quickstart.prettyPrintResponse;
import static org.apache.pinot.tools.Quickstart.printStatus;


public class HybridQuickstart extends QuickStartBase {
  private StreamDataServerStartable _kafkaStarter;
  private ZkStarter.ZookeeperInstance _zookeeperInstance;
  private File _schemaFile;
  private File _realtimeTableConfigFile;
  private File _dataFile;
  private File _ingestionJobSpecFile;

  public static void main(String[] args)
      throws Exception {
    List<String> arguments = new ArrayList<>();
    arguments.addAll(Arrays.asList("QuickStart", "-type", "HYBRID"));
    arguments.addAll(Arrays.asList(args));
    PinotAdministrator.main(arguments.toArray(new String[arguments.size()]));
  }

  private QuickstartTableRequest prepareTableRequest(File baseDir)
      throws IOException {
    _schemaFile = new File(baseDir, "airlineStats_schema.json");
    _ingestionJobSpecFile = new File(baseDir, "ingestionJobSpec.yaml");
    File tableConfigFile = new File(baseDir, "airlineStats_offline_table_config.json");

    ClassLoader classLoader = Quickstart.class.getClassLoader();
    URL resource = classLoader.getResource("examples/batch/airlineStats/airlineStats_schema.json");
    Preconditions.checkNotNull(resource);
    FileUtils.copyURLToFile(resource, _schemaFile);
    resource = classLoader.getResource("examples/batch/airlineStats/ingestionJobSpec.yaml");
    Preconditions.checkNotNull(resource);
    FileUtils.copyURLToFile(resource, _ingestionJobSpecFile);
    resource = classLoader.getResource("examples/batch/airlineStats/airlineStats_offline_table_config.json");
    Preconditions.checkNotNull(resource);
    FileUtils.copyURLToFile(resource, tableConfigFile);

    _realtimeTableConfigFile = new File(baseDir, "airlineStats_realtime_table_config.json");
    resource = Quickstart.class.getClassLoader()
        .getResource("examples/stream/airlineStats/airlineStats_realtime_table_config.json");
    Preconditions.checkNotNull(resource);
    FileUtils.copyURLToFile(resource, _realtimeTableConfigFile);
    resource = Quickstart.class.getClassLoader()
        .getResource("examples/stream/airlineStats/sample_data/airlineStats_data.avro");
    Preconditions.checkNotNull(resource);
    _dataFile = new File(baseDir, "airlineStats_data.avro");
    FileUtils.copyURLToFile(resource, _dataFile);

    return new QuickstartTableRequest(baseDir.getAbsolutePath());
  }

  private void startKafka() {
    _zookeeperInstance = ZkStarter.startLocalZkServer();
    try {
      _kafkaStarter = StreamDataProvider.getServerDataStartable(KafkaStarterUtils.KAFKA_SERVER_STARTABLE_CLASS_NAME,
          KafkaStarterUtils.getDefaultKafkaConfiguration(_zookeeperInstance));
    } catch (Exception e) {
      throw new RuntimeException("Failed to start " + KafkaStarterUtils.KAFKA_SERVER_STARTABLE_CLASS_NAME, e);
    }
    _kafkaStarter.start();
    _kafkaStarter.createTopic("flights-realtime", KafkaStarterUtils.getTopicCreationProps(10));
  }

  public void execute()
      throws Exception {
    File quickstartTmpDir = new File(_tmpDir, String.valueOf(System.currentTimeMillis()));
    File baseDir = new File(quickstartTmpDir, "airlineStats");
    File dataDir = new File(baseDir, "data");
    Preconditions.checkState(dataDir.mkdirs());
    QuickstartTableRequest bootstrapTableRequest = prepareTableRequest(baseDir);
    final QuickstartRunner runner = new QuickstartRunner(Lists.newArrayList(bootstrapTableRequest), 1, 1, 1, dataDir);
    printStatus(Color.YELLOW, "***** Starting Kafka  *****");
    startKafka();
    printStatus(Color.YELLOW, "***** Starting airline data stream and publishing to Kafka *****");
    Schema schema = Schema.fromFile(_schemaFile);
    TableConfig tableConfig = JsonUtils.fileToObject(_realtimeTableConfigFile, TableConfig.class);
    AirlineDataStream stream = new AirlineDataStream(schema, tableConfig, _dataFile);
    stream.run();
    printStatus(Color.YELLOW, "***** Starting Zookeeper, 1 servers, 1 brokers and 1 controller *****");
    runner.startAll();
    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      try {
        printStatus(Color.GREEN, "***** Shutting down hybrid quick start *****");
        runner.stop();
        stream.shutdown();
        _kafkaStarter.stop();
        ZkStarter.stopLocalZkServer(_zookeeperInstance);
        FileUtils.deleteDirectory(quickstartTmpDir);
      } catch (Exception e) {
        e.printStackTrace();
      }
    }));
    printStatus(Color.YELLOW, "***** Bootstrap airlineStats offline and realtime table *****");
    runner.bootstrapTable();

    printStatus(Color.YELLOW, "***** Pinot Hybrid with hybrid table setup is complete *****");
    printStatus(Color.YELLOW, "***** Sequence of operations *****");
    printStatus(Color.YELLOW, "*****    1. Started 1 controller instance where tenant creation is enabled *****");
    printStatus(Color.YELLOW, "*****    2. Started 2 servers and 2 brokers *****");
    printStatus(Color.YELLOW, "*****    3. Created a server tenant with 1 offline and 1 realtime instance *****");
    printStatus(Color.YELLOW, "*****    4. Created a broker tenant with 2 instances *****");
    printStatus(Color.YELLOW, "*****    5. Added a schema *****");
    printStatus(Color.YELLOW,
        "*****    6. Created an offline and a realtime table with the tenant names created above *****");
    printStatus(Color.YELLOW, "*****    7. Built and pushed an offline segment *****");
    printStatus(Color.YELLOW,
        "*****    8. Started publishing a Kafka stream for the realtime instance to start consuming *****");
    printStatus(Color.YELLOW, "*****    9. Sleep 5 Seconds to wait for all components brought up *****");
    Thread.sleep(5000);

    String q1 = "select count(*) from airlineStats limit 1";
    printStatus(Color.YELLOW, "Total number of documents in the table");
    printStatus(Color.CYAN, "Query : " + q1);
    printStatus(Color.YELLOW, prettyPrintResponse(runner.runQuery(q1)));
    printStatus(Color.GREEN, "***************************************************");

    String q2 =
        "select AirlineID, sum(Cancelled) from airlineStats group by AirlineID order by sum(Cancelled) desc limit 5";
    printStatus(Color.YELLOW, "Top 5 airlines in cancellation ");
    printStatus(Color.CYAN, "Query : " + q2);
    printStatus(Color.YELLOW, prettyPrintResponse(runner.runQuery(q2)));
    printStatus(Color.GREEN, "***************************************************");

    String q3 =
        "select AirlineID, Year, sum(Flights) from airlineStats where Year > 2010 group by AirlineID, Year order by "
            + "sum(Flights) desc limit 5";
    printStatus(Color.YELLOW, "Top 5 airlines in number of flights after 2010");
    printStatus(Color.CYAN, "Query : " + q3);
    printStatus(Color.YELLOW, prettyPrintResponse(runner.runQuery(q3)));
    printStatus(Color.GREEN, "***************************************************");

    String q4 =
        "select OriginCityName, max(Flights) from airlineStats group by OriginCityName order by max(Flights) desc "
            + "limit 5";
    printStatus(Color.YELLOW, "Top 5 cities for number of flights");
    printStatus(Color.CYAN, "Query : " + q4);
    printStatus(Color.YELLOW, prettyPrintResponse(runner.runQuery(q4)));
    printStatus(Color.GREEN, "***************************************************");

    String q5 = "select AirlineID, OriginCityName, DestCityName, Year from airlineStats order by Year limit 5";
    printStatus(Color.YELLOW, "Print AirlineID, OriginCityName, DestCityName, Year for 5 records ordered by Year");
    printStatus(Color.CYAN, "Query : " + q5);
    printStatus(Color.YELLOW, prettyPrintResponse(runner.runQuery(q5)));
    printStatus(Color.GREEN, "***************************************************");

    printStatus(Color.GREEN, "You can always go to http://localhost:9000 to play around in the query console");
  }
}
