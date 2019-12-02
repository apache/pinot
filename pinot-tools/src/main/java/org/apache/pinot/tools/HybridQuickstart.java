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
import org.apache.commons.io.FileUtils;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.common.utils.ZkStarter;
import org.apache.pinot.core.data.readers.FileFormat;
import org.apache.pinot.core.realtime.impl.kafka.KafkaStarterUtils;
import org.apache.pinot.core.realtime.stream.StreamDataProvider;
import org.apache.pinot.core.realtime.stream.StreamDataServerStartable;
import org.apache.pinot.tools.Quickstart.Color;
import org.apache.pinot.tools.admin.command.QuickstartRunner;
import org.apache.pinot.tools.streams.AirlineDataStream;

import static org.apache.pinot.tools.Quickstart.printStatus;


public class HybridQuickstart {
  private File _offlineQuickStartDataDir;
  private File _realtimeQuickStartDataDir;
  private StreamDataServerStartable _kafkaStarter;
  private ZkStarter.ZookeeperInstance _zookeeperInstance;
  private File _schemaFile;
  private File _dataFile;

  private HybridQuickstart() {
  }

  public static void main(String[] args)
      throws Exception {
    new HybridQuickstart().execute();
  }

  private QuickstartTableRequest prepareOfflineTableRequest()
      throws IOException {
    _offlineQuickStartDataDir = new File("quickStartData" + System.currentTimeMillis());

    if (!_offlineQuickStartDataDir.exists()) {
      Preconditions.checkState(_offlineQuickStartDataDir.mkdirs());
    }

    _schemaFile = new File(_offlineQuickStartDataDir, "airlineStats_schema.json");
    _dataFile = new File(_offlineQuickStartDataDir, "airlineStats_data.avro");
    File tableConfigFile = new File(_offlineQuickStartDataDir, "airlineStats_offline_table_config.json");

    ClassLoader classLoader = Quickstart.class.getClassLoader();
    URL resource = classLoader.getResource("sample_data/airlineStats_schema.json");
    Preconditions.checkNotNull(resource);
    FileUtils.copyURLToFile(resource, _schemaFile);
    resource = classLoader.getResource("sample_data/airlineStats_data.avro");
    Preconditions.checkNotNull(resource);
    FileUtils.copyURLToFile(resource, _dataFile);
    resource = classLoader.getResource("sample_data/airlineStats_offline_table_config.json");
    Preconditions.checkNotNull(resource);
    FileUtils.copyURLToFile(resource, tableConfigFile);

    return new QuickstartTableRequest("airlineStats", _schemaFile, tableConfigFile, _offlineQuickStartDataDir,
        FileFormat.AVRO);
  }

  private QuickstartTableRequest prepareRealtimeTableRequest()
      throws IOException {
    _realtimeQuickStartDataDir = new File("quickStartData" + System.currentTimeMillis());

    if (!_realtimeQuickStartDataDir.exists()) {
      Preconditions.checkState(_realtimeQuickStartDataDir.mkdirs());
    }

    File tableConfigFile = new File(_realtimeQuickStartDataDir, "airlineStats_realtime_table_config.json");

    URL resource = Quickstart.class.getClassLoader().getResource(
        "sample_data/airlineStats_realtime_table_config.json");
    Preconditions.checkNotNull(resource);
    FileUtils.copyURLToFile(resource, tableConfigFile);

    return new QuickstartTableRequest("airlineStats", _schemaFile, tableConfigFile);
  }

  private void startKafka() {
    _zookeeperInstance = ZkStarter.startLocalZkServer();
    try {
      _kafkaStarter = StreamDataProvider.getServerDataStartable(KafkaStarterUtils.KAFKA_SERVER_STARTABLE_CLASS_NAME, KafkaStarterUtils.getDefaultKafkaConfiguration());
    } catch (Exception e) {
      throw new RuntimeException("Failed to start " + KafkaStarterUtils.KAFKA_SERVER_STARTABLE_CLASS_NAME, e);
    }
    _kafkaStarter.start();
    _kafkaStarter.createTopic("airlineStatsEvents", KafkaStarterUtils.getTopicCreationProps(10));
  }

  public void execute()
      throws Exception {
    QuickstartTableRequest offlineRequest = prepareOfflineTableRequest();
    QuickstartTableRequest realtimeTableRequest = prepareRealtimeTableRequest();

    File tempDir = new File("/tmp", String.valueOf(System.currentTimeMillis()));
    Preconditions.checkState(tempDir.mkdirs());
    final QuickstartRunner runner =
        new QuickstartRunner(Lists.newArrayList(offlineRequest, realtimeTableRequest), 2, 2, 1, tempDir, false);
    printStatus(Color.YELLOW, "***** Starting Kafka  *****");
    startKafka();
    printStatus(Color.YELLOW, "***** Starting Zookeeper, 2 servers, 2 brokers and 1 controller *****");
    runner.startAll();
    printStatus(Color.YELLOW, "***** Creating a server tenant with name 'airline' *****");
    runner.createServerTenantWith(1, 1, "airline");
    printStatus(Color.YELLOW, "***** Creating a broker tenant with name 'airline_broker' *****");
    runner.createBrokerTenantWith(2, "airline_broker");
    printStatus(Color.YELLOW, "***** Adding airlineStats schema *****");
    runner.addSchema();
    printStatus(Color.YELLOW, "***** Adding airlineStats offline and realtime table *****");
    runner.addTable();
    printStatus(Color.YELLOW, "***** Building index segment for airlineStats *****");
    runner.buildSegment();
    printStatus(Color.YELLOW, "***** Pushing segment to the controller *****");
    runner.pushSegment();

    printStatus(Color.YELLOW, "***** Starting airline data stream and publishing to Kafka *****");
    final AirlineDataStream stream = new AirlineDataStream(Schema.fromFile(_schemaFile), _dataFile);
    stream.run();

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
    printStatus(Color.YELLOW, "***** go to http://localhost:9000/query to run a few queries *****");

    Runtime.getRuntime().addShutdownHook(new Thread() {
      @Override
      public void run() {
        try {
          printStatus(Color.GREEN, "***** Shutting down hybrid quick start *****");
          stream.shutdown();
          Thread.sleep(2000);
          runner.stop();
          _kafkaStarter.stop();
          ZkStarter.stopLocalZkServer(_zookeeperInstance);
          FileUtils.deleteDirectory(_offlineQuickStartDataDir);
          FileUtils.deleteDirectory(_realtimeQuickStartDataDir);
        } catch (Exception e) {
          e.printStackTrace();
        }
      }
    });
  }
}
