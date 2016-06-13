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
package com.linkedin.pinot.tools;

import static com.linkedin.pinot.tools.Quickstart.printStatus;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.apache.log4j.Level;
import org.json.JSONException;

import com.google.common.collect.Lists;
import com.linkedin.pinot.common.data.Schema;
import com.linkedin.pinot.common.utils.KafkaStarterUtils;
import com.linkedin.pinot.common.utils.ZkStarter;
import com.linkedin.pinot.core.data.readers.FileFormat;
import com.linkedin.pinot.tools.Quickstart.color;
import com.linkedin.pinot.tools.admin.command.QuickstartRunner;
import com.linkedin.pinot.tools.streams.AirlineDataStream;

import kafka.server.KafkaServerStartable;


/**
 * 
 * 
 *
 */
public class HybridQuickstart {

  private File _offlineQuickStartDataDir;
  private File _realtimeQuickStartDataDir;
  KafkaServerStartable kafkaStarter;
  private ZkStarter.ZookeeperInstance _zookeeperInstance;

  private QuickstartTableRequest prepareOfflineTableRequest() throws IOException {
    _offlineQuickStartDataDir = new File("quickStartData" + System.currentTimeMillis());

    if (!_offlineQuickStartDataDir.exists()) {
      _offlineQuickStartDataDir.mkdir();
    }

    _offlineQuickStartDataDir = new File("quickStartData" + System.currentTimeMillis());

    if (!_offlineQuickStartDataDir.exists()) {
      _offlineQuickStartDataDir.mkdir();
    }

    File schemaFile = new File(_offlineQuickStartDataDir + "/airlineStats.schema");
    File dataFile = new File(_offlineQuickStartDataDir + "/airline.avro");
    File tableCreationJsonFileName = new File(_offlineQuickStartDataDir + "/airlineStatsOffline_hybrid.json");

    FileUtils.copyURLToFile(Quickstart.class.getClassLoader().getResource("sample_data/airlineStats.schema"),
        schemaFile);
    FileUtils.copyURLToFile(Quickstart.class.getClassLoader().getResource("sample_data/airline.avro"), dataFile);

    FileUtils.copyURLToFile(
        Quickstart.class.getClassLoader().getResource("sample_data/airlineStatsOffline_hybrid.json"),
        tableCreationJsonFileName);

    String tableName = "airlineStats";
    return new QuickstartTableRequest(tableName, schemaFile, tableCreationJsonFileName, _offlineQuickStartDataDir,
        FileFormat.AVRO);
  }

  private QuickstartTableRequest prepareRealtimeTableRequest() throws IOException {
    _realtimeQuickStartDataDir = new File("quickStartData" + System.currentTimeMillis());
    String quickStartDataDirName = _realtimeQuickStartDataDir.getName();

    if (!_realtimeQuickStartDataDir.exists()) {
      _realtimeQuickStartDataDir.mkdir();
    }

    File schema = new File(quickStartDataDirName + "/airlineStats.schema");
    File tableCreate = new File(quickStartDataDirName + "/airlineStatsRealtime_hybrid.json");

    FileUtils.copyURLToFile(RealtimeQuickStart.class.getClassLoader().getResource("sample_data/airlineStats.schema"),
        schema);

    FileUtils.copyURLToFile(
        RealtimeQuickStart.class.getClassLoader().getResource("sample_data/airlineStatsRealtime_hybrid.json"),
        tableCreate);

    return new QuickstartTableRequest("airlineStats", schema, tableCreate);
  }

  private void startKafka() {
    _zookeeperInstance = ZkStarter.startLocalZkServer();

    kafkaStarter =
        KafkaStarterUtils.startServer(KafkaStarterUtils.DEFAULT_KAFKA_PORT, KafkaStarterUtils.DEFAULT_BROKER_ID,
            KafkaStarterUtils.DEFAULT_ZK_STR, KafkaStarterUtils.getDefaultKafkaConfiguration());

    KafkaStarterUtils.createTopic("airlineStatsEvents", KafkaStarterUtils.DEFAULT_ZK_STR);
  }

  public void execute() throws JSONException, Exception {

    QuickstartTableRequest offlineRequest = prepareOfflineTableRequest();
    QuickstartTableRequest realtimeTableRequest = prepareRealtimeTableRequest();

    File tempDir = new File("/tmp/" + System.currentTimeMillis());
    tempDir.mkdir();
    final QuickstartRunner runner =
        new QuickstartRunner(Lists.newArrayList(offlineRequest, realtimeTableRequest), 2, 2, 1, tempDir, false);
    printStatus(color.YELLOW, "***** starting kafka  *****");
    startKafka();

    printStatus(color.YELLOW, "***** starting 2 servers, 2 brokers and 1 controller  *****");
    runner.startAll();
    printStatus(color.YELLOW, "***** creating a server tenant with name  airline  *****");
    runner.createServerTenantWith(1, 1, "airline");
    printStatus(color.YELLOW, "***** creating a broker tenant with name  airline_broker  *****");
    runner.createBrokerTenantWith(2, "airline_broker");

    printStatus(color.YELLOW, "***** adding airline schema  *****");
    runner.addSchema();
    printStatus(color.YELLOW, "***** adding airline offline and realtime table  *****");
    runner.addTable();
    printStatus(color.YELLOW, "***** adding airline offline segment  *****");
    runner.buildSegment();
    printStatus(color.YELLOW, "***** pushing airline offline segment to the controller  *****");
    runner.pushSegment();
    File dataFile = new File(_offlineQuickStartDataDir + "/airline.avro");
    File schemaFile = new File(_offlineQuickStartDataDir + "/airlineStats.schema");

    printStatus(color.YELLOW,
        "***** publishing data to kafka for airline realtime to start consuming event stream  *****");
    final AirlineDataStream stream = new AirlineDataStream(Schema.fromFile(schemaFile), dataFile);
    stream.run();
    
    printStatus(color.YELLOW, "***** Pinot Hybrid with hybrid table setup is complete *****");
    printStatus(color.YELLOW, "*****    1. Sequence of operations *****");
    printStatus(color.YELLOW, "*****    2. Started 1 controller instances where tenant creation is enabled *****");
    printStatus(color.YELLOW, "*****    3. Started 2 servers and 2 brokers *****");
    printStatus(color.YELLOW, "*****    4. created a server tenant with 1 offline and 1 realtime instance *****");
    printStatus(color.YELLOW, "*****    5. Created a broker tenant with 2 instances *****");
    printStatus(color.YELLOW, "*****    6. Added a schema *****");
    printStatus(color.YELLOW, "*****    7. Created a offline and a realtime table with the tenant names created abover *****");
    printStatus(color.YELLOW, "*****    8. Created and published an offline segment *****");
    printStatus(color.YELLOW, "***** Also Started publishing a kafka stream for the realtime instance so start consuming *****");
    printStatus(color.YELLOW, "***** go to http://localhost:9000/query to run a few queries *****");

    Runtime.getRuntime().addShutdownHook(new Thread() {
      @Override
      public void run() {
        try {
          stream.shutdown();
          Thread.sleep(2000);
          printStatus(color.GREEN, "***** shutting down hybrid quick start *****");
          runner.stop();
          runner.clean();
          FileUtils.deleteDirectory(_offlineQuickStartDataDir);
          FileUtils.deleteDirectory(_realtimeQuickStartDataDir);
          KafkaStarterUtils.stopServer(kafkaStarter);
          ZkStarter.stopLocalZkServer(_zookeeperInstance);
        } catch (Exception e) {
        }
      }
    });
  }

  public static void main(String[] args) throws Exception {
    org.apache.log4j.Logger.getRootLogger().setLevel(Level.ERROR);
    HybridQuickstart st = new HybridQuickstart();
    st.execute();

    while (true) {

    }
  }
}
