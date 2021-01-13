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
import java.net.URL;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.common.utils.ZkStarter;
import org.apache.pinot.spi.stream.StreamDataProvider;
import org.apache.pinot.spi.stream.StreamDataServerStartable;
import org.apache.pinot.tools.Quickstart.Color;
import org.apache.pinot.tools.admin.command.QuickstartRunner;
import org.apache.pinot.tools.utils.KafkaStarterUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.pinot.tools.Quickstart.prettyPrintResponse;
import static org.apache.pinot.tools.Quickstart.printStatus;


/**
 * Sets up a demo Pinot cluster with 1 zookeeper, 1 controller, 1 broker and 1 server
 * Sets up a demo Kafka cluster for real-time ingestion
 * It takes a directory as input
 * <code>
 *  rawData
 *    - 1.csv
 *  table_config.json
 *  schema.json
 *  ingestion_job_spec.json
 *  </code>
 */
public class GenericQuickstart {
  private static final Logger LOGGER = LoggerFactory.getLogger(GenericQuickstart.class);
  private final File _schemaFile;
  private final File _tableConfigFile;
  private final File _tableDirectory;
  private final String _tableName;
  private StreamDataServerStartable _kafkaStarter;
  private ZkStarter.ZookeeperInstance _zookeeperInstance;

  public GenericQuickstart(String tableDirectoryPath, String tableName) {
    _tableDirectory = new File(tableDirectoryPath);
    _tableName = tableName;

    if (!_tableDirectory.exists()) {
      Preconditions.checkState(_tableDirectory.mkdirs());
    }
    _schemaFile = new File(_tableDirectory, "schema.json");
    _tableConfigFile = new File(_tableDirectory, "table_config.json");
  }

  private void startKafka() {
    _zookeeperInstance = ZkStarter.startLocalZkServer();
    try {
      _kafkaStarter = StreamDataProvider.getServerDataStartable(KafkaStarterUtils.KAFKA_SERVER_STARTABLE_CLASS_NAME,
          KafkaStarterUtils.getDefaultKafkaConfiguration());
    } catch (Exception e) {
      throw new RuntimeException("Failed to start " + KafkaStarterUtils.KAFKA_SERVER_STARTABLE_CLASS_NAME, e);
    }
    _kafkaStarter.start();
    _kafkaStarter.createTopic("pullRequestMergedEvents", KafkaStarterUtils.getTopicCreationProps(2));
  }

  public void execute()
      throws Exception {

    File tempDir = new File(FileUtils.getTempDirectory(), String.valueOf(System.currentTimeMillis()));
    Preconditions.checkState(tempDir.mkdirs());
    QuickstartTableRequest request = new QuickstartTableRequest(_tableDirectory.getAbsolutePath());
    final QuickstartRunner runner = new QuickstartRunner(Lists.newArrayList(request), 1, 1, 1, tempDir);

    printStatus(Color.CYAN, "***** Starting Kafka *****");
    startKafka();

    printStatus(Color.CYAN, "***** Starting zookeeper, controller, server and broker *****");
    runner.startAll();

    printStatus(Color.CYAN, "***** Adding table *****");
    runner.bootstrapTable();

    printStatus(Color.CYAN, "***** Waiting for 10 seconds for a few events to get populated *****");
    Thread.sleep(10000);

    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      try {
        printStatus(Color.GREEN, "***** Shutting down QuickStart cluster *****");
        runner.stop();
        _kafkaStarter.stop();
        ZkStarter.stopLocalZkServer(_zookeeperInstance);
        FileUtils.deleteDirectory(_tableDirectory);
      } catch (Exception e) {
        LOGGER.error("Caught exception in shutting down QuickStart cluster", e);
      }
    }));

    printStatus(Color.YELLOW, "***** GenericQuickStart demo quickstart setup complete *****");

    String q1 = "select count(*) from starbucksStores limit 0";
    printStatus(Color.YELLOW, "Total number of documents in the table");
    printStatus(Color.CYAN, "Query : " + q1);
    printStatus(Color.YELLOW, prettyPrintResponse(runner.runQuery(q1)));
    printStatus(Color.GREEN, "***************************************************");

    String q2 = "select address, ST_DISTANCE(location_st_point, ST_Point(-122, 37, 1)) from starbucksStores"
        + " WHERE ST_DISTANCE(location_st_point, ST_Point(-122, 37, 1)) < 5000 limit 1000";
    printStatus(Color.YELLOW, "Starbucks stores within 5km of the given point in bay area");
    printStatus(Color.CYAN, "Query : " + q2);
    printStatus(Color.YELLOW, prettyPrintResponse(runner.runQuery(q2)));
    printStatus(Color.GREEN, "***************************************************");

    String q3 = "select address, ST_DISTANCE(location_st_point, ST_Point(-122, 37, 1)) from starbucksStores"
        + " WHERE ST_DISTANCE(location_st_point, ST_Point(-122, 37, 1)) between 5000 and 10000 limit 1000";
    printStatus(Color.YELLOW, "Starbucks stores with distance of 5km to 10km from the given point in bay area");
    printStatus(Color.CYAN, "Query : " + q3);
    printStatus(Color.YELLOW, prettyPrintResponse(runner.runQuery(q3)));
    printStatus(Color.GREEN, "***************************************************");

    printStatus(Color.GREEN, "You can always go to http://localhost:9000 to play around in the query console");
  }

  public static void main(String[] args)
      throws Exception {
    ClassLoader classLoader = GenericQuickstart.class.getClassLoader();
    URL resource = classLoader.getResource("examples/batch/starbucksStores");
    String tableDirectoryPath = resource.getPath();

    GenericQuickstart quickstart = new GenericQuickstart(tableDirectoryPath, "starbucksStores");
    quickstart.execute();
  }
}
