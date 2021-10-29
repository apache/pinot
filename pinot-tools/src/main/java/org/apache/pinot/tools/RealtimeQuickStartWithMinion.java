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
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.LineIterator;
import org.apache.pinot.common.utils.ZkStarter;
import org.apache.pinot.spi.stream.StreamDataProducer;
import org.apache.pinot.spi.stream.StreamDataProvider;
import org.apache.pinot.spi.stream.StreamDataServerStartable;
import org.apache.pinot.tools.Quickstart.Color;
import org.apache.pinot.tools.admin.PinotAdministrator;
import org.apache.pinot.tools.admin.command.QuickstartRunner;
import org.apache.pinot.tools.utils.KafkaStarterUtils;

import static org.apache.pinot.tools.Quickstart.prettyPrintResponse;
import static org.apache.pinot.tools.Quickstart.printStatus;


/**
 * This quickstart shows how RealtimeToOfflineSegmentsTask and MergeRollupTask minion
 * tasks continuously optimize segments as data gets ingested into Realtime table.
 */
public class RealtimeQuickStartWithMinion extends QuickStartBase {
  private StreamDataServerStartable _kafkaStarter;

  public static void main(String[] args)
      throws Exception {
    List<String> arguments = new ArrayList<>();
    arguments.addAll(Arrays.asList("QuickStart", "-type", "REALTIME-MINION"));
    arguments.addAll(Arrays.asList(args));
    PinotAdministrator.main(arguments.toArray(new String[arguments.size()]));
  }

  public Map<String, Object> getConfigOverrides() {
    Map<String, Object> properties = new HashMap<>();
    properties.put("controller.task.scheduler.enabled", true);
    return properties;
  }

  public void execute()
      throws Exception {
    File quickstartTmpDir = new File(_tmpDir, String.valueOf(System.currentTimeMillis()));
    File baseDir = new File(quickstartTmpDir, "githubEvents");
    File dataDir = new File(baseDir, "rawdata");
    Preconditions.checkState(dataDir.mkdirs());

    File schemaFile = new File(baseDir, "githubEvents_schema.json");
    File realtimeTableConfigFile = new File(baseDir, "githubEvents_realtime_table_config.json");
    File offlineTableConfigFile = new File(baseDir, "githubEvents_offline_table_config.json");
    File inputDataFile = new File(baseDir, "2021-07-21-few-hours.json");

    ClassLoader classLoader = Quickstart.class.getClassLoader();
    URL resource = classLoader.getResource("examples/minions/stream/githubEvents/githubEvents_schema.json");
    Preconditions.checkNotNull(resource);
    FileUtils.copyURLToFile(resource, schemaFile);
    resource = classLoader.getResource("examples/minions/stream/githubEvents/githubEvents_realtime_table_config.json");
    Preconditions.checkNotNull(resource);
    FileUtils.copyURLToFile(resource, realtimeTableConfigFile);
    resource = classLoader.getResource("examples/minions/stream/githubEvents/githubEvents_offline_table_config.json");
    Preconditions.checkNotNull(resource);
    FileUtils.copyURLToFile(resource, offlineTableConfigFile);
    resource = Quickstart.class.getClassLoader()
        .getResource("examples/minions/stream/githubEvents/rawdata/2021-07-21-few-hours.json");
    Preconditions.checkNotNull(resource);
    FileUtils.copyURLToFile(resource, inputDataFile);

    QuickstartTableRequest request = new QuickstartTableRequest(baseDir.getAbsolutePath());
    QuickstartRunner runner =
        new QuickstartRunner(Lists.newArrayList(request), 1, 1, 1, 1, dataDir, true, null, getConfigOverrides());

    printStatus(Color.CYAN, "***** Starting Kafka *****");
    final ZkStarter.ZookeeperInstance zookeeperInstance = ZkStarter.startLocalZkServer();
    try {
      _kafkaStarter = StreamDataProvider.getServerDataStartable(KafkaStarterUtils.KAFKA_SERVER_STARTABLE_CLASS_NAME,
          KafkaStarterUtils.getDefaultKafkaConfiguration(zookeeperInstance));
    } catch (Exception e) {
      throw new RuntimeException("Failed to start " + KafkaStarterUtils.KAFKA_SERVER_STARTABLE_CLASS_NAME, e);
    }
    _kafkaStarter.start();

    printStatus(Color.CYAN, "***** Starting Zookeeper, controller, server and broker *****");
    runner.startAll();
    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      try {
        printStatus(Color.GREEN, "***** Shutting down realtime-minion quick start *****");
        runner.stop();
        _kafkaStarter.stop();
        ZkStarter.stopLocalZkServer(zookeeperInstance);
        FileUtils.deleteDirectory(quickstartTmpDir);
      } catch (Exception e) {
        e.printStackTrace();
      }
    }));

    printStatus(Color.CYAN, "***** Sending events to Kafka *****");
    _kafkaStarter.createTopic("githubEvents", KafkaStarterUtils.getTopicCreationProps(2));
    publishGithubEventsToKafka("githubEvents", inputDataFile);

    printStatus(Color.CYAN, "***** Bootstrap githubEvents tables *****");
    runner.bootstrapTable();
    printStatus(Color.CYAN, "***** Waiting for 5 seconds for a few events to get populated *****");
    Thread.sleep(5000);

    printStatus(Color.YELLOW, "***** Realtime-minion quickstart setup complete *****");

    String q1 = "select count(*) from githubEvents limit 1";
    printStatus(Color.YELLOW, "Current number of documents in the table");
    printStatus(Color.CYAN, "Query : " + q1);
    printStatus(Color.YELLOW, prettyPrintResponse(runner.runQuery(q1)));
    printStatus(Color.GREEN, "***************************************************");

    printStatus(Color.GREEN, "You can always go to http://localhost:9000 to play around in the query console");
    printStatus(Color.GREEN, "In particular, you will find that OFFLINE table gets segments from REALTIME table;");
    printStatus(Color.GREEN, "and segments in OFFLINE table get merged into larger ones within a few minutes.");
  }

  private static void publishGithubEventsToKafka(String topicName, File dataFile)
      throws Exception {
    Properties properties = new Properties();
    properties.put("metadata.broker.list", KafkaStarterUtils.DEFAULT_KAFKA_BROKER);
    properties.put("serializer.class", "kafka.serializer.DefaultEncoder");
    properties.put("request.required.acks", "1");
    StreamDataProducer producer =
        StreamDataProvider.getStreamDataProducer(KafkaStarterUtils.KAFKA_PRODUCER_CLASS_NAME, properties);
    try {
      LineIterator dataStream = FileUtils.lineIterator(dataFile);
      while (dataStream.hasNext()) {
        producer.produce(topicName, dataStream.nextLine().getBytes(StandardCharsets.UTF_8));
      }
    } finally {
      producer.close();
    }
  }
}
