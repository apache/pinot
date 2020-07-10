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
import org.apache.pinot.tools.streams.githubevents.PullRequestMergedEventsStream;
import org.apache.pinot.tools.utils.KafkaStarterUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.pinot.tools.Quickstart.prettyPrintResponse;
import static org.apache.pinot.tools.Quickstart.printStatus;


/**
 * Sets up a demo Pinot cluster with 1 zookeeper, 1 controller, 1 broker and 1 server
 * Sets up a demo Kafka cluster, and creates a topic pullRequestMergedEvents
 * Creates a realtime table pullRequestMergedEvents
 * Starts the {@link PullRequestMergedEventsStream} to publish pullRequestMergedEvents into the topic
 */
public class GitHubEventsQuickstart {
  private static final Logger LOGGER = LoggerFactory.getLogger(GitHubEventsQuickstart.class);
  private StreamDataServerStartable _kafkaStarter;
  private ZkStarter.ZookeeperInstance _zookeeperInstance;

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

  public void execute(String personalAccessToken)
      throws Exception {
    final File quickStartDataDir = new File("githubEvents" + System.currentTimeMillis());

    if (!quickStartDataDir.exists()) {
      Preconditions.checkState(quickStartDataDir.mkdirs());
    }

    File schemaFile = new File(quickStartDataDir, "pullRequestMergedEvents_schema.json");
    File tableConfigFile = new File(quickStartDataDir, "pullRequestMergedEvents_realtime_table_config.json");

    ClassLoader classLoader = Quickstart.class.getClassLoader();
    URL resource = classLoader.getResource("examples/stream/githubEvents/pullRequestMergedEvents_schema.json");
    Preconditions.checkNotNull(resource);
    FileUtils.copyURLToFile(resource, schemaFile);
    resource =
        classLoader.getResource("examples/stream/githubEvents/pullRequestMergedEvents_realtime_table_config.json");
    Preconditions.checkNotNull(resource);
    FileUtils.copyURLToFile(resource, tableConfigFile);

    File tempDir = new File(FileUtils.getTempDirectory(), String.valueOf(System.currentTimeMillis()));
    Preconditions.checkState(tempDir.mkdirs());
    QuickstartTableRequest request = new QuickstartTableRequest("pullRequestMergedEvents", schemaFile, tableConfigFile);
    final QuickstartRunner runner = new QuickstartRunner(Lists.newArrayList(request), 1, 1, 1, tempDir);

    printStatus(Color.CYAN, "***** Starting Kafka *****");
    startKafka();

    printStatus(Color.CYAN, "***** Starting zookeeper, controller, server and broker *****");
    runner.startAll();

    printStatus(Color.CYAN, "***** Adding pullRequestMergedEvents table *****");
    runner.addTable();

    printStatus(Color.CYAN, "***** Starting pullRequestMergedEvents data stream and publishing to Kafka *****");
    final PullRequestMergedEventsStream pullRequestMergedEventsStream =
        new PullRequestMergedEventsStream(schemaFile.getAbsolutePath(), "pullRequestMergedEvents",
            KafkaStarterUtils.DEFAULT_KAFKA_BROKER, personalAccessToken);
    pullRequestMergedEventsStream.execute();
    printStatus(Color.CYAN, "***** Waiting for 10 seconds for a few events to get populated *****");
    Thread.sleep(10000);

    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      try {
        printStatus(Color.GREEN, "***** Shutting down GitHubEventsQuickStart *****");
        runner.stop();
        _kafkaStarter.stop();
        ZkStarter.stopLocalZkServer(_zookeeperInstance);
        FileUtils.deleteDirectory(quickStartDataDir);
      } catch (Exception e) {
        LOGGER.error("Caught exception in shutting down GitHubEvents QuickStart", e);
      }
    }));

    printStatus(Color.YELLOW, "***** Realtime github demo quickstart setup complete *****");

    String q1 = "select count(*) from pullRequestMergedEvents limit 0";
    printStatus(Color.YELLOW, "Total number of documents in the table");
    printStatus(Color.CYAN, "Query : " + q1);
    printStatus(Color.YELLOW, prettyPrintResponse(runner.runQuery(q1)));
    printStatus(Color.GREEN, "***************************************************");

    String q2 = "select sum(numLinesAdded) from pullRequestMergedEvents group by repo top 10 limit 0";
    printStatus(Color.YELLOW, "Top 10 repo with the most lines added");
    printStatus(Color.CYAN, "Query : " + q2);
    printStatus(Color.YELLOW, prettyPrintResponse(runner.runQuery(q2)));
    printStatus(Color.GREEN, "***************************************************");

    String q3 = "select * from pullRequestMergedEvents where authorAssociation = 'COLLABORATOR' limit 10";
    printStatus(Color.YELLOW, "Show data for COLLABORATORS");
    printStatus(Color.CYAN, "Query : " + q3);
    printStatus(Color.YELLOW, prettyPrintResponse(runner.runQuery(q3)));
    printStatus(Color.GREEN, "***************************************************");

    String q4 = "select max(elapsedTimeMillis) from pullRequestMergedEvents group by repo top 10 limit 0";
    printStatus(Color.YELLOW, "Show repos with longest alive pull requests");
    printStatus(Color.CYAN, "Query : " + q4);
    printStatus(Color.YELLOW, prettyPrintResponse(runner.runQuery(q4)));
    printStatus(Color.GREEN, "***************************************************");

    String q5 = "select count(*) from pullRequestMergedEvents";
    printStatus(Color.YELLOW, "Total number of documents in the table");
    printStatus(Color.CYAN, "Query : " + q5);
    printStatus(Color.YELLOW, prettyPrintResponse(runner.runQuery(q5)));
    printStatus(Color.GREEN, "***************************************************");

    printStatus(Color.GREEN, "You can always go to http://localhost:9000 to play around in the query console");
  }
}
