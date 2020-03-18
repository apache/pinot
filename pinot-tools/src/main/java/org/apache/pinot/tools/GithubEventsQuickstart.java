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
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.plugin.PluginManager;
import org.apache.pinot.spi.stream.StreamDataProvider;
import org.apache.pinot.spi.stream.StreamDataServerStartable;
import org.apache.pinot.tools.Quickstart.Color;
import org.apache.pinot.tools.admin.command.QuickstartRunner;
import org.apache.pinot.tools.streams.githubevents.PullRequestMergedEventStream;
import org.apache.pinot.tools.utils.KafkaStarterUtils;

import static org.apache.pinot.tools.Quickstart.prettyPrintResponse;
import static org.apache.pinot.tools.Quickstart.printStatus;


/**
 * Sets up a demo Pinot cluster with a realtime table  consuming realtime data from Github events
 */
public class GithubEventsQuickstart {
  private StreamDataServerStartable _kafkaStarter;
  private ZkStarter.ZookeeperInstance _zookeeperInstance;

  public static void main(String[] args)
      throws Exception {
    PluginManager.get().init();
    new GithubEventsQuickstart().execute(args[0]);
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

    File tempDir = new File("/tmp", String.valueOf(System.currentTimeMillis()));
    Preconditions.checkState(tempDir.mkdirs());
    QuickstartTableRequest request = new QuickstartTableRequest("pullRequestMergedEvents", schemaFile, tableConfigFile);
    final QuickstartRunner runner = new QuickstartRunner(Lists.newArrayList(request), 1, 1, 1, tempDir);

    printStatus(Color.CYAN, "***** Starting Kafka *****");
    startKafka();

    printStatus(Color.CYAN, "***** Starting Zookeeper, controller, server and broker *****");
    runner.startAll();

    printStatus(Color.CYAN, "***** Adding pullRequestMergedEvents table *****");
    runner.addTable();

    printStatus(Color.CYAN, "***** Starting pullRequestMergedEvents data stream and publishing to Kafka *****");
    final PullRequestMergedEventStream pullRequestMergedEventStream =
        new PullRequestMergedEventStream(Schema.fromFile(schemaFile), "pullRequestMergedEvents", personalAccessToken);
    pullRequestMergedEventStream.run();
    printStatus(Color.CYAN, "***** Waiting for 10 seconds for a few events to get populated *****");
    Thread.sleep(10000);

    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      try {
        printStatus(Color.GREEN, "***** Shutting down realtime quick start *****");
        pullRequestMergedEventStream.shutdown();
        runner.stop();
        _kafkaStarter.stop();
        ZkStarter.stopLocalZkServer(_zookeeperInstance);
        FileUtils.deleteDirectory(quickStartDataDir);
      } catch (Exception e) {
        e.printStackTrace();
      }
    }));

    printStatus(Color.YELLOW, "***** Realtime quickstart setup complete *****");

    String q1 = "select count(*) from pullRequestMergedEvents limit 0";
    printStatus(Color.YELLOW, "Total number of documents in the table");
    printStatus(Color.CYAN, "Query : " + q1);
    printStatus(Color.YELLOW, prettyPrintResponse(runner.runQuery(q1)));
    printStatus(Color.GREEN, "***************************************************");

    String q2 = "select repo, sum(numLinesAdded), sum(numLinesDeleted) from pullRequestMergedEvents group by repo top 10 limit 0";
    printStatus(Color.YELLOW, "Top 10 repo with the most ");
    printStatus(Color.CYAN, "Query : " + q2);
    printStatus(Color.YELLOW, prettyPrintResponse(runner.runQuery(q2)));
    printStatus(Color.GREEN, "***************************************************");

    String q3 = "select count(*), authorAssociation from pullRequestMergedEvents group by authorAssociation order by count(*) limit 10";
    printStatus(Color.YELLOW, "Show 10 most recent merged pull requests");
    printStatus(Color.CYAN, "Query : " + q3);
    printStatus(Color.YELLOW, prettyPrintResponse(runner.runQuery(q3)));
    printStatus(Color.GREEN, "***************************************************");

    String q4 = "select max(elapsedTimeMillis) from pullRequestMergedEvents group by repo top 10 limit 0";
    printStatus(Color.YELLOW, "Show longest alive pull requests");
    printStatus(Color.CYAN, "Query : " + q4);
    printStatus(Color.YELLOW, prettyPrintResponse(runner.runQuery(q4)));
    printStatus(Color.GREEN, "***************************************************");

    String q5 = "select count(*) from pullRequestMergedEvents limit 0";
    printStatus(Color.YELLOW, "Total number of documents in the table");
    printStatus(Color.CYAN, "Query : " + q5);
    printStatus(Color.YELLOW, prettyPrintResponse(runner.runQuery(q5)));
    printStatus(Color.GREEN, "***************************************************");

    printStatus(Color.GREEN, "You can always go to http://localhost:9000/query/ to play around in the query console");
  }
}
