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
import org.apache.pinot.core.realtime.impl.kafka.KafkaStarterUtils;
import org.apache.pinot.core.realtime.stream.StreamDataProvider;
import org.apache.pinot.core.realtime.stream.StreamDataServerStartable;
import org.apache.pinot.tools.Quickstart.Color;
import org.apache.pinot.tools.admin.command.QuickstartRunner;
import org.apache.pinot.tools.streams.MeetupRsvpStream;

import static org.apache.pinot.tools.Quickstart.prettyPrintResponse;
import static org.apache.pinot.tools.Quickstart.printStatus;


public class RealtimeQuickStart {
  private StreamDataServerStartable _kafkaStarter;

  private RealtimeQuickStart() {
  }

  public static void main(String[] args)
      throws Exception {
    new RealtimeQuickStart().execute();
  }

  public void execute()
      throws Exception {
    final File quickStartDataDir = new File("quickStartData" + System.currentTimeMillis());

    if (!quickStartDataDir.exists()) {
      Preconditions.checkState(quickStartDataDir.mkdirs());
    }

    File schemaFile = new File(quickStartDataDir, "meetupRsvp_schema.json");
    File tableConfigFile = new File(quickStartDataDir, "meetupRsvp_realtime_table_config.json");

    ClassLoader classLoader = Quickstart.class.getClassLoader();
    URL resource = classLoader.getResource("sample_data/meetupRsvp_schema.json");
    com.google.common.base.Preconditions.checkNotNull(resource);
    FileUtils.copyURLToFile(resource, schemaFile);
    resource = classLoader.getResource("sample_data/meetupRsvp_realtime_table_config.json");
    com.google.common.base.Preconditions.checkNotNull(resource);
    FileUtils.copyURLToFile(resource, tableConfigFile);

    File tempDir = new File("/tmp", String.valueOf(System.currentTimeMillis()));
    Preconditions.checkState(tempDir.mkdirs());
    QuickstartTableRequest request = new QuickstartTableRequest("meetupRsvp", schemaFile, tableConfigFile);
    final QuickstartRunner runner = new QuickstartRunner(Lists.newArrayList(request), 1, 1, 1, tempDir);

    printStatus(Color.CYAN, "***** Starting Kafka *****");
    final ZkStarter.ZookeeperInstance zookeeperInstance = ZkStarter.startLocalZkServer();
    try {
      _kafkaStarter = StreamDataProvider.getServerDataStartable(KafkaStarterUtils.KAFKA_SERVER_STARTABLE_CLASS_NAME, KafkaStarterUtils.getDefaultKafkaConfiguration());
    } catch (Exception e) {
      throw new RuntimeException("Failed to start " + KafkaStarterUtils.KAFKA_SERVER_STARTABLE_CLASS_NAME, e);
    }
    _kafkaStarter.start();
    _kafkaStarter.createTopic("meetupRSVPEvents", KafkaStarterUtils.getTopicCreationProps(10));

    printStatus(Color.CYAN, "***** Starting Zookeeper, controller, server and broker *****");
    runner.startAll();
    printStatus(Color.CYAN, "***** Adding meetupRSVP schema *****");
    runner.addSchema();
    printStatus(Color.CYAN, "***** Adding meetupRSVP table *****");
    runner.addTable();
    printStatus(Color.CYAN, "***** Starting meetup data stream and publishing to Kafka *****");
    final MeetupRsvpStream meetupRSVPProvider = new MeetupRsvpStream(schemaFile);
    meetupRSVPProvider.run();
    printStatus(Color.CYAN, "***** Waiting for 5 seconds for a few events to get populated *****");
    Thread.sleep(5000);

    Runtime.getRuntime().addShutdownHook(new Thread() {
      @Override
      public void run() {
        try {
          printStatus(Color.GREEN, "***** Shutting down realtime quick start *****");
          meetupRSVPProvider.stopPublishing();
          runner.stop();
          _kafkaStarter.stop();
          ZkStarter.stopLocalZkServer(zookeeperInstance);
          FileUtils.deleteDirectory(quickStartDataDir);
        } catch (Exception e) {
          e.printStackTrace();
        }
      }
    });

    printStatus(Color.YELLOW, "***** Realtime quickstart setup complete *****");

    String q1 = "select count(*) from meetupRsvp limit 0";
    printStatus(Color.YELLOW, "Total number of documents in the table");
    printStatus(Color.CYAN, "Query : " + q1);
    printStatus(Color.YELLOW, prettyPrintResponse(runner.runQuery(q1)));
    printStatus(Color.GREEN, "***************************************************");

    String q2 = "select sum(rsvp_count) from meetupRsvp group by group_city top 10 limit 0";
    printStatus(Color.YELLOW, "Top 10 cities with the most rsvp");
    printStatus(Color.CYAN, "Query : " + q2);
    printStatus(Color.YELLOW, prettyPrintResponse(runner.runQuery(q2)));
    printStatus(Color.GREEN, "***************************************************");

    String q3 = "select * from meetupRsvp order by mtime limit 10";
    printStatus(Color.YELLOW, "Show 10 most recent rsvps");
    printStatus(Color.CYAN, "Query : " + q3);
    printStatus(Color.YELLOW, prettyPrintResponse(runner.runQuery(q3)));
    printStatus(Color.GREEN, "***************************************************");

    String q4 = "select sum(rsvp_count) from meetupRsvp group by event_name top 10 limit 0";
    printStatus(Color.YELLOW, "Show top 10 rsvp'ed events");
    printStatus(Color.CYAN, "Query : " + q4);
    printStatus(Color.YELLOW, prettyPrintResponse(runner.runQuery(q4)));
    printStatus(Color.GREEN, "***************************************************");

    String q5 = "select count(*) from meetupRsvp limit 0";
    printStatus(Color.YELLOW, "Total number of documents in the table");
    printStatus(Color.CYAN, "Query : " + q5);
    printStatus(Color.YELLOW, prettyPrintResponse(runner.runQuery(q5)));
    printStatus(Color.GREEN, "***************************************************");

    printStatus(Color.GREEN, "You can always go to http://localhost:9000/query/ to play around in the query console");
  }
}
