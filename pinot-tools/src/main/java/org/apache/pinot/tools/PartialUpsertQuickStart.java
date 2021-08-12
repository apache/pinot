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
import org.apache.pinot.spi.plugin.PluginManager;
import org.apache.pinot.spi.stream.StreamDataProvider;
import org.apache.pinot.spi.stream.StreamDataServerStartable;
import org.apache.pinot.tools.Quickstart.Color;
import org.apache.pinot.tools.admin.command.QuickstartRunner;
import org.apache.pinot.tools.streams.MeetupRsvpStream;
import org.apache.pinot.tools.utils.KafkaStarterUtils;

import static org.apache.pinot.tools.Quickstart.prettyPrintResponse;
import static org.apache.pinot.tools.Quickstart.printStatus;


public class PartialUpsertQuickStart {
  private StreamDataServerStartable _kafkaStarter;

  public static void main(String[] args)
      throws Exception {
    PluginManager.get().init();
    new PartialUpsertQuickStart().execute();
  }

  // Todo: add a quick start demo
  public void execute()
      throws Exception {
    File quickstartTmpDir = new File(FileUtils.getTempDirectory(), String.valueOf(System.currentTimeMillis()));
    File bootstrapTableDir = new File(quickstartTmpDir, "meetupRsvp");
    File dataDir = new File(bootstrapTableDir, "data");
    Preconditions.checkState(dataDir.mkdirs());

    File schemaFile = new File(bootstrapTableDir, "meetupRsvp_schema.json");
    File tableConfigFile = new File(bootstrapTableDir, "meetupRsvp_realtime_table_config.json");

    ClassLoader classLoader = Quickstart.class.getClassLoader();
    URL resource = classLoader.getResource("examples/stream/meetupRsvp/upsert_partial_meetupRsvp_schema.json");
    Preconditions.checkNotNull(resource);
    FileUtils.copyURLToFile(resource, schemaFile);
    resource =
        classLoader.getResource("examples/stream/meetupRsvp/upsert_partial_meetupRsvp_realtime_table_config.json");
    Preconditions.checkNotNull(resource);
    FileUtils.copyURLToFile(resource, tableConfigFile);

    QuickstartTableRequest request = new QuickstartTableRequest(bootstrapTableDir.getAbsolutePath());
    final QuickstartRunner runner = new QuickstartRunner(Lists.newArrayList(request), 1, 1, 1, dataDir);

    printStatus(Color.CYAN, "***** Starting Kafka *****");
    final ZkStarter.ZookeeperInstance zookeeperInstance = ZkStarter.startLocalZkServer();
    try {
      _kafkaStarter = StreamDataProvider.getServerDataStartable(KafkaStarterUtils.KAFKA_SERVER_STARTABLE_CLASS_NAME,
          KafkaStarterUtils.getDefaultKafkaConfiguration());
    } catch (Exception e) {
      throw new RuntimeException("Failed to start " + KafkaStarterUtils.KAFKA_SERVER_STARTABLE_CLASS_NAME, e);
    }
    _kafkaStarter.start();
    _kafkaStarter.createTopic("meetupRSVPEvents", KafkaStarterUtils.getTopicCreationProps(2));
    printStatus(Color.CYAN, "***** Starting  meetup data stream and publishing to Kafka *****");
    MeetupRsvpStream meetupRSVPProvider = new MeetupRsvpStream(true);
    meetupRSVPProvider.run();
    printStatus(Color.CYAN, "***** Starting Zookeeper, controller, server and broker *****");
    runner.startAll();
    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      try {
        printStatus(Color.GREEN, "***** Shutting down realtime quick start *****");
        runner.stop();
        meetupRSVPProvider.stopPublishing();
        _kafkaStarter.stop();
        ZkStarter.stopLocalZkServer(zookeeperInstance);
        FileUtils.deleteDirectory(quickstartTmpDir);
      } catch (Exception e) {
        e.printStackTrace();
      }
    }));
    printStatus(Color.CYAN, "***** Bootstrap meetupRSVP(upsert) table *****");
    runner.bootstrapTable();
    printStatus(Color.CYAN, "***** Waiting for 15 seconds for a few events to get populated *****");
    Thread.sleep(15000);

    printStatus(Color.YELLOW, "***** Upsert quickstart setup complete *****");

    // The expected behavior for total number of documents per PK should be 1.
    // The expected behavior for total number of rsvp_counts per PK should >=1 since it's incremented and updated.
    // The expected behavior for nums of values in group_name fields should equals to rsvp_counts.
    String q1 =
        "select event_id, count(*), sum(rsvp_count) from meetupRsvp group by event_id order by sum(rsvp_count) desc limit 10";
    printStatus(Color.YELLOW, "Total number of documents, total number of rsvp_counts per event_id in the table");
    printStatus(Color.YELLOW, "***** The expected behavior for total number of documents per PK should be 1 *****");
    printStatus(Color.YELLOW,
        "***** The expected behavior for total number of rsvp_counts per PK should >=1 since it's incremented and updated. *****");
    printStatus(Color.YELLOW,
        "***** The expected behavior for nums of values in group_name fields should equals to rsvp_counts. *****");
    printStatus(Color.CYAN, "Query : " + q1);
    printStatus(Color.YELLOW, prettyPrintResponse(runner.runQuery(q1)));
    printStatus(Color.GREEN, "***************************************************");

    // The expected behavior for nums of values in group_name fields should equals to rsvp_counts.
    String q2 =
        "select event_id, group_name, venue_name, rsvp_count from meetupRsvp where rsvp_count > 1 order by rsvp_count desc limit 10";
    printStatus(Color.YELLOW, "Event_id, group_name, venue_name, rsvp_count per per event_id in the table");
    printStatus(Color.YELLOW,
        "***** Nums of values in group_name fields should less than or equals to rsvp_count. Duplicate records are not allowed. *****");
    printStatus(Color.YELLOW,
        "***** Nums of values in renue_name fields should equals to rsvp_count. Duplicates are allowed. *****");
    printStatus(Color.CYAN, "Query : " + q2);
    printStatus(Color.YELLOW, prettyPrintResponse(runner.runQuery(q2)));
    printStatus(Color.GREEN, "***************************************************");

    printStatus(Color.GREEN, "You can always go to http://localhost:9000 to play around in the query console");
  }
}
