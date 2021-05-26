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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.common.utils.ZkStarter;
import org.apache.pinot.spi.stream.StreamDataProvider;
import org.apache.pinot.spi.stream.StreamDataServerStartable;
import org.apache.pinot.tools.Quickstart.Color;
import org.apache.pinot.tools.admin.PinotAdministrator;
import org.apache.pinot.tools.admin.command.QuickstartRunner;
import org.apache.pinot.tools.streams.MeetupRsvpStream;
import org.apache.pinot.tools.utils.KafkaStarterUtils;

import static org.apache.pinot.tools.Quickstart.prettyPrintResponse;
import static org.apache.pinot.tools.Quickstart.printStatus;


public class UpsertQuickStart extends QuickStartBase {
  private StreamDataServerStartable _kafkaStarter;

  public static void main(String[] args)
      throws Exception {
    List<String> arguments = new ArrayList<>();
    arguments.addAll(Arrays.asList("QuickStart", "-type", "UPSERT"));
    arguments.addAll(Arrays.asList(args));
    PinotAdministrator.main(arguments.toArray(new String[arguments.size()]));
  }

  public void execute()
      throws Exception {
    File quickstartTmpDir = new File(_tmpDir, String.valueOf(System.currentTimeMillis()));
    File bootstrapTableDir = new File(quickstartTmpDir, "meetupRsvp");
    File dataDir = new File(bootstrapTableDir, "data");
    Preconditions.checkState(dataDir.mkdirs());

    File schemaFile = new File(bootstrapTableDir, "meetupRsvp_schema.json");
    File tableConfigFile = new File(bootstrapTableDir, "meetupRsvp_realtime_table_config.json");

    ClassLoader classLoader = Quickstart.class.getClassLoader();
    URL resource = classLoader.getResource("examples/stream/meetupRsvp/upsert_meetupRsvp_schema.json");
    com.google.common.base.Preconditions.checkNotNull(resource);
    FileUtils.copyURLToFile(resource, schemaFile);
    resource = classLoader.getResource("examples/stream/meetupRsvp/upsert_meetupRsvp_realtime_table_config.json");
    com.google.common.base.Preconditions.checkNotNull(resource);
    FileUtils.copyURLToFile(resource, tableConfigFile);

    QuickstartTableRequest request = new QuickstartTableRequest(bootstrapTableDir.getAbsolutePath());
    final QuickstartRunner runner = new QuickstartRunner(Lists.newArrayList(request), 1, 1, 1, dataDir);

    printStatus(Color.CYAN, "***** Starting Kafka *****");
    final ZkStarter.ZookeeperInstance zookeeperInstance = ZkStarter.startLocalZkServer();
    try {
      _kafkaStarter = StreamDataProvider.getServerDataStartable(KafkaStarterUtils.KAFKA_SERVER_STARTABLE_CLASS_NAME,
          KafkaStarterUtils.getDefaultKafkaConfiguration(zookeeperInstance));
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
    printStatus(Color.CYAN, "***** Waiting for 5 seconds for a few events to get populated *****");
    Thread.sleep(5000);

    printStatus(Color.YELLOW, "***** Upsert quickstart setup complete *****");

    String q1 = "select event_id, count(*) from meetupRsvp group by event_id limit 10";
    printStatus(Color.YELLOW, "Total number of documents per event_id in the table");
    printStatus(Color.CYAN, "Query : " + q1);
    printStatus(Color.YELLOW, prettyPrintResponse(runner.runQuery(q1)));
    printStatus(Color.GREEN, "***************************************************");

    printStatus(Color.GREEN, "You can always go to http://localhost:9000 to play around in the query console");
  }
}
