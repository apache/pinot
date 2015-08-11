/**
 * Copyright (C) 2014-2015 LinkedIn Corp. (pinot-core@linkedin.com)
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

import static com.linkedin.pinot.tools.Quickstart.prettyprintResponse;
import static com.linkedin.pinot.tools.Quickstart.printStatus;

import java.io.File;
import java.net.URISyntaxException;

import org.apache.commons.io.FileUtils;
import org.json.JSONException;

import kafka.server.KafkaServerStartable;

import com.linkedin.pinot.common.utils.KafkaStarterUtils;
import com.linkedin.pinot.common.utils.ZkStarter;
import com.linkedin.pinot.tools.Quickstart.color;
import com.linkedin.pinot.tools.admin.command.QuickstartRunner;
import com.linkedin.pinot.tools.streams.MeetupRsvpStream;


public class RealtimeQuickStart {
  private File _quickStartDataDir;

  public void  execute() throws JSONException, Exception {
      _quickStartDataDir = new File("quickStartData" + System.currentTimeMillis());
      String quickStartDataDirName = _quickStartDataDir.getName();

      if (!_quickStartDataDir.exists()) {
        _quickStartDataDir.mkdir();
      }

    File schema = new File(quickStartDataDirName + "/rsvp_pinot_schema.json");
    File tableCreate = new File(quickStartDataDirName + "/rsvp_create_table_request.json");

    FileUtils.copyURLToFile(RealtimeQuickStart.class.getClassLoader().
        getResource("sample_data/rsvp_pinot_schema.json"), schema);

    FileUtils.copyURLToFile(RealtimeQuickStart.class.getClassLoader().
        getResource("sample_data/rsvp_create_table_request.json"), tableCreate);

    printStatus(color.CYAN, "Starting Kafka");

    ZkStarter.startLocalZkServer();

    final KafkaServerStartable kafkaStarter =
        KafkaStarterUtils.startServer(KafkaStarterUtils.DEFAULT_KAFKA_PORT, KafkaStarterUtils.DEFAULT_BROKER_ID,
            KafkaStarterUtils.DEFAULT_ZK_STR, KafkaStarterUtils.getDefaultKafkaConfiguration());

    KafkaStarterUtils.createTopic("meetupRSVPEvents", KafkaStarterUtils.DEFAULT_ZK_STR);

    final QuickstartRunner runner =
        new QuickstartRunner(schema, null, new File("/tm/" + System.currentTimeMillis()), "meetupRsvp",
            tableCreate.getAbsolutePath());

    runner.startAll();

    printStatus(color.CYAN, "Starting controller, server and broker");

    runner.addSchema();
    runner.addTable();
    printStatus(color.CYAN, "Added schema and table");

    printStatus(color.YELLOW, "Realtime quickstart setup complete");

    final MeetupRsvpStream meetupRSVPProvider = new MeetupRsvpStream(schema);
    meetupRSVPProvider.run();
    printStatus(color.CYAN, "Starting meetup data stream and publishing to kafka");

    // lets wait for a few events to get populated
    Thread.sleep(5000);

    String q1 = "select count(*) from meetupRsvp limit 0";
    printStatus(color.YELLOW, "Total number of documents in the table");
    printStatus(color.CYAN, "Query : " + q1);
    printStatus(color.YELLOW, prettyprintResponse(runner.runQuery(q1)));
    printStatus(color.GREEN, "***************************************************");

    String q2 = "select sum(rsvp_count) from meetupRsvp group by group_city top 10 limit 0";
    printStatus(color.YELLOW, "Top 10 cities with the most rsvp");
    printStatus(color.CYAN, "Query : " + q2);
    printStatus(color.YELLOW, prettyprintResponse(runner.runQuery(q2)));
    printStatus(color.GREEN, "***************************************************");

    String q3 = "select * from meetupRsvp order by mtime limit 10";
    printStatus(color.YELLOW, "Show 10 most recent rsvps");
    printStatus(color.CYAN, "Query : " + q3);
    printStatus(color.YELLOW, prettyprintResponse(runner.runQuery(q3)));
    printStatus(color.GREEN, "***************************************************");

    String q4 = "select sum(rsvp_count) from meetupRsvp group by event_name top 10 limit 0";
    printStatus(color.YELLOW, "Show top 10 rsvp'ed events");
    printStatus(color.CYAN, "Query : " + q4);
    printStatus(color.YELLOW, prettyprintResponse(runner.runQuery(q4)));
    printStatus(color.GREEN, "***************************************************");

    String q5 = "select count(*) from meetupRsvp limit 0";
    printStatus(color.YELLOW, "Total number of documents in the table");
    printStatus(color.CYAN, "Query : " + q5);
    printStatus(color.YELLOW, prettyprintResponse(runner.runQuery(q5)));
    printStatus(color.GREEN, "***************************************************");

    printStatus(color.GREEN,
        "you can always go to http://localhost:9000/query/ to play around in the query console");

    Runtime.getRuntime().addShutdownHook(new Thread() {
      @Override
      public void run() {
        try {
          printStatus(color.GREEN, "***** shutting down realtime quickstart *****");
          meetupRSVPProvider.stopPublishing();
          FileUtils.deleteDirectory(_quickStartDataDir);
          runner.stop();
          runner.clean();
          KafkaStarterUtils.stopServer(kafkaStarter);
          ZkStarter.stopLocalZkServer();
        } catch (Exception e) {
          e.printStackTrace();
        }
      }
    });

    long st = System.currentTimeMillis();
    while (true) {
      if (System.currentTimeMillis() - st >= (60 * 60) * 1000) {
        break;
      }
    }

    printStatus(color.YELLOW, "running since an hour, stopping now");

  }

    public static void main(String[] args) {
      RealtimeQuickStart rst = new RealtimeQuickStart();
      try {
        rst.execute();
      } catch (Exception e) {
        System.out.println(e.getMessage());
        e.printStackTrace();
      }
    }

}
