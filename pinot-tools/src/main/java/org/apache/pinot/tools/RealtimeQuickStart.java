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
import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.tools.Quickstart.Color;
import org.apache.pinot.tools.admin.PinotAdministrator;
import org.apache.pinot.tools.admin.command.QuickstartRunner;


/// The basic Realtime/Streaming Quickstart.
///
/// This quickstart bootstraps every stream example table and demonstrates the stream-side features that used to
/// have a dedicated quickstart each: JSON indexes, complex type handling, full upsert, partial upsert and upsert
/// with a JSON index. The types those quickstarts used to own are kept as deprecated aliases.
public class RealtimeQuickStart extends QuickStartBase {
  private static final List<String> DEPRECATED_TYPES =
      List.of("UPSERT", "UPSERT_JSON_INDEX", "UPSERT-JSON-INDEX", "PARTIAL_UPSERT", "PARTIAL-UPSERT",
          "REALTIME_JSON_INDEX", "REALTIME-JSON-INDEX", "STREAM_JSON_INDEX", "STREAM-JSON-INDEX",
          "REALTIME_COMPLEX_TYPE", "REALTIME-COMPLEX-TYPE", "STREAM_COMPLEX_TYPE", "STREAM-COMPLEX-TYPE");

  @Override
  public List<String> types() {
    List<String> types = new ArrayList<>(List.of("REALTIME", "STREAM"));
    types.addAll(DEPRECATED_TYPES);
    return types;
  }

  @Override
  public List<String> deprecatedTypes() {
    return DEPRECATED_TYPES;
  }

  public static void main(String[] args)
      throws Exception {
    List<String> arguments = new ArrayList<>();
    arguments.addAll(Arrays.asList("QuickStart", "-type", "REALTIME"));
    arguments.addAll(Arrays.asList(args));
    PinotAdministrator.main(arguments.toArray(new String[arguments.size()]));
  }

  @Override
  protected Map<String, Object> getConfigOverrides() {
    Map<String, Object> configOverrides = new HashMap<>(super.getConfigOverrides());
    configOverrides.put(CommonConstants.Server.CONFIG_OF_ENABLE_THREAD_CPU_TIME_MEASUREMENT, true);
    return configOverrides;
  }

  @Override
  public void runSampleQueries(QuickstartRunner runner)
      throws Exception {
    runMeetupRsvpQueries(runner);
    runJsonIndexQueries(runner);
    runComplexTypeQueries(runner);
    runUpsertQueries(runner);
    runUpsertJsonIndexQueries(runner);
    runPartialUpsertQueries(runner);
    runFineFoodReviewsQueries(runner);
    if (hasTables(runner, "fineFoodReviews")) {
      runVectorQueryExamples(runner);
    }
  }

  private void runMeetupRsvpQueries(QuickstartRunner runner)
      throws Exception {
    if (!hasTables(runner, "meetupRsvp")) {
      return;
    }
    printStatus(Color.YELLOW, "***** Meetup RSVPs *****");
    runAndPrintQuery(runner, "Total number of documents in the table", "select count(*) from meetupRsvp limit 1");
    runAndPrintQuery(runner, "Top 10 cities with the most rsvp", "select group_city, sum(rsvp_count) from meetupRsvp "
        + "group by group_city order by sum(rsvp_count) desc limit 10");
    runAndPrintQuery(runner, "Show 10 most recent rsvps", "select * from meetupRsvp order by mtime limit 10");
    runAndPrintQuery(runner, "Show top 10 rsvp'ed events", "select event_name, sum(rsvp_count) from meetupRsvp "
        + "group by event_name order by sum(rsvp_count) desc limit 10");
  }

  private void runJsonIndexQueries(QuickstartRunner runner)
      throws Exception {
    if (!hasTables(runner, "meetupRsvpJson")) {
      return;
    }
    printStatus(Color.YELLOW, "***** JSON index *****");
    runAndPrintQuery(runner, "Events related to topic_name0",
        "select json_extract_scalar(event_json, '$.event_name', 'STRING') from meetupRsvpJson where json_match"
            + "(group_json, '\"$.group_topics[*].topic_name\"=''topic_name0''') limit 10");
  }

  private void runComplexTypeQueries(QuickstartRunner runner)
      throws Exception {
    if (!hasTables(runner, "meetupRsvpComplexType")) {
      return;
    }
    printStatus(Color.YELLOW, "***** Complex type handling *****");
    runAndPrintQuery(runner, "Flattened group topics",
        "select \"group.group_topics.urlkey\", \"group.group_topics.topic_name\", \"group.group_id\" from "
            + "meetupRsvpComplexType limit 10");
  }

  private void runUpsertQueries(QuickstartRunner runner)
      throws Exception {
    if (!hasTables(runner, "upsertMeetupRsvp")) {
      return;
    }
    printStatus(Color.YELLOW, "***** Upsert *****");
    printStatus(Color.YELLOW, "***** The expected number of documents per event_id is 1 *****");
    runAndPrintQuery(runner, "Total number of documents per event_id in the table",
        "select event_id, count(*) from upsertMeetupRsvp group by event_id limit 10");
  }

  private void runUpsertJsonIndexQueries(QuickstartRunner runner)
      throws Exception {
    if (!hasTables(runner, "upsertJsonMeetupRsvp")) {
      return;
    }
    printStatus(Color.YELLOW, "***** Upsert with JSON index *****");
    runAndPrintQuery(runner, "Events related to topic_name0",
        "select json_extract_scalar(event_json, '$.event_name', 'STRING') from upsertJsonMeetupRsvp where json_match"
            + "(group_json, '\"$.group_topics[*].topic_name\"=''topic_name0''') limit 10");
  }

  private void runPartialUpsertQueries(QuickstartRunner runner)
      throws Exception {
    if (!hasTables(runner, "upsertPartialMeetupRsvp")) {
      return;
    }
    // The expected behavior for total number of documents per PK should be 1.
    // The expected behavior for total number of rsvp_counts per PK should >=1 since it's incremented and updated.
    // The expected behavior for nums of values in group_name fields should equals to rsvp_counts.
    printStatus(Color.YELLOW, "***** Partial upsert *****");
    printStatus(Color.YELLOW, "***** The expected behavior for total number of documents per PK should be 1 *****");
    printStatus(Color.YELLOW,
        "***** The expected behavior for total number of rsvp_counts per PK should >=1 since it's incremented and "
            + "updated. *****");
    printStatus(Color.YELLOW,
        "***** The expected behavior for nums of values in group_name fields should equals to rsvp_counts. *****");
    runAndPrintQuery(runner, "Total number of documents, total number of rsvp_counts per event_id in the table",
        "select event_id, count(*), sum(rsvp_count) from upsertPartialMeetupRsvp group by event_id order by sum"
            + "(rsvp_count) desc limit 10");

    printStatus(Color.YELLOW,
        "***** Nums of values in group_name fields should less than or equals to rsvp_count. Duplicate records are "
            + "not allowed. *****");
    printStatus(Color.YELLOW,
        "***** Nums of values in venue_name fields should equals to rsvp_count. Duplicates are allowed. *****");
    runAndPrintQuery(runner, "Event_id, group_name, venue_name, rsvp_count per event_id in the table",
        "select event_id, group_name, venue_name, rsvp_count from upsertPartialMeetupRsvp where rsvp_count > 1 order "
            + "by rsvp_count desc limit 10");
  }

  private void runFineFoodReviewsQueries(QuickstartRunner runner)
      throws Exception {
    if (!hasTables(runner, "fineFoodReviews", "fineFoodReviews_part_0", "fineFoodReviews_part_1")) {
      return;
    }
    printStatus(Color.YELLOW, "***** Fine food reviews *****");
    runAndPrintQuery(runner, "Total number of documents in fineFoodReviews", "select count(*) from fineFoodReviews");
    runAndPrintQuery(runner, "Total number of documents in fineFoodReviews-federated",
        "select count(*) from \"fineFoodReviews-federated\"");
    runAndPrintQuery(runner, "Total number of documents in fineFoodReviews_part_0",
        "select count(*) from \"fineFoodReviews_part_0\"");
    runAndPrintQuery(runner, "Total number of documents in fineFoodReviews_part_1",
        "select count(*) from \"fineFoodReviews_part_1\"");
  }

  public void execute()
      throws Exception {
    File quickstartTmpDir =
        _setCustomDataDir ? _dataDir : new File(_dataDir, String.valueOf(System.currentTimeMillis()));
    File quickstartRunnerDir = new File(quickstartTmpDir, "quickstart");
    Preconditions.checkState(quickstartRunnerDir.mkdirs());
    List<QuickstartTableRequest> quickstartTableRequests = bootstrapStreamTableDirectories(quickstartTmpDir);
    final QuickstartRunner runner =
        new QuickstartRunner(quickstartTableRequests, 1, 1, 4, 1, quickstartRunnerDir, getConfigOverrides());

    startKafka();
    startAllDataStreams(_kafkaStarter, quickstartTmpDir);

    printStatus(Color.CYAN, "***** Starting Zookeeper, controller, broker, server and minion *****");
    runner.startAll();
    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      try {
        printStatus(Color.GREEN, "***** Shutting down realtime quick start *****");
        runner.stop();
        FileUtils.deleteDirectory(quickstartTmpDir);
      } catch (Exception e) {
        e.printStackTrace();
      }
    }));

    printStatus(Color.CYAN, "***** Bootstrap all tables *****");
    runner.bootstrapTable();
    createFineFoodReviewsFederatedTable();

    printStatus(Color.CYAN, "***** Waiting for 5 seconds for a few events to get populated *****");
    Thread.sleep(5000);

    printStatus(Color.YELLOW, "***** Realtime quickstart setup complete *****");
    runSampleQueries(runner);

    printStatus(Color.GREEN,
        String.format("You can always go to http://localhost:%d to play around in the query console",
            QuickstartRunner.DEFAULT_CONTROLLER_PORT));
  }
}
