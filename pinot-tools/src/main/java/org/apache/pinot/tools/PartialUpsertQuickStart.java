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

import java.util.Arrays;
import java.util.List;
import org.apache.pinot.spi.plugin.PluginManager;
import org.apache.pinot.tools.Quickstart.Color;
import org.apache.pinot.tools.admin.command.QuickstartRunner;


public class PartialUpsertQuickStart extends RealtimeQuickStart {

  public static void main(String[] args)
      throws Exception {
    PluginManager.get().init();
    new PartialUpsertQuickStart().execute();
  }

  @Override
  public void runSampleQueries(QuickstartRunner runner)
      throws Exception {
    // The expected behavior for total number of documents per PK should be 1.
    // The expected behavior for total number of rsvp_counts per PK should >=1 since it's incremented and updated.

    // The expected behavior for nums of values in group_name fields should equals to rsvp_counts.
    String q1 =
        "select event_id, count(*), sum(rsvp_count) from upsertPartialMeetupRsvp group by event_id order by sum"
            + "(rsvp_count) desc limit 10";
    printStatus(Color.YELLOW, "Total number of documents, total number of rsvp_counts per event_id in the table");
    printStatus(Color.YELLOW, "***** The expected behavior for total number of documents per PK should be 1 *****");
    printStatus(Color.YELLOW,
        "***** The expected behavior for total number of rsvp_counts per PK should >=1 since it's incremented and "
            + "updated. *****");
    printStatus(Color.YELLOW,
        "***** The expected behavior for nums of values in group_name fields should equals to rsvp_counts. *****");
    printStatus(Color.CYAN, "Query : " + q1);
    printStatus(Color.YELLOW, prettyPrintResponse(runner.runQuery(q1)));
    printStatus(Color.GREEN, "***************************************************");

    // The expected behavior for nums of values in group_name fields should equals to rsvp_counts.
    String q2 =
        "select event_id, group_name, venue_name, rsvp_count from upsertPartialMeetupRsvp where rsvp_count > 1 order "
            + "by rsvp_count desc limit 10";
    printStatus(Color.YELLOW, "Event_id, group_name, venue_name, rsvp_count per per event_id in the table");
    printStatus(Color.YELLOW,
        "***** Nums of values in group_name fields should less than or equals to rsvp_count. Duplicate records are "
            + "not allowed. *****");
    printStatus(Color.YELLOW,
        "***** Nums of values in renue_name fields should equals to rsvp_count. Duplicates are allowed. *****");
    printStatus(Color.CYAN, "Query : " + q2);
    printStatus(Color.YELLOW, prettyPrintResponse(runner.runQuery(q2)));
    printStatus(Color.GREEN, "***************************************************");
  }

  @Override
  public List<String> types() {
    return Arrays.asList("PARTIAL-UPSERT", "PARTIAL_UPSERT");
  }
}
