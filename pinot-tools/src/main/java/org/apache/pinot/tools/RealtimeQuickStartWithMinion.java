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

import com.google.common.collect.ImmutableMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.pinot.tools.admin.PinotAdministrator;
import org.apache.pinot.tools.admin.command.QuickstartRunner;


/**
 * This quickstart shows how RealtimeToOfflineSegmentsTask and MergeRollupTask minion
 * tasks continuously optimize segments as data gets ingested into Realtime table.
 */
public class RealtimeQuickStartWithMinion extends HybridQuickstart {
  @Override
  public List<String> types() {
    return Arrays.asList("REALTIME_MINION", "REALTIME-MINION");
  }

  @Override
  public void runSampleQueries(QuickstartRunner runner)
      throws Exception {
    printStatus(Color.YELLOW, "***** Realtime-minion quickstart setup complete *****");

    String q1 = "select count(*) from githubEvents limit 1";
    printStatus(Color.YELLOW, "Current number of documents in the table");
    printStatus(Color.CYAN, "Query : " + q1);
    printStatus(Color.YELLOW, prettyPrintResponse(runner.runQuery(q1)));
    printStatus(Color.GREEN, "***************************************************");

    printStatus(Color.GREEN, "In particular, you will find that OFFLINE table gets segments from REALTIME table;");
    printStatus(Color.GREEN, "and segments in OFFLINE table get merged into larger ones within a few minutes.");
  }

  public static void main(String[] args)
      throws Exception {
    List<String> arguments = new ArrayList<>();
    arguments.addAll(Arrays.asList("QuickStart", "-type", "REALTIME-MINION"));
    arguments.addAll(Arrays.asList(args));
    PinotAdministrator.main(arguments.toArray(new String[arguments.size()]));
  }

  public Map<String, Object> getConfigOverrides() {
    Map<String, Object> properties = new HashMap<>(super.getConfigOverrides());
    properties.putIfAbsent("controller.task.scheduler.enabled", true);
    return properties;
  }

  @Override
  protected String[] getDefaultBatchTableDirectories() {
    return new String[]{"examples/minions/stream/githubEvents"};
  }

  @Override
  protected Map<String, String> getDefaultStreamTableDirectories() {
    return ImmutableMap.<String, String>builder()
        .put("githubEvents", "examples/minions/stream/githubEvents")
        .build();
  }

  @Override
  protected String getValidationTypesToSkip() {
    return "TASK";
  }
}
