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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.pinot.tools.admin.PinotAdministrator;
import org.apache.pinot.tools.admin.command.QuickstartRunner;
import org.apache.pinot.tools.utils.PinotConfigUtils;
import org.apache.pinot.tools.utils.SampleQueries;

public class SingleStageResourceTrackingQuickStart extends Quickstart {
  @Override
  protected Map<String, Object> getConfigOverrides() {
    Map<String, Object> configOverrides = new HashMap<>();
    // Quickstart.getConfigOverrides may return an ImmutableMap.
    configOverrides.putAll(super.getConfigOverrides());
    configOverrides.putAll(PinotConfigUtils.getResourceTrackingConf());
    return configOverrides;
  }

  @Override
  public void runSampleQueries(QuickstartRunner runner)
      throws Exception {
    List<String> queries = getQueries();
    Map<String, String> queryOptions = getQueryOptions();

    printStatus(Color.YELLOW, "***** Running queries for eternity *****");
    ExecutorService service = Executors.newFixedThreadPool(10);

    for (int i = 0; i < 10; i++) {
      service.submit(() -> {
        try {
          while (true) {
            for (String query : queries) {
              runner.runQuery(query, queryOptions);
            }
            Thread.sleep(10);
          }
        } catch (Exception e) {
          printStatus(Color.CYAN, e.getMessage());
        }
      });
    }
  }

  protected List<String> getQueries() {
    return List.of(SampleQueries.COUNT_BASEBALL_STATS,
        SampleQueries.BASEBALL_SUM_RUNS_Q1,
        SampleQueries.BASEBALL_SUM_RUNS_Q2,
        SampleQueries.BASEBALL_SUM_RUNS_Q3,
        SampleQueries.BASEBALL_ORDER_BY_YEAR);
  }

  protected Map<String, String> getQueryOptions() {
    return Collections.emptyMap();
  }

  @Override
  public List<String> types() {
    return Collections.singletonList("SINGLE_STAGE_RESOURCE_TRACKING");
  }

  public static void main(String[] args)
      throws Exception {
    List<String> arguments = new ArrayList<>();
    arguments.addAll(Arrays.asList("QuickStart", "-type", "SINGLE_STAGE_RESOURCE_TRACKING"));
    arguments.addAll(Arrays.asList(args));
    PinotAdministrator.main(arguments.toArray(new String[arguments.size()]));
  }
}
