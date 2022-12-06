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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.pinot.tools.admin.PinotAdministrator;
import org.apache.pinot.tools.admin.command.QuickstartRunner;


/**
 * A Quickstart to demo how multi directory support works and its typical configs.
 */
public class MultiDirQuickstart extends Quickstart {
  @Override
  public List<String> types() {
    return Arrays.asList("OFFLINE_MULTIDIR", "OFFLINE-MULTIDIR", "BATCH_MULTIDIR", "BATCH-MULTIDIR");
  }

  @Override
  protected Map<String, Object> getConfigOverrides() {
    Map<String, Object> properties = new HashMap<>(super.getConfigOverrides());
    properties.put("pinot.server.instance.segment.directory.loader", "tierBased");
    properties.put("controller.segment.relocator.frequencyPeriod", "60s");
    properties.put("controller.segmentRelocator.initialDelayInSeconds", "10");
    properties.put("controller.segmentRelocator.enableLocalTierMigration", "true");
    /*
     * One can also set `dataDir` as part of tierConfigs in TableConfig to overwrite the instance configs (or set as
     * cluster configs), but it's recommended to use instance (or cluster) configs for consistency across tables.
     * "tierBackendProperties": {
     *     "dataDir": "/tmp/multidir_test/hotTier"
     *  }
     */
    properties.put("pinot.server.instance.tierConfigs.tierNames", "hotTier,coldTier");
    properties.put("pinot.server.instance.tierConfigs.hotTier.dataDir", "/tmp/multidir_test/hotTier");
    properties.put("pinot.server.instance.tierConfigs.coldTier.dataDir", "/tmp/multidir_test/coldTier");
    return properties;
  }

  protected String[] getDefaultBatchTableDirectories() {
    return new String[]{"examples/batch/airlineStats"};
  }

  @Override
  public void runSampleQueries(QuickstartRunner runner)
      throws Exception {
    String q1 = "select count(*) from airlineStats limit 1";
    printStatus(Color.YELLOW, "Total number of documents in the table");
    printStatus(Color.CYAN, "Query : " + q1);
    printStatus(Color.YELLOW, prettyPrintResponse(runner.runQuery(q1)));
    printStatus(Color.GREEN, "***************************************************");

    String q2 =
        "select AirlineID, sum(Cancelled) from airlineStats group by AirlineID order by sum(Cancelled) desc limit 5";
    printStatus(Color.YELLOW, "Top 5 airlines in cancellation ");
    printStatus(Color.CYAN, "Query : " + q2);
    printStatus(Color.YELLOW, prettyPrintResponse(runner.runQuery(q2)));
    printStatus(Color.GREEN, "***************************************************");

    String q3 =
        "select AirlineID, Year, sum(Flights) from airlineStats where Year > 2010 group by AirlineID, Year order by "
            + "sum(Flights) desc limit 5";
    printStatus(Color.YELLOW, "Top 5 airlines in number of flights after 2010");
    printStatus(Color.CYAN, "Query : " + q3);
    printStatus(Color.YELLOW, prettyPrintResponse(runner.runQuery(q3)));
    printStatus(Color.GREEN, "***************************************************");

    String q4 =
        "select OriginCityName, max(Flights) from airlineStats group by OriginCityName order by max(Flights) desc "
            + "limit 5";
    printStatus(Color.YELLOW, "Top 5 cities for number of flights");
    printStatus(Color.CYAN, "Query : " + q4);
    printStatus(Color.YELLOW, prettyPrintResponse(runner.runQuery(q4)));
    printStatus(Color.GREEN, "***************************************************");

    String q5 = "select AirlineID, OriginCityName, DestCityName, Year from airlineStats order by Year limit 5";
    printStatus(Color.YELLOW, "Print AirlineID, OriginCityName, DestCityName, Year for 5 records ordered by Year");
    printStatus(Color.CYAN, "Query : " + q5);
    printStatus(Color.YELLOW, prettyPrintResponse(runner.runQuery(q5)));
    printStatus(Color.GREEN, "***************************************************");
  }

  public static void main(String[] args)
      throws Exception {
    List<String> arguments = new ArrayList<>();
    arguments.addAll(Arrays.asList("QuickStart", "-type", "BATCH-MULTIDIR"));
    arguments.addAll(Arrays.asList(args));
    PinotAdministrator.main(arguments.toArray(new String[arguments.size()]));
  }
}
