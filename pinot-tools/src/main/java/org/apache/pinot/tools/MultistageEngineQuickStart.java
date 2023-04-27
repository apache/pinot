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
import java.util.List;
import java.util.Map;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.tools.admin.PinotAdministrator;
import org.apache.pinot.tools.admin.command.QuickstartRunner;


public class MultistageEngineQuickStart extends Quickstart {
  private static final String[] MULTI_STAGE_TABLE_DIRECTORIES = new String[]{
      "examples/batch/airlineStats",
      "examples/batch/baseballStats",
      "examples/batch/billing",
      "examples/batch/dimBaseballTeams",
      "examples/batch/githubEvents",
      "examples/batch/githubComplexTypeEvents",
      "examples/batch/ssb/customer",
      "examples/batch/ssb/dates",
      "examples/batch/ssb/lineorder",
      "examples/batch/ssb/part",
      "examples/batch/ssb/supplier",
      "examples/batch/starbucksStores",
  };

  @Override
  public List<String> types() {
    return Collections.singletonList("MULTI_STAGE");
  }

  @Override
  public void runSampleQueries(QuickstartRunner runner)
      throws Exception {

    printStatus(Quickstart.Color.YELLOW, "***** Multi-stage engine quickstart setup complete *****");
    Map<String, String> queryOptions = Collections.singletonMap("queryOptions",
        CommonConstants.Broker.Request.QueryOptionKey.USE_MULTISTAGE_ENGINE + "=true");
    String q1 = "SELECT count(*) FROM baseballStats_OFFLINE LIMIT 10";
    printStatus(Quickstart.Color.YELLOW, "Total number of documents in the table");
    printStatus(Quickstart.Color.CYAN, "Query : " + q1);
    printStatus(Quickstart.Color.YELLOW, prettyPrintResponse(runner.runQuery(q1, queryOptions)));
    printStatus(Quickstart.Color.GREEN, "***************************************************");

    String q2 = "SELECT a.playerID, a.runs, a.yearID, b.runs, b.yearID"
        + " FROM baseballStats_OFFLINE AS a JOIN baseballStats_OFFLINE AS b ON a.playerID = b.playerID"
        + " WHERE a.runs > 160 AND b.runs < 2 LIMIT 10";
    printStatus(Quickstart.Color.YELLOW, "Correlate the same player(s) with more than 160-run some year(s) and"
        + " with less than 2-run some other year(s)");
    printStatus(Quickstart.Color.CYAN, "Query : " + q2);
    printStatus(Quickstart.Color.YELLOW, prettyPrintResponse(runner.runQuery(q2, queryOptions)));
    printStatus(Quickstart.Color.GREEN, "***************************************************");

    String q3 = "SELECT a.playerName, a.teamID, b.teamName \n"
        + "FROM baseballStats_OFFLINE AS a\n"
        + "JOIN dimBaseballTeams_OFFLINE AS b\n"
        + "ON a.teamID = b.teamID LIMIT 10";
    printStatus(Quickstart.Color.YELLOW, "Baseball Stats with joined team names");
    printStatus(Quickstart.Color.CYAN, "Query : " + q3);
    printStatus(Quickstart.Color.YELLOW, prettyPrintResponse(runner.runQuery(q3, queryOptions)));
    printStatus(Quickstart.Color.GREEN, "***************************************************");

    printStatus(Quickstart.Color.GREEN, "***************************************************");
    printStatus(Quickstart.Color.YELLOW, "Example query run completed.");
    printStatus(Quickstart.Color.GREEN, "***************************************************");
  }

  @Override
  public String[] getDefaultBatchTableDirectories() {
    return MULTI_STAGE_TABLE_DIRECTORIES;
  }

  @Override
  protected int getNumQuickstartRunnerServers() {
    return 3;
  }

  public static void main(String[] args)
      throws Exception {
    List<String> arguments = new ArrayList<>();
    arguments.addAll(Arrays.asList("QuickStart", "-type", "MULTI_STAGE"));
    arguments.addAll(Arrays.asList(args));
    PinotAdministrator.main(arguments.toArray(new String[arguments.size()]));
  }
}
