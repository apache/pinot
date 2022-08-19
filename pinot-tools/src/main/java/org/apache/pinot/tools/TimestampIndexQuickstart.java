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
import org.apache.pinot.tools.admin.PinotAdministrator;
import org.apache.pinot.tools.admin.command.QuickstartRunner;


public class TimestampIndexQuickstart extends Quickstart {
  @Override
  public List<String> types() {
    return Collections.singletonList("TIMESTAMP");
  }

  @Override
  public void runSampleQueries(QuickstartRunner runner)
      throws Exception {
    printStatus(Color.YELLOW, "***** Pinot Timestamp with timestamp table setup is complete *****");
    String q1 = "select ts, $ts$DAY, $ts$WEEK, $ts$MONTH from airlineStats limit 1";
    printStatus(Color.YELLOW, "Pick one row with timestamp and different granularity using generated column name ");
    printStatus(Color.CYAN, "Query : " + q1);
    printStatus(Color.YELLOW, prettyPrintResponse(runner.runQuery(q1)));
    printStatus(Color.GREEN, "***************************************************");

    String q2 =
        "select ts, dateTrunc('DAY', ts), dateTrunc('WEEK', ts), dateTrunc('MONTH', ts) from airlineStats limit 1";
    printStatus(Color.YELLOW, "Pick one row with timestamp and different granularity using dateTrunc function");
    printStatus(Color.CYAN, "Query : " + q2);
    printStatus(Color.YELLOW, prettyPrintResponse(runner.runQuery(q2)));
    printStatus(Color.GREEN, "***************************************************");

    String q3 =
        "select count(*), toTimestamp(dateTrunc('WEEK', ts)) as tsWeek from airlineStats GROUP BY tsWeek limit 1";
    printStatus(Color.YELLOW, "Count events in week basis ");
    printStatus(Color.CYAN, "Query : " + q3);
    printStatus(Color.YELLOW, prettyPrintResponse(runner.runQuery(q3)));
    printStatus(Color.GREEN, "***************************************************");
  }

  public static void main(String[] args)
      throws Exception {
    List<String> arguments = new ArrayList<>();
    arguments.addAll(Arrays.asList("QuickStart", "-type", "TIMESTAMP"));
    arguments.addAll(Arrays.asList(args));
    PinotAdministrator.main(arguments.toArray(new String[arguments.size()]));
  }
}
