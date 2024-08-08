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
package org.apache.pinot.tools.utils;

public class SampleQueries {
  private SampleQueries() {
  }

  public final static String COUNT_BASEBALL_STATS = "SELECT count(*) FROM baseballStats_OFFLINE LIMIT 10";
  public final static String BASEBALL_STATS_SELF_JOIN = "SELECT a.playerID, a.runs, a.yearID, b.runs, b.yearID"
      + " FROM baseballStats_OFFLINE AS a JOIN baseballStats_OFFLINE AS b ON a.playerID = b.playerID"
      + " WHERE a.runs > 160 AND b.runs < 2 LIMIT 10";
  public final static String BASEBALL_JOIN_DIM_BASEBALL_TEAMS =
      "SELECT a.playerName, a.teamID, b.teamName \n" + "FROM baseballStats_OFFLINE AS a\n"
          + "JOIN dimBaseballTeams_OFFLINE AS b\n" + "ON a.teamID = b.teamID LIMIT 10";
  public final static String BASEBALL_SUM_RUNS_Q1 =
      "select playerName, sum(runs) from baseballStats group by playerName order by sum(runs) desc limit 5";
  public final static String BASEBALL_SUM_RUNS_Q2 =
      "select playerName, sum(runs) from baseballStats where yearID=2000 group by playerName order by sum(runs) "
          + "desc limit 5";
  public final static String BASEBALL_SUM_RUNS_Q3 =
      "select playerName, sum(runs) from baseballStats where yearID>=2000 group by playerName order by sum(runs) "
          + "desc limit 10";
  public final static String BASEBALL_ORDER_BY_YEAR =
      "select playerName, runs, homeRuns from baseballStats order by yearID limit 10";
}
