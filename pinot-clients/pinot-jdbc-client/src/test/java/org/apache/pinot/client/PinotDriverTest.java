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
package org.apache.pinot.client;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.HashSet;
import java.util.Set;
import org.testng.Assert;
import org.testng.annotations.Test;


public class PinotDriverTest {
  static final String DB_URL = "jdbc:pinot://localhost:8000/";

  @Test(enabled = false)
  public void testDriver() throws Exception {
    Connection conn = DriverManager.getConnection(DB_URL);
    Statement statement = conn.createStatement();
    Integer limitResults = 10;
    ResultSet rs = statement.executeQuery(String.format("SELECT UPPER(playerName) AS name FROM baseballStats LIMIT %d", limitResults));
    Set<String> results = new HashSet<>();
    Integer resultCount = 0;
    while(rs.next()){
      String playerName = rs.getString("name");
      results.add(playerName);
      resultCount++;
    }

    Set<String> expectedResults = new HashSet<>();
    expectedResults.add("HENRY LOUIS");
    expectedResults.add("DAVID ALLAN");

    Assert.assertEquals(resultCount, limitResults);
    Assert.assertEquals(results, expectedResults);

    rs.close();
    statement.close();;
    conn.close();
  }

  public static void main(String[] args) throws Exception {
    String query  = "SELECT baseballStats.AtBatting AS AtBatting,\n" + "  baseballStats.G_old AS G_old,\n"
        + "  baseballStats.baseOnBalls AS baseOnBalls,\n" + "  baseballStats.caughtStealing AS caughtStealing,\n"
        + "  baseballStats.doules AS doules,\n"
        + "  baseballStats.groundedIntoDoublePlays AS groundedIntoDoublePlays,\n" + "  baseballStats.hits AS hits,\n"
        + "  baseballStats.hitsByPitch AS hitsByPitch,\n" + "  baseballStats.homeRuns AS homeRuns,\n"
        + "  baseballStats.intentionalWalks AS intentionalWalks,\n" + "  baseballStats.league AS league,\n"
        + "  baseballStats.numberOfGames AS numberOfGames,\n"
        + "  baseballStats.numberOfGamesAsBatter AS numberOfGamesAsBatter,\n"
        + "  baseballStats.playerID AS playerID,\n" + "  baseballStats.playerName AS playerName,\n"
        + "  baseballStats.playerStint AS playerStint,\n" + "  baseballStats.runs AS runs,\n"
        + "  baseballStats.runsBattedIn AS runsBattedIn,\n" + "  baseballStats.sacrificeFlies AS sacrificeFlies,\n"
        + "  baseballStats.sacrificeHits AS sacrificeHits,\n" + "  baseballStats.stolenBases AS stolenBases,\n"
        + "  baseballStats.strikeouts AS strikeouts,\n" + "  baseballStats.teamID AS teamID,\n"
        + "  baseballStats.tripples AS tripples\n" + "FROM baseballStats\n" + "LIMIT 1000 ";
    System.out.println(query.replaceAll("baseballStats[.]", ""));
    System.out.println(query.replaceAll("baseballStats[.]", "").replaceAll(" AS (.*?)(\n)", ",").replace(",F", " F"));
  }
}
