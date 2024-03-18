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

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.HashSet;
import java.util.Set;
import org.testng.Assert;
import org.testng.annotations.Test;


public class PinotDriverTest {
  static final String DB_URL = "jdbc:pinot://localhost:8000?controller=localhost:9000";
  static final String BAD_URL = "jdbc:someOtherDB://localhost:8000?controller=localhost:9000";
  static final String GOOD_URL_NO_CONNECTION = "jdbc:pinot://localhost:1111?controller=localhost:2222";

  @Test(enabled = false)
  public void testDriver()
      throws Exception {
    Connection conn = DriverManager.getConnection(DB_URL);
    Statement statement = conn.createStatement();
    Integer limitResults = 10;
    ResultSet rs = statement
        .executeQuery(String.format("SELECT UPPER(playerName) AS name FROM baseballStats LIMIT %d", limitResults));
    Set<String> results = new HashSet<>();
    Integer resultCount = 0;
    while (rs.next()) {
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
    statement.close();
    ;
    conn.close();
  }

  @Test
  public void testDriverBadURL() {
    try {
      DriverManager.getConnection(BAD_URL);
    } catch (Exception e) {
      Assert.assertTrue(e.getMessage().contains("No suitable driver found"));
    }
  }

  @Test
  public void testDriverGoodURLNoConnection() {
    try {
      DriverManager.getConnection(GOOD_URL_NO_CONNECTION);
    } catch (Exception e) {
      Assert.assertTrue(e.getMessage().contains("Failed to connect to url"));
    }
  }
}
