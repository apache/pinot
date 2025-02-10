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
package org.apache.pinot.client.examples;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;


public class PinotJdbcExample {

  private PinotJdbcExample() {
  }

  public static void testPinotJdbcForQuickStart(String jdbcUrl, final String query) {
    try (Connection connection = DriverManager.getConnection(jdbcUrl);
        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery(query)) {

      System.out.println("Connected to Apache Pinot with JDBC Url: " + jdbcUrl);
      System.out.println("Running query: " + query);

      // Print results
      ResultSetMetaData metaData = resultSet.getMetaData();
      int columnCount = metaData.getColumnCount();

      while (resultSet.next()) {
        for (int i = 1; i <= columnCount; i++) {
          System.out.print(metaData.getColumnName(i) + ": " + resultSet.getString(i) + "\t");
        }
        System.out.println();
      }
    } catch (SQLException e) {
      e.printStackTrace();
    }
  }

  public static void main(String[] args) {
    String query = "SELECT count(*) FROM airlineStats";
    testPinotJdbcForQuickStart("jdbc:pinot://localhost:9000", query);
    testPinotJdbcForQuickStart("jdbc:pinotgrpc://localhost:9000?brokers=localhost:8010", query);
  }
}
