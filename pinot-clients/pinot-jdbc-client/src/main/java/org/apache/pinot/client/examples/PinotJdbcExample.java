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

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Properties;
import org.apache.pinot.client.ConnectionFactory;
import org.apache.pinot.client.ResultSetGroup;
import org.apache.pinot.client.grpc.GrpcConnection;


public class PinotJdbcExample {

  private PinotJdbcExample() {
  }

  public static void testPinotJdbcForQuickStart(String jdbcUrl, final String query) {
    try (Connection connection = DriverManager.getConnection(jdbcUrl);
        Statement statement = connection.createStatement(); ResultSet resultSet = statement.executeQuery(query)) {

      System.out.println("Connected to Apache Pinot with JDBC Url: " + jdbcUrl);
      System.out.println("Running query: " + query);

      // Print results
      ResultSetMetaData metaData = resultSet.getMetaData();
      int columnCount = metaData.getColumnCount();
      while (resultSet.next()) {
        System.out.print("Row Id: " + resultSet.getRow() + "\t");
        for (int i = 1; i <= columnCount; i++) {
          System.out.print(metaData.getColumnName(i) + ": " + resultSet.getString(i) + "\t");
        }
        System.out.println();
      }
    } catch (SQLException e) {
      e.printStackTrace();
    }
  }

  public static void testQuery(GrpcConnection grpcConnection, String query) {
    try {
      ResultSetGroup resultSetGroup = grpcConnection.execute(query);
      for (int i = 0; i < resultSetGroup.getResultSetCount(); i++) {
        org.apache.pinot.client.ResultSet resultSet = resultSetGroup.getResultSet(i);
        for (int rowId = 0; rowId < resultSet.getRowCount(); rowId++) {
          System.out.print("Row Id: " + rowId + "\t");
          for (int colId = 0; colId < resultSet.getColumnCount(); colId++) {
            System.out.print(resultSet.getString(rowId, colId) + "\t");
          }
          System.out.println();
        }
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
    grpcConnection.close();
  }

  public static void main(String[] args)
      throws IOException {
    Properties properties = new Properties();

    String query;
    // query = "SELECT count(*) FROM airlineStats";
    query = "SELECT * FROM airlineStats limit 1000";
    testPinotJdbcForQuickStart("jdbc:pinot://localhost:9000", query);
    testPinotJdbcForQuickStart("jdbc:pinotgrpc://localhost:9000?blockRowSize=100", query);

    testQuery(ConnectionFactory.fromControllerGrpc(properties, "localhost:9000"), query);
    testQuery(ConnectionFactory.fromZookeeperGrpc(properties, "localhost:2123/QuickStartCluster"), query);
    testQuery(ConnectionFactory.fromHostListGrpc(properties, List.of("localhost:8010")), query);
  }
}
