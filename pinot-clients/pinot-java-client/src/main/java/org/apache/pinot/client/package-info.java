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
/**
 * This package contains the Pinot client API.
 *
 * Connections to Pinot are created using the
 * {@link org.apache.pinot.client.ConnectionFactory} class' utility methods to create connections to a Pinot cluster
 * given a Zookeeper URL, a Java Properties object or a list of broker addresses to connect to.
 *
 * <pre>{@code Connection connection = ConnectionFactory.fromZookeeper
 *     (some-zookeeper-server:2191/zookeeperPath");
 *
 * Connection connection = ConnectionFactory.fromProperties("demo.properties");
 *
 * Connection connection = ConnectionFactory.fromHostList
 *     ("some-server:1234", "some-other-server:1234", ...);}</pre>
 *
 * Queries can be sent directly to the Pinot cluster using the
 * {@link org.apache.pinot.client.Connection#execute(org.apache.pinot.client.Request)} and
 * {@link org.apache.pinot.client.Connection#executeAsync(org.apache.pinot.client.Request)} methods of
 * {@link org.apache.pinot.client.Connection}.
 *
 * <pre>{@code ResultSetGroup resultSetGroup = connection.execute("select * from foo...");
 * Future<ResultSetGroup> futureResultSetGroup = connection.executeAsync
 *     ("select * from foo...");}</pre>
 *
 * Queries can also use a {@link org.apache.pinot.client.PreparedStatement} to escape query parameters:
 *
 * <pre>{@code PreparedStatement statement = connection.prepareStatement
 *     ("select * from foo where a = ?");
 * statement.setString(1, "bar");
 *
 * ResultSetGroup resultSetGroup = statement.execute();
 * Future<ResultSetGroup> futureResultSetGroup = statement.executeAsync();}</pre>
 *
 * In the case of a selection query, results can be obtained with the various <code>get</code> methods in the first
 * {@link org.apache.pinot.client.ResultSet}, obtained through the
 * {@link org.apache.pinot.client.ResultSetGroup#getResultSet(int)} method:
 *
 * <pre>{@code ResultSet resultSet = connection.execute
 *     ("select foo, bar from baz where quux = 'quuux'").getResultSet(0);
 *
 * for(int i = 0; i < resultSet.getRowCount(); ++i) {
 *     System.out.println("foo: " + resultSet.getString(i, 0);
 *     System.out.println("bar: " + resultSet.getInt(i, 1);
 * }
 *
 * resultSet.close();}</pre>
 *
 * In the case where there is an aggregation, each aggregation function is within its own
 * {@link org.apache.pinot.client.ResultSet}:
 *
 * <pre>{@code ResultSetGroup resultSetGroup = connection.execute("select count(*) from foo");
 *
 * ResultSet resultSet = resultSetGroup.getResultSet(0);
 * System.out.println("Number of records: " + resultSet.getInt(0));
 * resultSet.close();}</pre>
 *
 * There can be more than one {@link org.apache.pinot.client.ResultSet}, each of which can contain multiple results
 * grouped by a group key.
 *
 * <pre>{@code ResultSetGroup resultSetGroup = connection.execute
 *     ("select min(foo), max(foo) from bar group by baz");
 *
 * System.out.println("Number of result groups:" +
 *     resultSetGroup.getResultSetCount(); // 2, min(foo) and max(foo)
 *
 * ResultSet minResultSet = resultSetGroup.getResultSet(0);
 * for(int i = 0; i < minResultSet.length(); ++i) {
 *     System.out.println("Minimum foo for " + minResultSet.getGroupKeyString(i, 1) +
 *         ": " + minResultSet.getInt(i));
 * }
 *
 * ResultSet maxResultSet = resultSetGroup.getResultSet(1);
 * for(int i = 0; i < maxResultSet.length(); ++i) {
 *     System.out.println("Maximum foo for " + maxResultSet.getGroupKeyString(i, 1) +
 *         ": " + maxResultSet.getInt(i));
 * }
 *
 * resultSet.close();}</pre>
 */
package org.apache.pinot.client;
