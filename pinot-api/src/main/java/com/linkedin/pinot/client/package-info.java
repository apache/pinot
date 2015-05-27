/**
 * Copyright (C) 2014-2015 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
/**
 * This package contains the Pinot client API.
 *
 * Connections to Pinot are created using the
 * {@link com.linkedin.pinot.client.ConnectionFactory} class' utility methods to create connections to a Pinot cluster
 * given a Zookeeper URL, a Java Properties object or a list of broker addresses to connect to.
 *
 * <pre>{@code Connection connection = ConnectionFactory.fromZookeeper
 *     (zk-eat1-pinot.corp.linkedin.com:12913/mpSprintDemoCluster");
 *
 * Connection connection = ConnectionFactory.fromProperties("demo.properties");
 *
 * Connection connection = ConnectionFactory.fromHostList
 *     ("eat1-app1234:1234", "eat1-app2345:1234", ...);}</pre>
 *
 * Queries can be sent directly to the Pinot cluster using the
 * {@link com.linkedin.pinot.client.Connection#execute(java.lang.String)} and
 * {@link com.linkedin.pinot.client.Connection#executeAsync(java.lang.String)} methods of
 * {@link com.linkedin.pinot.client.Connection}.
 *
 * <pre>{@code ResultSet resultSet = connection.execute("select * from foo...");
 * Future<ResultSet> futureResultSet = connection.executeAsync
 *     ("select * from foo...");}</pre>
 *
 * Queries can also use a {@link com.linkedin.pinot.client.PreparedStatement} to escape query parameters:
 *
 * <pre>{@code PreparedStatement statement = connection.prepareStatement
 *     ("select * from foo where a = ?");
 * statement.setString(1, "bar");
 *
 * ResultSet resultSet = statement.execute();
 * Future<ResultSet> futureResultSet = statement.executeAsync();}</pre>
 *
 * In the case of a selection query, results can be obtained with the various <code>get</code> methods in the first
 * {@link com.linkedin.pinot.client.ResultSet}:
 *
 * <pre>{@code ResultSet resultSet = connection.execute
 *     ("select foo, bar from baz where quux = 'quuux'").getResultSet(0);
 *
 * for(int i = 0; i < resultSet.getRowCount(); ++i) {
 *     System.out.println("foo: " + resultSet.getString(i, 1);
 *     System.out.println("bar: " + resultSet.getInt(i, 2);
 * }
 *
 * resultSet.close();}</pre>
 *
 * In the case where there is an aggregation, each aggregation function is within its own
 * {@link com.linkedin.pinot.client.AggregationResultSet}:
 *
 * <pre>{@code ResultSet resultSet = connection.execute("select count(*) from foo");
 *
 * ResultGroup resultGroup = resultSet.getResultSet(0);
 * System.out.println("Number of records: " + resultGroup.getInt(0, 1));
 * resultSet.close();}</pre>
 *
 * There can be more than one {@link com.linkedin.pinot.client.AggregationResultSet}, each of which can contain multiple results
 * grouped by a group key.
 *
 * <pre>{@code ResultSet resultSet = connection.execute
 *     ("select min(foo), max(foo) from bar group by baz");
 *
 * System.out.println("Number of result groups:" +
 *     resultSet.getResultSetCount(); // 2, min(foo) and max(foo)
 *
 * ResultGroup minResultGroup = resultSet.getResultSet(0);
 * for(int i = 0; i < minResultGroup.length(); ++i) {
 *     System.out.println("Minimum foo for " + minResultGroup.getGroupKeyString(i, 1) +
 *         ": " + minResultGroup.getInt(i, 1));
 * }
 *
 * ResultGroup maxResultGroup = resultSet.getResultSet(1);
 * for(int i = 0; i < maxResultGroup.length(); ++i) {
 *     System.out.println("Maximum foo for " + maxResultGroup.getGroupKeyString(i, 1) +
 *         ": " + maxResultGroup.getInt(i, 1));
 * }
 *
 * resultSet.close();}</pre>
 *
 * @author jfim
 */
package com.linkedin.pinot.client;