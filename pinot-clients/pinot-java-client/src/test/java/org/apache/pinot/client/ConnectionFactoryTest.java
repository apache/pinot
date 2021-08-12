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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * Tests for the connection factory
 */
public class ConnectionFactoryTest {
  @Test
  public void testZkConnection() {
    // TODO Write test
    // Create a dummy Helix structure
    // Create the connection
    // Check that the broker list has the right length and has the same servers
  }

  @Test
  public void testPropertiesConnection() {
    // TODO Write test
    // Create a properties object
    // Create the connection
    // Check that the broker list has the right length and has the same servers
  }

  @Test
  public void testBrokerList() {
    // Create the connection
    String broker1 = "127.0.0.1:1234";
    String broker2 = "localhost:2345";
    Connection connection = ConnectionFactory.fromHostList(broker1, broker2);

    // Check that the broker list has the right length and has the same servers
    List<String> brokers = new ArrayList<String>();
    brokers.add(broker1);
    brokers.add(broker2);
    Assert.assertEquals(connection.getBrokerList(), brokers);
  }

  @Test
  public void testBrokerListWithHeaders() {
    // Create the connection
    List<String> brokers = new ArrayList<>();
    brokers.add("127.0.0.1:1234");
    brokers.add("localhost:2345");

    Map<String, String> headers = new HashMap<>();
    headers.put("Caller", "curl");

    JsonAsyncHttpPinotClientTransportFactory factory = new JsonAsyncHttpPinotClientTransportFactory();
    factory.setHeaders(headers);

    Connection connection = ConnectionFactory.fromHostList(brokers, factory.buildTransport());

    // Check that the broker list has the right length and has the same servers
    Assert.assertEquals(connection.getBrokerList(), brokers);
  }

  // For testing DynamicBrokerSelector

  /**
   * ConnectionFactoryTest <ZK_URL> <tableName> <query>
   * @param args
   */
  public static void main(String[] args) {
    if (args.length != 3) {
      System.err.println("USAGE ConnectionFactoryTest <ZK_URL> <tableName> <query>");
      System.exit(1);
    }
    String zkUrl = args[0];
    Connection connection = ConnectionFactory.fromZookeeper(zkUrl);
    String tableName = args[1];
    ResultSetGroup resultSetGroup = connection.execute(tableName, args[2]);
    System.out.println(resultSetGroup);
  }
}
