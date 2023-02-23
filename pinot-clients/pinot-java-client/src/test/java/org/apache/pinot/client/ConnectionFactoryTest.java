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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.I0Itec.zkclient.ZkClient;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * Tests for the connection factory
 */
public class ConnectionFactoryTest {
  @Test
  public void testZkConnection() {
    // Create a dummy Helix structure
    final String givenZkServers = "127.0.0.1:1234";
    DynamicBrokerSelector dynamicBrokerSelector = Mockito.spy(new DynamicBrokerSelector(givenZkServers) {
      @Override
      protected ZkClient getZkClient(String zkServers) {
        return Mockito.mock(ZkClient.class);
      }

      @Override
      protected ExternalViewReader getEvReader(ZkClient zkClient) {
        return Mockito.mock(ExternalViewReader.class);
      }
    });

    PinotClientTransport pinotClientTransport = Mockito.mock(PinotClientTransport.class);

    // Create the connection
    Connection connection = ConnectionFactory.fromZookeeper(
            dynamicBrokerSelector,
            pinotClientTransport);

    // Check that the broker list has the right length and has the same servers
    Assert.assertEquals(connection.getBrokerList(), ImmutableList.of(givenZkServers));
  }

  @Test
  public void testPropertiesConnection() {
    // Create properties
    Properties properties = new Properties();
    properties.setProperty("brokerList", "127.0.0.1:1234,localhost:2345");

    // Create the connection
    Connection connection = ConnectionFactory.fromProperties(properties);

    // Check that the broker list has the right length and has the same servers
    Assert.assertEquals(connection.getBrokerList(), ImmutableList.of("127.0.0.1:1234", "localhost:2345"));
  }

  @Test
  public void testBrokerList() {
    // Create the connection
    String broker1 = "127.0.0.1:1234";
    String broker2 = "localhost:2345";
    Connection connection = ConnectionFactory.fromHostList(broker1, broker2);

    // Check that the broker list has the right length and has the same servers
    List<String> brokers = ImmutableList.of(broker1, broker2);
    Assert.assertEquals(connection.getBrokerList(), brokers);
  }

  @Test
  public void testBrokerListWithHeaders() {
    // Create the connection
    List<String> brokers = ImmutableList.of("127.0.0.1:1234", "localhost:2345");

    Map<String, String> headers = ImmutableMap.of("Caller", "curl");

    JsonAsyncHttpPinotClientTransportFactory factory = new JsonAsyncHttpPinotClientTransportFactory();
    factory.setHeaders(headers);

    Connection connection = ConnectionFactory.fromHostList(brokers, factory.buildTransport());

    // Check that the broker list has the right length and has the same servers
    Assert.assertEquals(connection.getBrokerList(), brokers);
  }

  @Test
  public void testConnectionTransport() {
    // Create properties
    Properties properties = new Properties();
    properties.setProperty("brokerList", "127.0.0.1:1234,localhost:2345");

    // Create the connection
    Connection connection = ConnectionFactory.fromProperties(properties);

    Assert.assertNotNull(connection.getTransport());
    Assert.assertNotNull(connection.getTransport().getClientMetrics());
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
