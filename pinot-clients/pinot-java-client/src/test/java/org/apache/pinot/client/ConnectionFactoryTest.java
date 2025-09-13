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
import java.lang.reflect.Method;
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
    final String givenBrokerInfo = "localhost:2345";

    DynamicBrokerSelector dynamicBrokerSelector = Mockito.spy(new DynamicBrokerSelector(givenZkServers) {
      @Override
      protected ZkClient getZkClient(String zkServers) {
        return Mockito.mock(ZkClient.class);
      }

      @Override
      protected ExternalViewReader getEvReader(ZkClient zkClient) {
        return Mockito.mock(ExternalViewReader.class);
      }

      @Override
      public List<String> getBrokers() {
        return ImmutableList.of(givenBrokerInfo);
      }
    });

    PinotClientTransport pinotClientTransport = Mockito.mock(PinotClientTransport.class);

    // Create the connection
    Connection connection = ConnectionFactory.fromZookeeper(
            dynamicBrokerSelector,
            pinotClientTransport);

    // Check that the broker list has the right length and has the same servers
    Assert.assertEquals(connection.getBrokerList(), ImmutableList.of(givenBrokerInfo));
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

  @Test
  public void testConnectionFactoryMethodsPreserved() {
    // Test that the ConnectionFactory class has the expected methods
    Method[] methods = ConnectionFactory.class.getDeclaredMethods();

    boolean hasFromZookeeper = false;
    boolean hasFromController = false;
    boolean hasFromHostList = false;
    boolean hasFromProperties = false;

    for (Method method : methods) {
      if (method.getName().equals("fromZookeeper") && method.getReturnType() == Connection.class) {
        hasFromZookeeper = true;
      }
      if (method.getName().equals("fromController") && method.getReturnType() == Connection.class) {
        hasFromController = true;
      }
      if (method.getName().equals("fromHostList") && method.getReturnType() == Connection.class) {
        hasFromHostList = true;
      }
      if (method.getName().equals("fromProperties") && method.getReturnType() == Connection.class) {
        hasFromProperties = true;
      }
    }

    // Verify existing methods are preserved
    Assert.assertTrue(hasFromZookeeper, "fromZookeeper methods should be preserved");
    Assert.assertTrue(hasFromController, "fromController methods should be preserved");
    Assert.assertTrue(hasFromHostList, "fromHostList methods should be preserved");
    Assert.assertTrue(hasFromProperties, "fromProperties methods should be preserved");

    // Verify that Connection has cursor methods (hybrid approach)
    Method[] connectionMethods = Connection.class.getDeclaredMethods();
    boolean hasExecuteWithCursor = false;
    boolean hasFetchNext = false;
    boolean hasFetchPrevious = false;
    boolean hasSeekToPage = false;

    for (Method method : connectionMethods) {
      if (method.getName().equals("executeCursorQuery")) {
        hasExecuteWithCursor = true;
      }
      if (method.getName().equals("fetchNext")) {
        hasFetchNext = true;
      }
      if (method.getName().equals("fetchPrevious")) {
        hasFetchPrevious = true;
      }
      if (method.getName().equals("seekToPage")) {
        hasSeekToPage = true;
      }
    }

    Assert.assertTrue(hasExecuteWithCursor, "Connection should have executeCursorQuery method");
    Assert.assertTrue(hasFetchNext, "Connection should have fetchNext method");
    Assert.assertTrue(hasFetchPrevious, "Connection should have fetchPrevious method");
    Assert.assertTrue(hasSeekToPage, "Connection should have seekToPage method");
  }

  @Test
  public void testConnectionCursorFunctionalityWithJsonTransport() {
    // Test that connections created with JsonAsyncHttpPinotClientTransport support cursor operations
    List<String> brokers = ImmutableList.of("127.0.0.1:1234");
    JsonAsyncHttpPinotClientTransportFactory factory = new JsonAsyncHttpPinotClientTransportFactory();
    Connection connection = ConnectionFactory.fromHostList(brokers, factory.buildTransport());

    // Verify the connection has JsonAsyncHttpPinotClientTransport
    Assert.assertTrue(connection.getTransport() instanceof JsonAsyncHttpPinotClientTransport,
        "Connection should use JsonAsyncHttpPinotClientTransport for cursor support");
  }

  @Test
  public void testConnectionCursorFunctionalityWithOtherTransports() {
    // Test that connections created with other transports handle cursor operations gracefully
    List<String> brokers = ImmutableList.of("127.0.0.1:1234");
    PinotClientTransport mockTransport = Mockito.mock(PinotClientTransport.class);
    Connection connection = ConnectionFactory.fromHostList(brokers, mockTransport);

    // Verify cursor operations throw UnsupportedOperationException for non-JsonAsyncHttpPinotClientTransport
    try {
      connection.fetchNext("test-cursor");
      Assert.fail("Should throw UnsupportedOperationException");
    } catch (UnsupportedOperationException e) {
      Assert.assertEquals(e.getMessage(), "Cursor operations not supported by this connection type");
    }
  }
}
