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
import com.google.common.collect.ImmutableList;
import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
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

    Map<String, String> headers = Map.of("Caller", "curl");

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

    // Verify that Connection has current cursor API (openCursor method)
    Method[] connectionMethods = Connection.class.getDeclaredMethods();
    boolean hasOpenCursor = false;

    for (Method method : connectionMethods) {
      if (method.getName().equals("openCursor")) {
        hasOpenCursor = true;
      }
    }

    Assert.assertTrue(hasOpenCursor, "Connection should have openCursor method");
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
  public void testOpenCursorWithUnsupportedTransport() {
    // Test that openCursor throws UnsupportedOperationException with non-JsonAsyncHttpPinotClientTransport
    List<String> brokers = ImmutableList.of("127.0.0.1:1234");
    PinotClientTransport<?> mockTransport = Mockito.mock(PinotClientTransport.class);
    Connection connection = new Connection(brokers, mockTransport);

    try {
      connection.openCursor("SELECT * FROM testTable", 10);
      Assert.fail("Expected UnsupportedOperationException");
    } catch (UnsupportedOperationException e) {
      Assert.assertEquals("Cursor operations not supported by this connection type", e.getMessage());
    }
  }

  @Test
  public void testOpenCursorWithNullBroker() {
    // Test that openCursor throws PinotClientException when no broker is available
    BrokerSelector mockBrokerSelector = Mockito.mock(BrokerSelector.class);
    Mockito.when(mockBrokerSelector.selectBroker(Mockito.any())).thenReturn(null);

    JsonAsyncHttpPinotClientTransport mockTransport = Mockito.mock(JsonAsyncHttpPinotClientTransport.class);
    Connection connection = new Connection(mockBrokerSelector, mockTransport);

    try {
      connection.openCursor("SELECT * FROM testTable", 10);
      Assert.fail("Expected PinotClientException");
    } catch (PinotClientException e) {
      Assert.assertEquals("Could not find broker to execute cursor query", e.getMessage());
    }
  }

  @Test
  public void testOpenCursorWithValidParameters() throws Exception {
    // Test successful openCursor call with valid parameters
    BrokerSelector mockBrokerSelector = Mockito.mock(BrokerSelector.class);
    Mockito.when(mockBrokerSelector.selectBroker(Mockito.any())).thenReturn("localhost:8099");
    JsonAsyncHttpPinotClientTransport mockTransport = Mockito.mock(JsonAsyncHttpPinotClientTransport.class);
    CursorAwareBrokerResponse mockResponse = Mockito.mock(CursorAwareBrokerResponse.class);
    Mockito.when(mockResponse.hasExceptions()).thenReturn(false);

    // Mock both sync and async methods since openCursor now uses async internally
    Mockito.when(mockTransport.executeQueryWithCursor(Mockito.anyString(), Mockito.anyString(), Mockito.anyInt()))
        .thenReturn(mockResponse);
    Mockito.when(mockTransport.executeQueryWithCursorAsync(Mockito.anyString(), Mockito.anyString(), Mockito.anyInt()))
        .thenReturn(CompletableFuture.completedFuture(mockResponse));

    Connection connection = new Connection(mockBrokerSelector, mockTransport);

    ResultCursor cursor = connection.openCursor("SELECT * FROM testTable", 10);
    Assert.assertNotNull(cursor, "Cursor should not be null");

    // Verify that table name resolution was used
    Mockito.verify(mockBrokerSelector).selectBroker(Mockito.any(String[].class));
    Mockito.verify(mockTransport).executeQueryWithCursorAsync("localhost:8099", "SELECT * FROM testTable", 10);
  }

  @Test
  public void testOpenCursorWithQueryExceptions() throws Exception {
    // Test openCursor behavior when query has exceptions and failOnExceptions is true
    Properties props = new Properties();
    props.setProperty(Connection.FAIL_ON_EXCEPTIONS, "true");

    BrokerSelector mockBrokerSelector = Mockito.mock(BrokerSelector.class);
    Mockito.when(mockBrokerSelector.selectBroker(Mockito.any())).thenReturn("localhost:8099");

    JsonAsyncHttpPinotClientTransport mockTransport = Mockito.mock(JsonAsyncHttpPinotClientTransport.class);
    CursorAwareBrokerResponse mockResponse = Mockito.mock(CursorAwareBrokerResponse.class);
    Mockito.when(mockResponse.hasExceptions()).thenReturn(true);
    JsonNode mockExceptions = Mockito.mock(JsonNode.class);
    Mockito.when(mockResponse.getExceptions()).thenReturn(mockExceptions);
    Mockito.when(mockTransport.executeQueryWithCursor(Mockito.anyString(), Mockito.anyString(), Mockito.anyInt()))
        .thenReturn(mockResponse);

    Connection connection = new Connection(props, mockBrokerSelector, mockTransport);

    try {
      connection.openCursor("SELECT * FROM invalidTable", 10);
      Assert.fail("Expected PinotClientException due to query exceptions");
    } catch (PinotClientException e) {
      Assert.assertTrue(e.getMessage().contains("Failed to open cursor"),
          "Expected exception message to contain 'Failed to open cursor', but was: " + e.getMessage());
    }
  }
}
