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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.I0Itec.zkclient.ZkClient;
import org.apache.commons.io.IOUtils;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;
import static org.testng.Assert.assertEquals;

public class ExternalViewReaderTest {

  @Mock
  private ZkClient _mockZkClient;

  private ExternalViewReader _externalViewReaderUnderTest;

  private final String _instanceConfigPlain = "{\n"
      + "  \"id\": \"Broker_12.34.56.78_1234\",\n"
      + "  \"simpleFields\": {\n"
      + "    \"HELIX_ENABLED\": \"true\",\n"
      + "    \"HELIX_ENABLED_TIMESTAMP\": \"1646486555646\",\n"
      + "    \"HELIX_HOST\": \"first.pug-pinot-broker-headless\",\n"
      + "    \"HELIX_PORT\": \"8099\"\n"
      + "  },\n"
      + "  \"mapFields\": {},\n"
      + "  \"listFields\": {\n"
      + "    \"TAG_LIST\": [\n"
      + "      \"DefaultTenant_BROKER\"\n"
      + "    ]\n"
      + "  }\n"
      + "}";
  private final String _instanceConfigTls = "{\n"
      + "  \"id\": \"Broker_12.34.56.78_1234\",\n"
      + "  \"simpleFields\": {\n"
      + "    \"HELIX_ENABLED\": \"true\",\n"
      + "    \"HELIX_ENABLED_TIMESTAMP\": \"1646486555646\",\n"
      + "    \"HELIX_HOST\": \"first.pug-pinot-broker-headless\",\n"
      + "    \"HELIX_PORT\": \"8099\",\n"
      + "    \"PINOT_TLS_PORT\": \"8090\""
      + "  },\n"
      + "  \"mapFields\": {},\n"
      + "  \"listFields\": {\n"
      + "    \"TAG_LIST\": [\n"
      + "      \"DefaultTenant_BROKER\"\n"
      + "    ]\n"
      + "  }\n"
      + "}";
  @BeforeMethod
  public void setUp()
      throws Exception {
    initMocks(this);
    InputStream mockInputStream =
        IOUtils.toInputStream("{\"mapFields\":{\"field1\":{\"Broker_12.34.56.78_1234\":\"ONLINE\"}}}", "UTF-8");
    _externalViewReaderUnderTest = Mockito.spy(new ExternalViewReader(_mockZkClient) {
      @Override
      protected ByteArrayInputStream getInputStream(byte[] brokerResourceNodeData) {
        return (ByteArrayInputStream) mockInputStream;
      }
    });
  }

  @Test
  public void testGetLiveBrokers()
      throws IOException {
    // Setup
    final List<String> expectedResult = Arrays.asList("12.34.56.78:1234");
    when(_mockZkClient.readData(Mockito.anyString(), Mockito.anyBoolean())).thenReturn("json".getBytes());

    // Run the test
    final List<String> result = _externalViewReaderUnderTest.getLiveBrokers();

    // Verify the results
    assertEquals(expectedResult, result);
  }

  @Test
  public void testGetLiveBrokersExceptionState()
      throws IOException {
    // Setup
    final List<String> expectedResult = Arrays.asList();
    when(_mockZkClient.readData(Mockito.anyString(), Mockito.anyBoolean())).thenThrow(RuntimeException.class);

    // Run the test
    final List<String> result = _externalViewReaderUnderTest.getLiveBrokers();

    // Verify the results
    assertEquals(expectedResult, result);
  }

  @Test
  public void testGetTableToBrokersMap() {
    // Setup
    final Map<String, List<String>> expectedResult = new HashMap<>();
    expectedResult.put("field1", Arrays.asList("12.34.56.78:1234"));
    when(_mockZkClient.readData(Mockito.anyString(), Mockito.anyBoolean())).thenReturn("json".getBytes());

    // Run the test
    final Map<String, List<String>> result = _externalViewReaderUnderTest.getTableToBrokersMap();

    // Verify the results
    assertEquals(expectedResult, result);
  }

  @Test
  public void testGetTableToBrokersMapExceptionState() {
    // Setup
    final Map<String, List<String>> expectedResult = new HashMap<>();
    when(_mockZkClient.readData(Mockito.anyString(), Mockito.anyBoolean())).thenThrow(RuntimeException.class);

    // Run the test
    final Map<String, List<String>> result = _externalViewReaderUnderTest.getTableToBrokersMap();

    // Verify the results
    assertEquals(expectedResult, result);
  }

  @Test
  public void testGetBrokersMapByInstanceConfig() {
    configureData(_instanceConfigPlain, true);
    // Run the test
    final Map<String, List<String>> result = _externalViewReaderUnderTest.getTableToBrokersMap();
    final Map<String, List<String>> expectedResult = Map.of("field1",
        Arrays.asList("first.pug-pinot-broker-headless:8099"));
    // Verify the results
    assertEquals(expectedResult, result);
  }

  private void configureData(String instanceConfigPlain, boolean preferTls) {
    when(_mockZkClient.readData(ExternalViewReader.BROKER_EXTERNAL_VIEW_PATH, true))
        .thenReturn("json".getBytes());
    when(_mockZkClient.readData(ExternalViewReader.BROKER_INSTANCE_PATH + "/Broker_12.34.56.78_1234", true))
        .thenReturn(instanceConfigPlain.getBytes(StandardCharsets.UTF_8));
    _externalViewReaderUnderTest._preferTlsPort = preferTls;
  }

  @Test
  public void testGetBrokerListByInstanceConfigDefault() {
    configureData(_instanceConfigPlain, false);
    final List<String> brokers = _externalViewReaderUnderTest.getLiveBrokers();
    assertEquals(brokers, Arrays.asList("first.pug-pinot-broker-headless:8099"));
  }

  @Test
  public void testGetBrokersMapByInstanceConfigTlsDefault() {
    configureData(_instanceConfigTls, false);
    final Map<String, List<String>> result = _externalViewReaderUnderTest.getTableToBrokersMap();
    final Map<String, List<String>> expectedResult = Map.of("field1",
        Arrays.asList("first.pug-pinot-broker-headless:8099"));
    // Verify the results
    assertEquals(expectedResult, result);
  }
  @Test
  public void testGetBrokerListByInstanceConfigTlsDefault() {
    configureData(_instanceConfigTls, false);
    final List<String> brokers = _externalViewReaderUnderTest.getLiveBrokers();
    assertEquals(brokers, Arrays.asList("first.pug-pinot-broker-headless:8099"));
  }

  @Test
  public void testGetBrokersMapByInstanceConfigDefault() {
    configureData(_instanceConfigPlain, false);
    // Run the test
    final Map<String, List<String>> result = _externalViewReaderUnderTest.getTableToBrokersMap();
    final Map<String, List<String>> expectedResult = Map.of("field1",
        Arrays.asList("first.pug-pinot-broker-headless:8099"));
    // Verify the results
    assertEquals(expectedResult, result);
  }

  @Test
  public void testGetBrokerListByInstanceConfig() {
    configureData(_instanceConfigPlain, true);
    final List<String> brokers = _externalViewReaderUnderTest.getLiveBrokers();
    assertEquals(brokers, Arrays.asList("first.pug-pinot-broker-headless:8099"));
  }

  @Test
  public void testGetBrokersMapByInstanceConfigTls() {
    configureData(_instanceConfigTls, true);
    final Map<String, List<String>> result = _externalViewReaderUnderTest.getTableToBrokersMap();
    final Map<String, List<String>> expectedResult = Map.of("field1",
        Arrays.asList("first.pug-pinot-broker-headless:8090"));
    // Verify the results
    assertEquals(expectedResult, result);
  }
  @Test
  public void testGetBrokerListByInstanceConfigTls() {
    configureData(_instanceConfigTls, true);
    final List<String> brokers = _externalViewReaderUnderTest.getLiveBrokers();
    assertEquals(brokers, Arrays.asList("first.pug-pinot-broker-headless:8090"));
  }

  // -----------------------------------------------------------------------------------------------
  // Instance-config read amplification
  //
  // The broker resource is walked table by table, so the same broker is met once per table it
  // serves. Its address is a property of the broker, so every read after the first returns the same
  // bytes. The fixtures above cannot show this: they hold one table and one broker, where N x M and
  // M are both 1.
  // -----------------------------------------------------------------------------------------------

  private static final String BROKER_A = "Broker_10.0.0.1_8099";
  private static final String BROKER_B = "Broker_10.0.0.2_8099";

  /// Two tables, both served by the same two brokers: four (table, broker) pairs over two distinct
  /// brokers.
  private ExternalViewReader newReaderOverTwoTablesAndTwoBrokers() {
    String externalView = "{\"mapFields\":{"
        + "\"table1_OFFLINE\":{\"" + BROKER_A + "\":\"ONLINE\",\"" + BROKER_B + "\":\"ONLINE\"},"
        + "\"table2_OFFLINE\":{\"" + BROKER_A + "\":\"ONLINE\",\"" + BROKER_B + "\":\"ONLINE\"}}}";
    when(_mockZkClient.readData(ExternalViewReader.BROKER_EXTERNAL_VIEW_PATH, true))
        .thenReturn("json".getBytes(StandardCharsets.UTF_8));
    when(_mockZkClient.readData(ExternalViewReader.BROKER_INSTANCE_PATH + "/" + BROKER_A, true))
        .thenReturn(instanceConfig("10.0.0.1", "8099").getBytes(StandardCharsets.UTF_8));
    when(_mockZkClient.readData(ExternalViewReader.BROKER_INSTANCE_PATH + "/" + BROKER_B, true))
        .thenReturn(instanceConfig("10.0.0.2", "8099").getBytes(StandardCharsets.UTF_8));
    return new ExternalViewReader(_mockZkClient) {
      @Override
      protected ByteArrayInputStream getInputStream(byte[] brokerResourceNodeData) {
        return new ByteArrayInputStream(externalView.getBytes(StandardCharsets.UTF_8));
      }
    };
  }

  private static String instanceConfig(String host, String port) {
    return "{\"id\":\"Broker_" + host + "_" + port + "\",\"simpleFields\":{"
        + "\"HELIX_HOST\":\"" + host + "\",\"HELIX_PORT\":\"" + port + "\"},"
        + "\"mapFields\":{},\"listFields\":{}}";
  }

  @Test
  public void testGetTableToBrokersMapReadsEachInstanceConfigOncePerCall() {
    ExternalViewReader reader = newReaderOverTwoTablesAndTwoBrokers();

    reader.getTableToBrokersMap();

    // Two distinct brokers, so two reads -- not the four the four (table, broker) pairs would imply.
    Mockito.verify(_mockZkClient, Mockito.times(1))
        .readData(ExternalViewReader.BROKER_INSTANCE_PATH + "/" + BROKER_A, true);
    Mockito.verify(_mockZkClient, Mockito.times(1))
        .readData(ExternalViewReader.BROKER_INSTANCE_PATH + "/" + BROKER_B, true);
  }

  /// Fewer reads, identical result: every table still maps to both broker addresses.
  @Test
  public void testGetLiveBrokersReadsEachInstanceConfigOncePerCall() {
    ExternalViewReader reader = newReaderOverTwoTablesAndTwoBrokers();

    reader.getLiveBrokers();

    Mockito.verify(_mockZkClient, Mockito.times(1))
        .readData(ExternalViewReader.BROKER_INSTANCE_PATH + "/" + BROKER_A, true);
    Mockito.verify(_mockZkClient, Mockito.times(1))
        .readData(ExternalViewReader.BROKER_INSTANCE_PATH + "/" + BROKER_B, true);
  }

  /// getLiveBrokers returns a List and has always emitted one entry per (table, broker) pair rather
  /// than per broker. Reusing a resolved address must not quietly turn that into a de-duplicated
  /// list: it is public API, and a caller picking at random would see its weighting change.
  @Test
  public void testGetLiveBrokersStillReturnsOneEntryPerTableBrokerPair() {
    ExternalViewReader reader = newReaderOverTwoTablesAndTwoBrokers();

    List<String> brokers = reader.getLiveBrokers();

    assertEquals(brokers.size(), 4);
    assertEquals(Set.copyOf(brokers), Set.of("10.0.0.1:8099", "10.0.0.2:8099"));
  }

  @Test
  public void testGetTableToBrokersMapResultIsUnchangedByReusingResolvedAddresses() {
    ExternalViewReader reader = newReaderOverTwoTablesAndTwoBrokers();

    Map<String, List<String>> result = reader.getTableToBrokersMap();

    assertEquals(result.keySet(), Set.of("table1", "table2"));
    assertEquals(Set.copyOf(result.get("table1")), Set.of("10.0.0.1:8099", "10.0.0.2:8099"));
    assertEquals(Set.copyOf(result.get("table2")), Set.of("10.0.0.1:8099", "10.0.0.2:8099"));
  }

  /// A fresh call must re-read: the map lives for one call only, so a broker that moved host or
  /// port is picked up on the next refresh without any invalidation machinery.
  @Test
  public void testResolvedAddressesAreNotRetainedAcrossCalls() {
    ExternalViewReader reader = newReaderOverTwoTablesAndTwoBrokers();

    reader.getTableToBrokersMap();
    reader.getTableToBrokersMap();

    Mockito.verify(_mockZkClient, Mockito.times(2))
        .readData(ExternalViewReader.BROKER_INSTANCE_PATH + "/" + BROKER_A, true);
  }
}
