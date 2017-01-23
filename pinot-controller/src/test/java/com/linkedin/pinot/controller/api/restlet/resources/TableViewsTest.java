/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
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
package com.linkedin.pinot.controller.api.restlet.resources;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Collections;
import java.util.Map;
import org.json.JSONException;
import org.json.JSONObject;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.pinot.common.request.helper.ControllerRequestBuilder;
import com.linkedin.pinot.common.segment.SegmentMetadata;
import com.linkedin.pinot.common.utils.CommonConstants.Helix.DataSource;
import com.linkedin.pinot.common.utils.ZkStarter;
import com.linkedin.pinot.controller.helix.ControllerRequestBuilderUtil;
import com.linkedin.pinot.controller.helix.ControllerRequestURLBuilder;
import com.linkedin.pinot.controller.helix.ControllerTest;
import com.linkedin.pinot.controller.helix.core.PinotHelixResourceManager;
import com.linkedin.pinot.core.query.utils.SimpleSegmentMetadata;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.AssertJUnit.assertNotSame;


public class TableViewsTest extends ControllerTest {

  public static final String TABLE_NAME = "VIEWS_TABLE";
  public static final String OFFLINE_ONLY_TABLE = "OFFLINE_ONLY_TABLE";
  private static PinotHelixResourceManager pinotHelixResourceManager;

  @BeforeClass
  public void setupTest()
      throws Exception {
    startZk();
    startController();
    pinotHelixResourceManager =
        new PinotHelixResourceManager(ZkStarter.DEFAULT_ZK_STR,
            getHelixClusterName(), TableViewsTest.class.getName() + "_controller",
            null, 10000L, true, /*isUpdateStateModel=*/false);
    pinotHelixResourceManager.start();

    ControllerRequestBuilderUtil.addFakeBrokerInstancesToAutoJoinHelixCluster(getHelixClusterName(),
        ZkStarter.DEFAULT_ZK_STR, 5, true);
    ControllerRequestBuilderUtil.addFakeDataInstancesToAutoJoinHelixCluster(getHelixClusterName(),
        ZkStarter.DEFAULT_ZK_STR, 20, true);

    JSONObject request = ControllerRequestBuilderUtil.buildBrokerTenantCreateRequestJSON("default", 5);
    sendPostRequest(ControllerRequestURLBuilder.baseUrl(CONTROLLER_BASE_API_URL).forBrokerTenantCreate(),
        request.toString());

    request = ControllerRequestBuilderUtil.buildServerTenantCreateRequestJSON("default", 20, 16, 2);
    sendPostRequest(ControllerRequestURLBuilder.baseUrl(CONTROLLER_BASE_API_URL).forBrokerTenantCreate(),
        request.toString());

    request = ControllerRequestBuilder.buildCreateOfflineTableJSON(OFFLINE_ONLY_TABLE, "default", "default",
        2, "BalanceNumSegmentAssignmentStrategy");
    sendPostRequest(ControllerRequestURLBuilder.baseUrl(CONTROLLER_BASE_API_URL).forTableCreate(),
        request.toString());
    addOneSegment(OFFLINE_ONLY_TABLE);

    request = ControllerRequestBuilder.buildCreateOfflineTableJSON(TABLE_NAME, "default", "default",
        2, "BalanceNumSegmentAssignmentStrategy");
    sendPostRequest(ControllerRequestURLBuilder.baseUrl(CONTROLLER_BASE_API_URL).forTableCreate(),
        request.toString());
    JSONObject metadata = new JSONObject();
    metadata.put("streamType", "kafka");
    metadata.put(DataSource.STREAM_PREFIX + "." + DataSource.Realtime.Kafka.CONSUMER_TYPE, DataSource.Realtime.Kafka.ConsumerType.highLevel.toString());
    metadata.put(DataSource.STREAM_PREFIX + "." + DataSource.Realtime.Kafka.TOPIC_NAME, "fakeTopic");
    metadata.put(DataSource.STREAM_PREFIX + "." + DataSource.Realtime.Kafka.DECODER_CLASS, "fakeClass");
    metadata.put(DataSource.STREAM_PREFIX + "." + DataSource.Realtime.Kafka.ZK_BROKER_URL, "fakeUrl");
    metadata.put(DataSource.STREAM_PREFIX + "." + DataSource.Realtime.Kafka.HighLevelConsumer.ZK_CONNECTION_STRING, "potato");
    metadata.put(DataSource.Realtime.REALTIME_SEGMENT_FLUSH_SIZE, Integer.toString(1234));
    metadata.put(DataSource.STREAM_PREFIX + "." + DataSource.Realtime.Kafka.KAFKA_CONSUMER_PROPS_PREFIX + "." + DataSource.Realtime.Kafka.AUTO_OFFSET_RESET,
        "smallest");
    request = ControllerRequestBuilder.buildCreateRealtimeTableJSON(TABLE_NAME, "default", "default",
        "potato", "DAYS", "DAYS", "5", 2, "BalanceNumSegmentAssignmentStrategy", metadata, "fakeSchema", "fakeColumn",
        Collections.<String>emptyList(), "MMAP", true);
    sendPostRequest(ControllerRequestURLBuilder.baseUrl(CONTROLLER_BASE_API_URL).forTableCreate(),
        request.toString());
  }

  @AfterClass
  public void teardownTest()
      throws Exception {
    stopController();
    stopZk();
  }

  @DataProvider(name = "stateProvider")
  public Object[][] stateProvider() {
    Object[][] configs = {
        {TableViews.IDEALSTATE},
        {TableViews.EXTERNALVIEW}
    };
    return configs;
  }

  @Test(dataProvider = "stateProvider")
  public void getOfflineTableState(String state)
      throws IOException, JSONException {
    String response = getState(OFFLINE_ONLY_TABLE, state, null);
    TableViews.TableView tableView = toTableViews(response);
    assertNotNull(tableView.offline);
    assertNull(tableView.realtime);
    assertEquals(tableView.offline.size(), 1);

    for (Map.Entry<String, Map<String, String>> stringMapEntry : tableView.offline.entrySet()) {
      assertTrue(stringMapEntry.getKey().startsWith("SimpleSegment"));
      Map<String, String> serverMap = stringMapEntry.getValue();
      assertEquals(serverMap.size(), 2);
      for (Map.Entry<String, String> serverMapEntry : serverMap.entrySet()) {
        assertTrue(serverMapEntry.getKey().startsWith("Server_"));
        assertEquals(serverMapEntry.getValue(), "ONLINE");
      }
    }
  }

  @Test(dataProvider = "stateProvider")
  public void testTableNotFound(String state)
      throws IOException, JSONException {
     ControllerRequestURLBuilder requestBuilder =
        ControllerRequestURLBuilder.baseUrl(CONTROLLER_BASE_API_URL);
    String url = requestBuilder.forTableView("UNKNOWN_TABLE", state, null);
    HttpURLConnection connection = (HttpURLConnection) new URL(url).openConnection();
    assertEquals(connection.getResponseCode(), 404);
  }

  @Test(dataProvider = "stateProvider")
  public void testBadRequest(String state)
      throws IOException {
    ControllerRequestURLBuilder requestURLBuilder =
        ControllerRequestURLBuilder.baseUrl(CONTROLLER_BASE_API_URL);
    String url = requestURLBuilder.forTableView("UNKNOWN_TABLE", state, "no_such_type");
    HttpURLConnection connection = (HttpURLConnection) new URL(url).openConnection();
    assertEquals(connection.getResponseCode(), 400);
  }

  @Test(dataProvider = "stateProvider")
  public void testGetState(String state)
      throws IOException, JSONException {
    String response = getState(TABLE_NAME, state, "realtime");
    TableViews.TableView tableView = toTableViews(response);
    assertNull(tableView.offline);
    assertNotNull(tableView.realtime);
    assertNotSame(tableView.realtime.size(), 0);

    response = getState(TABLE_NAME, state, "offline");
    tableView = toTableViews(response);
    assertNull(tableView.realtime);
    assertNotNull(tableView.offline);
    // empty because we didn't add any segment
    assertEquals(tableView.offline.size(), 0);

    response = getState(TABLE_NAME, state, null);
    tableView = toTableViews(response);
    assertNotNull(tableView.offline);
    assertNotNull(tableView.realtime);
    assertEquals(tableView.offline.size(), 0);
    assertNotSame(tableView.realtime.size(), 0);

    response = getState(TABLE_NAME + "_REALTIME", state, null);
    tableView = toTableViews(response);
    assertNull(tableView.offline);
    assertNotNull(tableView.realtime);
  }

  private String getState(String tableName, String state, String tableType)
      throws IOException, JSONException {
    ControllerRequestURLBuilder requestBuilder =
        ControllerRequestURLBuilder.baseUrl(CONTROLLER_BASE_API_URL);
    return sendGetRequest(requestBuilder.forTableView(tableName, state, tableType));
  }

  private void addOneSegment(String tableName) {
    SegmentMetadata metadata = new SimpleSegmentMetadata(tableName);
    pinotHelixResourceManager.addSegment(metadata, "someurl");
  }

  private TableViews.TableView toTableViews(String response)
      throws IOException {
    ObjectMapper mapper = new ObjectMapper();
    return mapper.readValue(response, TableViews.TableView.class);
  }
}
