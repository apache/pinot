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
package org.apache.pinot.integration.tests;

import com.fasterxml.jackson.databind.JsonNode;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState;
import org.apache.pinot.broker.broker.helix.BaseBrokerStarter;
import org.apache.pinot.common.utils.URIUtils;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.exception.QueryErrorCode;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.apache.pinot.util.TestUtils;
import org.intellij.lang.annotations.Language;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.fail;


public class HybridClusterIntegrationTest extends BaseHybridClusterIntegrationTest {
  @Test
  public void testUpdateBrokerResource()
      throws Exception {
    // Add a new broker to the cluster
    BaseBrokerStarter brokerStarter = startOneBroker(1);

    // Check if broker is added to all the tables in broker resource
    String clusterName = getHelixClusterName();
    String brokerId = brokerStarter.getInstanceId();
    IdealState brokerResourceIdealState =
        _helixAdmin.getResourceIdealState(clusterName, CommonConstants.Helix.BROKER_RESOURCE_INSTANCE);
    for (Map<String, String> brokerAssignment : brokerResourceIdealState.getRecord().getMapFields().values()) {
      assertEquals(brokerAssignment.get(brokerId), CommonConstants.Helix.StateModel.BrokerResourceStateModel.ONLINE);
    }
    TestUtils.waitForCondition(aVoid -> {
      ExternalView brokerResourceExternalView =
          _helixAdmin.getResourceExternalView(clusterName, CommonConstants.Helix.BROKER_RESOURCE_INSTANCE);
      for (Map<String, String> brokerAssignment : brokerResourceExternalView.getRecord().getMapFields().values()) {
        if (!brokerAssignment.containsKey(brokerId)) {
          return false;
        }
      }
      return true;
    }, 60_000L, "Failed to find broker in broker resource ExternalView");

    // Stop the broker
    brokerStarter.stop();
    _brokerPorts.remove(_brokerPorts.size() - 1);

    // Dropping the broker should fail because it is still in the broker resource
    try {
      sendDeleteRequest(_controllerRequestURLBuilder.forInstance(brokerId));
      fail("Dropping instance should fail because it is still in the broker resource");
    } catch (Exception e) {
      // Expected
    }

    // Untag the broker and update the broker resource so that it is removed from the broker resource
    sendPutRequest(_controllerRequestURLBuilder.forInstanceUpdateTags(brokerId, Collections.emptyList(), true));

    // Check if broker is removed from all the tables in broker resource
    brokerResourceIdealState =
        _helixAdmin.getResourceIdealState(clusterName, CommonConstants.Helix.BROKER_RESOURCE_INSTANCE);
    for (Map<String, String> brokerAssignment : brokerResourceIdealState.getRecord().getMapFields().values()) {
      assertFalse(brokerAssignment.containsKey(brokerId));
    }
    TestUtils.waitForCondition(aVoid -> {
      ExternalView brokerResourceExternalView =
          _helixAdmin.getResourceExternalView(clusterName, CommonConstants.Helix.BROKER_RESOURCE_INSTANCE);
      for (Map<String, String> brokerAssignment : brokerResourceExternalView.getRecord().getMapFields().values()) {
        if (brokerAssignment.containsKey(brokerId)) {
          return false;
        }
      }
      return true;
    }, 60_000L, "Failed to remove broker from broker resource ExternalView");

    // Dropping the broker should success now
    sendDeleteRequest(_controllerRequestURLBuilder.forInstance(brokerId));

    // Check if broker is dropped from the cluster
    assertFalse(_helixAdmin.getInstancesInCluster(clusterName).contains(brokerId));
  }

  @Test
  public void testSegmentMetadataApi()
      throws Exception {
    {
      String jsonOutputStr = sendGetRequest(_controllerRequestURLBuilder.forSegmentsMetadataFromServer(getTableName()));
      JsonNode tableSegmentsMetadata = JsonUtils.stringToJsonNode(jsonOutputStr);
      Assert.assertEquals(tableSegmentsMetadata.size(), 8);

      JsonNode segmentMetadataFromAllEndpoint = tableSegmentsMetadata.elements().next();
      String segmentName = segmentMetadataFromAllEndpoint.get("segmentName").asText();
      jsonOutputStr = sendGetRequest(_controllerRequestURLBuilder.forSegmentMetadata(getTableName(), segmentName));
      JsonNode segmentMetadataFromDirectEndpoint = JsonUtils.stringToJsonNode(jsonOutputStr);
      Assert.assertEquals(segmentMetadataFromAllEndpoint.get("totalDocs"),
          segmentMetadataFromDirectEndpoint.get("segment.total.docs"));
    }
    {
      List<String> segmentNames = getSegmentNames(getTableName(), TableType.OFFLINE.toString());
      List<String> segments = new ArrayList<>();
      for (String segment : segmentNames) {
        String encodedSegmentName = URLEncoder.encode(segment, StandardCharsets.UTF_8.toString());
        segments.add(encodedSegmentName);
      }
      String jsonOutputStr = sendGetRequest(_controllerRequestURLBuilder.forSegmentsMetadataFromServer(getTableName(),
          null, segments));
      JsonNode tableSegmentsMetadata = JsonUtils.stringToJsonNode(jsonOutputStr);
      Assert.assertEquals(tableSegmentsMetadata.size(), 8);
      JsonNode segmentMetadataFromAllEndpoint = tableSegmentsMetadata.elements().next();
      String segmentName = segmentMetadataFromAllEndpoint.get("segmentName").asText();
      jsonOutputStr = sendGetRequest(_controllerRequestURLBuilder.forSegmentMetadata(getTableName(), segmentName));
      JsonNode segmentMetadataFromDirectEndpoint = JsonUtils.stringToJsonNode(jsonOutputStr);
      Assert.assertEquals(segmentMetadataFromAllEndpoint.get("totalDocs"),
          segmentMetadataFromDirectEndpoint.get("segment.total.docs"));
    }
  }

  @Test
  public void testSegmentListApi()
      throws Exception {
    {
      String jsonOutputStr =
          sendGetRequest(_controllerRequestURLBuilder.forSegmentListAPI(getTableName(), TableType.OFFLINE.toString()));
      JsonNode array = JsonUtils.stringToJsonNode(jsonOutputStr);
      // There should be one element in the array
      JsonNode element = array.get(0);
      JsonNode segments = element.get("OFFLINE");
      Assert.assertEquals(segments.size(), 8);
    }
    {
      String jsonOutputStr =
          sendGetRequest(_controllerRequestURLBuilder.forSegmentListAPI(getTableName(), TableType.REALTIME.toString()));
      JsonNode array = JsonUtils.stringToJsonNode(jsonOutputStr);
      // There should be one element in the array
      JsonNode element = array.get(0);
      JsonNode segments = element.get("REALTIME");
      Assert.assertEquals(segments.size(), 24);
    }
    {
      String jsonOutputStr = sendGetRequest(_controllerRequestURLBuilder.forSegmentListAPI(getTableName()));
      JsonNode array = JsonUtils.stringToJsonNode(jsonOutputStr);
      JsonNode offlineSegments = array.get(0).get("OFFLINE");
      Assert.assertEquals(offlineSegments.size(), 8);
      JsonNode realtimeSegments = array.get(1).get("REALTIME");
      Assert.assertEquals(realtimeSegments.size(), 24);
    }
  }

  // NOTE: Reload consuming segment will force commit it, so run this test after segment list api test
  @Test(dependsOnMethods = "testSegmentListApi")
  public void testReload()
      throws Exception {
    super.testReload(true);
  }

  @Test
  public void testBrokerDebugOutput()
      throws Exception {
    String tableName = getTableName();
    Assert.assertNotNull(getDebugInfo("debug/timeBoundary/" + tableName));
    Assert.assertNotNull(getDebugInfo("debug/timeBoundary/" + TableNameBuilder.OFFLINE.tableNameWithType(tableName)));
    Assert.assertNotNull(getDebugInfo("debug/timeBoundary/" + TableNameBuilder.REALTIME.tableNameWithType(tableName)));
    Assert.assertNotNull(getDebugInfo("debug/routingTable/" + tableName));
    Assert.assertNotNull(getDebugInfo("debug/routingTable/" + TableNameBuilder.OFFLINE.tableNameWithType(tableName)));
    Assert.assertNotNull(getDebugInfo("debug/routingTable/" + TableNameBuilder.REALTIME.tableNameWithType(tableName)));
  }

  @Test(dataProvider = "useBothQueryEngines")
  public void testBrokerDebugRoutingTableSQL(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);
    String tableName = getTableName();
    String offlineTableName = TableNameBuilder.OFFLINE.tableNameWithType(tableName);
    String realtimeTableName = TableNameBuilder.REALTIME.tableNameWithType(tableName);
    String encodedSQL;
    encodedSQL = URIUtils.encode("select * from " + realtimeTableName);
    Assert.assertNotNull(getDebugInfo("debug/routingTable/sql?query=" + encodedSQL));
    encodedSQL = URIUtils.encode("select * from " + offlineTableName);
    Assert.assertNotNull(getDebugInfo("debug/routingTable/sql?query=" + encodedSQL));
  }

  @Test(dataProvider = "useBothQueryEngines")
  public void testQueryTracing(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);
    // Tracing is a v1 only concept and the v2 query engine has separate multi-stage stats that are enabled by default
    notSupportedInV2();
    JsonNode jsonNode = postQuery("SET trace = true; SELECT COUNT(*) FROM " + getTableName());
    Assert.assertEquals(jsonNode.get("resultTable").get("rows").get(0).get(0).asLong(), getCountStarResult());
    Assert.assertTrue(jsonNode.get("exceptions").isEmpty());
    JsonNode traceInfo = jsonNode.get("traceInfo");
    Assert.assertEquals(traceInfo.size(), 2);
    Assert.assertTrue(traceInfo.has("localhost_O"));
    Assert.assertTrue(traceInfo.has("localhost_R"));
  }

  @Test(dataProvider = "useBothQueryEngines")
  public void testQueryTracingWithLiteral(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);
    // Tracing is a v1 only concept and the v2 query engine has separate multi-stage stats that are enabled by default
    notSupportedInV2();
    JsonNode jsonNode =
        postQuery("SET trace = true; SELECT 1, \'test\', ArrDelay FROM " + getTableName() + " LIMIT 10");
    long countStarResult = 10;
    Assert.assertEquals(jsonNode.get("resultTable").get("rows").size(), 10);
    for (int rowId = 0; rowId < 10; rowId++) {
      Assert.assertEquals(jsonNode.get("resultTable").get("rows").get(rowId).get(0).asLong(), 1);
      Assert.assertEquals(jsonNode.get("resultTable").get("rows").get(rowId).get(1).asText(), "test");
    }
    Assert.assertTrue(jsonNode.get("exceptions").isEmpty());
    JsonNode traceInfo = jsonNode.get("traceInfo");
    Assert.assertEquals(traceInfo.size(), 2);
    Assert.assertTrue(traceInfo.has("localhost_O"));
    Assert.assertTrue(traceInfo.has("localhost_R"));
  }

  @Test(dataProvider = "useBothQueryEngines")
  public void testDropResults(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);
    final String query = String.format("SELECT * FROM %s limit 10", getTableName());
    final String resultTag = "resultTable";

    // dropResults=true - resultTable must not be in the response
    JsonNode jsonNode = postQueryWithOptions(query, "dropResults=true");
    Assert.assertFalse(jsonNode.has(resultTag));

    // dropResults=TrUE (case insensitive match) - resultTable must not be in the response
    Assert.assertFalse(postQueryWithOptions(query, "dropResults=TrUE").has(resultTag));

    // dropResults=truee - (anything other than true, is taken as false) - resultTable must be in the response
    Assert.assertTrue(postQueryWithOptions(query, "dropResults=truee").has(resultTag));
  }

  @Test(dataProvider = "useBothQueryEngines")
  public void testExplainDropResults(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);
    String resultTag = "resultTable";
    String query = String.format("EXPLAIN PLAN FOR SELECT * FROM %s limit 10", getTableName());

    // dropResults=true - resultTable must be in the response (it is a query plan)
    JsonNode jsonNode = postQueryWithOptions(query, "dropResults=true");
    Assert.assertTrue(jsonNode.has(resultTag));
    query = String.format("EXPLAIN PLAN WITHOUT IMPLEMENTATION FOR SELECT * FROM %s limit 10", getTableName());

    // dropResults=true - resultTable must be in the response (it is a query plan)
    jsonNode = postQueryWithOptions(query, "dropResults=true");
    Assert.assertTrue(jsonNode.has(resultTag));

    query = String.format("EXPLAIN IMPLEMENTATION PLAN FOR SELECT * FROM %s limit 10", getTableName());

    // dropResults=true - resultTable must be in the response (it is a query plan)
    jsonNode = postQueryWithOptions(query, "dropResults=true");
    Assert.assertTrue(jsonNode.has(resultTag));

    query = String.format("EXPLAIN PLAN FOR SELECT 1 + 1 FROM %s limit 10", getTableName());

    // dropResults=true - resultTable must be in the response (it is a query plan)
    jsonNode = postQueryWithOptions(query, "dropResults=true");
    Assert.assertTrue(jsonNode.has(resultTag));
  }

  @Test(dataProvider = "useBothQueryEngines")
  public void testHardcodedQueries(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);
    super.testHardcodedQueries();
  }

  @Test(dataProvider = "useBothQueryEngines")
  public void testQueriesFromQueryFile(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);
    // Some of the hardcoded queries in the query file need to be adapted for v2 (for instance, using the arrayToMV
    // with multi-value columns in filters / aggregations)
    notSupportedInV2();
    super.testQueriesFromQueryFile();
  }

  @Test(dataProvider = "useBothQueryEngines")
  public void testGeneratedQueries(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);
    super.testGeneratedQueries(true, useMultiStageQueryEngine);
  }

  @Test
  @Override
  public void testInstanceShutdown()
      throws Exception {
    super.testInstanceShutdown();
  }

  @Test
  @Override
  public void testBrokerResponseMetadata()
      throws Exception {
    super.testBrokerResponseMetadata();
  }

  @Test(dataProvider = "useBothQueryEngines")
  public void testVirtualColumnQueries(boolean useMultiStageQueryEngine)
      throws Exception {
    super.testVirtualColumnQueries();
  }

  @Test(dataProvider = "useBothQueryEngines")
  void testControllerQuerySubmit(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);
    // Hybrid Table
    @Language("sql")
    String query = "SELECT count(*) FROM " + getTableName();
    JsonNode response = postQueryToController(query);
    assertNoError(response);

    // Offline table
    String tableName = TableNameBuilder.OFFLINE.tableNameWithType(getTableName());
    query = "SELECT count(*) FROM " + tableName;
    response = postQueryToController(query);
    assertNoError(response);

    tableName = TableNameBuilder.REALTIME.tableNameWithType(getTableName());
    query = "SELECT count(*) FROM " + tableName;
    response = postQueryToController(query);
    assertNoError(response);

    query = "SELECT count(*) FROM unknown";
    response = postQueryToController(query);
    if (useMultiStageQueryEngine) {
      QueryAssert.assertThat(response).firstException().hasErrorCode(QueryErrorCode.TABLE_DOES_NOT_EXIST)
          .containsMessage("TableDoesNotExistError");
    } else {
      QueryAssert.assertThat(response).firstException().hasErrorCode(QueryErrorCode.BROKER_RESOURCE_MISSING)
          .containsMessage("BrokerResourceMissingError");
    }
  }

  @Test
  void testControllerJoinQuerySubmit()
      throws Exception {
    setUseMultiStageQueryEngine(true);
    // Hybrid Table
    @Language("sql")
    String query = "SELECT count(*) FROM unknown JOIN " + getTableName()
        + " ON unknown.FlightNum = " + getTableName() + ".FlightNum";
    JsonNode response = postQueryToController(query);
    QueryAssert.assertThat(response).firstException().hasErrorCode(QueryErrorCode.TABLE_DOES_NOT_EXIST)
        .containsMessage("TableDoesNotExistError");

    query = "SELECT count(*) FROM unknown_1 JOIN unknown_2  ON "
        + "unknown_1.FlightNum = unknown_2.FlightNum";
    response = postQueryToController(query);
    QueryAssert.assertThat(response).firstException().hasErrorCode(QueryErrorCode.TABLE_DOES_NOT_EXIST)
        .containsMessage("TableDoesNotExistError");
  }
}
