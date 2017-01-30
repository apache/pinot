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
package com.linkedin.pinot.integration.tests;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.linkedin.pinot.controller.helix.ControllerRequestURLBuilder;
import java.io.File;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import com.linkedin.pinot.common.data.Schema;
import com.linkedin.pinot.common.utils.FileUploadUtils;
import com.linkedin.pinot.common.utils.KafkaStarterUtils;
import junit.framework.Assert;


/**
 * Hybrid cluster integration test that uploads 8 months of data as offline and 6 months of data as realtime (with a
 * two month overlap) then deletes segments different ways to test delete APIs.
 *
 */
public class DeleteAPIHybridClusterIntegrationTest extends HybridClusterIntegrationTest {
  private static final Logger LOGGER = LoggerFactory.getLogger(RealtimeClusterIntegrationTest.class);
  private static final String TABLE_NAME = "mytable";
  private static final String TENANT_NAME = "TestTenant";
  private static int nRows;

  @BeforeClass
  public void setUp() throws Exception {
    super.setUp();
    org.json.JSONObject response = postQuery("select count(*) from 'mytable'");
    org.json.JSONArray aggregationResultsArray = response.getJSONArray("aggregationResults");
    org.json.JSONObject firstAggregationResult = aggregationResultsArray.getJSONObject(0);
    String pinotValue = firstAggregationResult.getString("value");
    nRows = Integer.parseInt(pinotValue);
  }

  private int numRowsReturned() throws Exception{
    org.json.JSONObject response = postQuery("select count(*) from 'mytable'");
    org.json.JSONArray aggregationResultsArray = response.getJSONArray("aggregationResults");
    org.json.JSONObject firstAggregationResult = aggregationResultsArray.getJSONObject(0);
    String pinotValue = firstAggregationResult.getString("value");
    return Integer.parseInt(pinotValue);
  }

  private void waitForTableReady() throws Exception {
    while (true) {
      int curOfflineRows = numRowsReturned();
      if (curOfflineRows != nRows) {
        Thread.sleep(1000);
      }
      else {
        break;
      }
    }
  }

  // @Test Commenting this out for now because of HLC/LLC
  public void deleteAllRealtimeSegmentsFromGetAPI() throws Exception{
    sendGetRequest(ControllerRequestURLBuilder.baseUrl(CONTROLLER_BASE_API_URL).
        forDeleteAllSegmentsWithTypeWithGetAPI(TABLE_NAME, "realtime"));
    Thread.sleep(3000);

    String postDeleteSegmentList = sendGetRequest(
        ControllerRequestURLBuilder.baseUrl(CONTROLLER_BASE_API_URL).forListAllSegments(TABLE_NAME));
    Assert.assertEquals(getSegmentsFromJson(postDeleteSegmentList, "realtime").size(), 1);
    Assert.assertNotSame(getSegmentsFromJson(postDeleteSegmentList, "offline").size(), 0);

    repushRealtimeSegments();
  }

  @Test
  public void deleteAllOfflineSegmentsFromGetAPI() throws Exception{
    sendGetRequest(ControllerRequestURLBuilder.baseUrl(CONTROLLER_BASE_API_URL).
        forDeleteAllSegmentsWithTypeWithGetAPI(TABLE_NAME, "offline"));
    Thread.sleep(3000);

    String postDeleteSegmentList = sendGetRequest(
        ControllerRequestURLBuilder.baseUrl(CONTROLLER_BASE_API_URL).forListAllSegments(TABLE_NAME));
    Assert.assertEquals(getSegmentsFromJson(postDeleteSegmentList, "offline"), null);
    Assert.assertNotSame(getSegmentsFromJson(postDeleteSegmentList, "realtime").size(), 0);

    repushOfflineSegments();
  }

  @Override
  public void testHardcodedQuerySet() {}

  @Test
  public void deleteOfflineSegmentFromGetAPI() throws Exception{
    String segmentList = sendGetRequest(
        ControllerRequestURLBuilder.baseUrl(CONTROLLER_BASE_API_URL).forListAllSegments(TABLE_NAME));
    JSONArray offlineSegmentsList = getSegmentsFromJson(segmentList, "offline");

    sendGetRequest(ControllerRequestURLBuilder.baseUrl(CONTROLLER_BASE_API_URL).
        forDeleteSegmentWithGetAPI(TABLE_NAME, offlineSegmentsList.get(0).toString(), "offline"));
    Thread.sleep(5000);

    String postDeleteSegmentList = sendGetRequest(
        ControllerRequestURLBuilder.baseUrl(CONTROLLER_BASE_API_URL).forListAllSegments(TABLE_NAME));
    JSONArray offlineSegmentsListReturn = getSegmentsFromJson(postDeleteSegmentList, "offline");
    offlineSegmentsList.remove(0);
    Assert.assertEquals(offlineSegmentsListReturn, offlineSegmentsList);
  }

  @Override
  public void testGeneratedQueriesWithMultiValues() {}

  @Test
  public void deleteRealtimeSegmentFromGetAPI() throws Exception{
    String segmentList = sendGetRequest(
        ControllerRequestURLBuilder.baseUrl(CONTROLLER_BASE_API_URL).forListAllSegments(TABLE_NAME));
    JSONArray realtimeSegmentsList = getSegmentsFromJson(segmentList, "realtime");

    sendGetRequest(ControllerRequestURLBuilder.baseUrl(CONTROLLER_BASE_API_URL).
        forDeleteSegmentWithGetAPI(TABLE_NAME, realtimeSegmentsList.get(0).toString(), "realtime"));
    Thread.sleep(3000);

    String postDeleteSegmentList = sendGetRequest(
        ControllerRequestURLBuilder.baseUrl(CONTROLLER_BASE_API_URL).forListAllSegments(TABLE_NAME));
    JSONArray realtimeSegmentsListReturn = getSegmentsFromJson(postDeleteSegmentList, "realtime");
    realtimeSegmentsList.remove(0);
    Assert.assertEquals(realtimeSegmentsListReturn, realtimeSegmentsList);
  }

  // @Test Commenting this out for now because of HLC/LLC
  public void deleteAllRealtimeSegmentsFromDeleteAPI() throws Exception{
    sendDeleteRequest(ControllerRequestURLBuilder.baseUrl(CONTROLLER_BASE_API_URL).
        forSegmentDeleteAllAPI(TABLE_NAME, "realtime"));
    Thread.sleep(3000);

    String postDeleteSegmentList = sendGetRequest(
        ControllerRequestURLBuilder.baseUrl(CONTROLLER_BASE_API_URL).forListAllSegments(TABLE_NAME));
    Assert.assertEquals(getSegmentsFromJson(postDeleteSegmentList, "realtime").size(), 1);
    Assert.assertNotSame(getSegmentsFromJson(postDeleteSegmentList, "offline").size(), 0);

     repushRealtimeSegments();
  }

    @Test
    public void deleteAllOfflineSegmentsFromDeleteAPI() throws Exception{
      sendDeleteRequest(ControllerRequestURLBuilder.baseUrl(CONTROLLER_BASE_API_URL).
          forSegmentDeleteAllAPI(TABLE_NAME, "offline"));
      Thread.sleep(3000);

      String postDeleteSegmentList = sendGetRequest(
          ControllerRequestURLBuilder.baseUrl(CONTROLLER_BASE_API_URL).forListAllSegments(TABLE_NAME));
      Assert.assertEquals(getSegmentsFromJson(postDeleteSegmentList, "offline"), null);
      Assert.assertNotSame(getSegmentsFromJson(postDeleteSegmentList, "realtime").size(), 0);

      repushOfflineSegments();
    }

    @Test
    public void deleteOfflineSegmentFromDeleteAPI() throws Exception{
      String segmentList = sendGetRequest(
          ControllerRequestURLBuilder.baseUrl(CONTROLLER_BASE_API_URL).forListAllSegments(TABLE_NAME));
      JSONArray offlineSegmentsList = getSegmentsFromJson(segmentList, "offline");

      sendDeleteRequest(ControllerRequestURLBuilder.baseUrl(CONTROLLER_BASE_API_URL).
          forSegmentDeleteAPI(TABLE_NAME, offlineSegmentsList.get(0).toString(), "offline"));
      Thread.sleep(3000);

      String postDeleteSegmentList = sendGetRequest(
          ControllerRequestURLBuilder.baseUrl(CONTROLLER_BASE_API_URL).forListAllSegments(TABLE_NAME));
      JSONArray offlineSegmentsListReturn = getSegmentsFromJson(postDeleteSegmentList, "offline");
      offlineSegmentsList.remove(0);
      Assert.assertEquals(offlineSegmentsListReturn, offlineSegmentsList);
    }

  @Test
  public void deleteRealtimeSegmentFromDeleteAPI() throws Exception{
    String segmentList = sendGetRequest(
        ControllerRequestURLBuilder.baseUrl(CONTROLLER_BASE_API_URL).forListAllSegments(TABLE_NAME));
    JSONArray realtimeSegmentsList = getSegmentsFromJson(segmentList, "realtime");

    sendDeleteRequest(ControllerRequestURLBuilder.baseUrl(CONTROLLER_BASE_API_URL).
        forSegmentDeleteAPI(TABLE_NAME, realtimeSegmentsList.get(0).toString(), "realtime"));
    Thread.sleep(3000);

    String postDeleteSegmentList = sendGetRequest(
        ControllerRequestURLBuilder.baseUrl(CONTROLLER_BASE_API_URL).forListAllSegments(TABLE_NAME));
    JSONArray realtimeSegmentsListReturn = getSegmentsFromJson(postDeleteSegmentList, "realtime");
    realtimeSegmentsList.remove(0);
    Assert.assertEquals(realtimeSegmentsListReturn, realtimeSegmentsList);
  }

  private JSONArray getSegmentsFromJson(String json, String type) {
    if (type.equalsIgnoreCase("realtime")) {
      com.alibaba.fastjson.JSONObject jsonObject2 = (JSONObject) JSON.parseArray(json).get(0);
      String jsonObject3 = (String) jsonObject2.get("segments");
      return (JSONArray) JSON.parseObject(jsonObject3).get("Server_172.21.224.26_8099");
    } else {
      com.alibaba.fastjson.JSONObject jsonObject2 = (JSONObject) JSON.parseArray(json).get(1);
      String jsonObject3 = (String) jsonObject2.get("segments");
      return (JSONArray) JSON.parseObject(jsonObject3).get("Server_172.21.224.26_8098");
    }
  }
  private void repushOfflineSegments() throws Exception {
    for (String segmentName : _tarDir.list()) {
      File file = new File(_tarDir, segmentName);
      FileUploadUtils.sendSegmentFile("localhost", "8998", segmentName, file, file.length());
    }

    // Wait for all offline segments to be online
    waitForTableReady();
  }

  @Override
  public void testBrokerDebugOutput() {}


  private void repushRealtimeSegments() throws Exception {
    File schemaFile = getSchemaFile();
    Schema schema = Schema.fromFile(schemaFile);
    addSchema(schemaFile, schema.getSchemaName());
    final List<String> invertedIndexColumns = makeInvertedIndexColumns();
    final String sortedColumn = makeSortedColumn();
    final List<File> avroFiles = getAllAvroFiles();

    // delete the realtime table and re-create it
    sendDeleteRequest(ControllerRequestURLBuilder.baseUrl(CONTROLLER_BASE_API_URL).forDeleteTableWithType(TABLE_NAME, "realtime"));
    addRealtimeTable("mytable", "DaysSinceEpoch", "daysSinceEpoch", 900, "Days", KafkaStarterUtils.DEFAULT_ZK_STR,
        KAFKA_TOPIC, schema.getSchemaName(), "TestTenant", "TestTenant",
        avroFiles.get(0), 1000000, sortedColumn, new ArrayList<String>(), null);

    waitForTableReady();
  }
}
