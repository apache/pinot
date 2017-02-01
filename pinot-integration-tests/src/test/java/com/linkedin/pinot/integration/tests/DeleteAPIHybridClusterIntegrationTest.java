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
  private String TABLE_NAME;
  private int nOfflineRows;
  private int nRealtimeRows;

  @BeforeClass
  public void setUp() throws Exception {
    super.setUp();
    TABLE_NAME = super.getTableName();
    nOfflineRows = numRowsReturned("REALTIME");
    nRealtimeRows = numRowsReturned("OFFLINE");
  }

  private int numRowsReturned(String tableType) throws Exception {
    org.json.JSONObject response = postQuery("select count(*) from '" + TABLE_NAME + "_" + tableType + "'");
    long start = System.currentTimeMillis();
    long end = start + 60 * 1000;
    while (System.currentTimeMillis() < end) {
      if (!response.get("numDocsScanned").equals(new Integer(0))) {
        String pinotValue = ((org.json.JSONArray) response.get("aggregationResults")).getJSONObject(0).get("value").toString();
        return Integer.parseInt(pinotValue);
      }
      Thread.sleep(200);
    }
    return 0;
  }

  private boolean checkAllRowsDeleted(String tableType) throws Exception {
    org.json.JSONObject response = postQuery("select count(*) from '" + TABLE_NAME + "_" + tableType + "'");
    long start = System.currentTimeMillis();
    long end = start + 60 * 1000;
    while (System.currentTimeMillis() < end) {
      if (response.get("numDocsScanned").equals(new Integer(0))) {
        return true;
      }
      Thread.sleep(200);
    }
    return false;
  }

  private void waitForTableReady() throws Exception {
    long start = System.currentTimeMillis();
    long end = start + 60 * 1000;
    while (System.currentTimeMillis() < end) {
      int curOfflineRows = numRowsReturned("offline");
      if (curOfflineRows != nOfflineRows) {
        Thread.sleep(200);
      } else {
        break;
      }
    }
  }

  // @Test TODO: Add back when we use LLC only
  public void deleteAllRealtimeSegmentsFromGetAPI() throws Exception{
    sendGetRequest(ControllerRequestURLBuilder.baseUrl(CONTROLLER_BASE_API_URL).
        forDeleteAllSegmentsWithTypeWithGetAPI(TABLE_NAME, "realtime"));

    checkAllRowsDeleted("REALTIME");

    String postDeleteSegmentList = sendGetRequest(
        ControllerRequestURLBuilder.baseUrl(CONTROLLER_BASE_API_URL).forListAllSegments(TABLE_NAME));
    Assert.assertEquals(getSegmentsFromJson(postDeleteSegmentList, "realtime").size(), 1);
    Assert.assertNotSame(getSegmentsFromJson(postDeleteSegmentList, "offline").size(), 0);

    repushRealtimeSegments();
  }

  private boolean checkOneRowDeleted(String tableType) throws Exception {
    org.json.JSONObject response = postQuery("select count(*) from '" + TABLE_NAME + "_" + tableType + "'");
    long start = System.currentTimeMillis();
    long end = start + 60 * 1000;
    while (System.currentTimeMillis() < end) {
      if (tableType.equals("REALTIME")) {
        if (Integer.parseInt(((org.json.JSONArray) response.get("aggregationResults")).getJSONObject(0).get("value").toString()) != nRealtimeRows) {
          return true;
        }
      } else {
        if (Integer.parseInt(((org.json.JSONArray) response.get("aggregationResults")).getJSONObject(0).get("value").toString()) != nOfflineRows) {
          return true;
        }
      }
      Thread.sleep(200);
    }
    return false;
  }

  @Test
  public void deleteAllOfflineSegmentsFromGetAPI() throws Exception{
    sendGetRequest(ControllerRequestURLBuilder.baseUrl(CONTROLLER_BASE_API_URL).
        forDeleteAllSegmentsWithTypeWithGetAPI(TABLE_NAME, "offline"));

    checkAllRowsDeleted("OFFLINE");

    String postDeleteSegmentList = sendGetRequest(
        ControllerRequestURLBuilder.baseUrl(CONTROLLER_BASE_API_URL).forListAllSegments(TABLE_NAME));
    Assert.assertEquals(getSegmentsFromJson(postDeleteSegmentList, "offline"), null);
    Assert.assertNotSame(getSegmentsFromJson(postDeleteSegmentList, "realtime").size(), 0);

    repushOfflineSegments();
  }

  @Override // Leaving this out because it is done in the superclass
  public void testHardcodedQuerySet() {}

  @Test
  public void deleteOfflineSegmentFromGetAPI() throws Exception{
    String segmentList = sendGetRequest(
        ControllerRequestURLBuilder.baseUrl(CONTROLLER_BASE_API_URL).forListAllSegments(TABLE_NAME));
    JSONArray offlineSegmentsList = getSegmentsFromJson(segmentList, "offline");

    String removedSegment = offlineSegmentsList.get(0).toString();

    sendGetRequest(ControllerRequestURLBuilder.baseUrl(CONTROLLER_BASE_API_URL).
        forDeleteSegmentWithGetAPI(TABLE_NAME, removedSegment, "offline"));

    checkOneRowDeleted("OFFLINE");

    String postDeleteSegmentList = sendGetRequest(
        ControllerRequestURLBuilder.baseUrl(CONTROLLER_BASE_API_URL).forListAllSegments(TABLE_NAME));
    JSONArray offlineSegmentsListReturn = getSegmentsFromJson(postDeleteSegmentList, "offline");
    offlineSegmentsList.remove(removedSegment);
    Assert.assertEquals(offlineSegmentsListReturn, offlineSegmentsList);
  }

  @Override // Leaving this out because it is done in the superclass
  public void testGeneratedQueriesWithMultiValues() {}

  @Test
  public void deleteRealtimeSegmentFromGetAPI() throws Exception{
    String segmentList = sendGetRequest(
        ControllerRequestURLBuilder.baseUrl(CONTROLLER_BASE_API_URL).forListAllSegments(TABLE_NAME));
    JSONArray realtimeSegmentsList = getSegmentsFromJson(segmentList, "realtime");

    String removedSegment = realtimeSegmentsList.get(0).toString();

    sendGetRequest(ControllerRequestURLBuilder.baseUrl(CONTROLLER_BASE_API_URL).
        forDeleteSegmentWithGetAPI(TABLE_NAME, removedSegment, "realtime"));

    checkOneRowDeleted("REALTIME");

    String postDeleteSegmentList = sendGetRequest(
        ControllerRequestURLBuilder.baseUrl(CONTROLLER_BASE_API_URL).forListAllSegments(TABLE_NAME));
    JSONArray realtimeSegmentsListReturn = getSegmentsFromJson(postDeleteSegmentList, "realtime");
    realtimeSegmentsList.remove(removedSegment);
    Assert.assertEquals(realtimeSegmentsListReturn, realtimeSegmentsList);
  }

  // @Test TODO: Add back when we use LLC only
  public void deleteAllRealtimeSegmentsFromDeleteAPI() throws Exception{
    sendDeleteRequest(ControllerRequestURLBuilder.baseUrl(CONTROLLER_BASE_API_URL).
        forSegmentDeleteAllAPI(TABLE_NAME, "realtime"));

    checkAllRowsDeleted("REALTIME");

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

      checkAllRowsDeleted("OFFLINE");

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

      String removedSegment = offlineSegmentsList.get(0).toString();

      sendDeleteRequest(ControllerRequestURLBuilder.baseUrl(CONTROLLER_BASE_API_URL).
          forSegmentDeleteAPI(TABLE_NAME, removedSegment, "offline"));

      checkOneRowDeleted("OFFLINE");

      String postDeleteSegmentList = sendGetRequest(
          ControllerRequestURLBuilder.baseUrl(CONTROLLER_BASE_API_URL).forListAllSegments(TABLE_NAME));
      JSONArray offlineSegmentsListReturn = getSegmentsFromJson(postDeleteSegmentList, "offline");
      offlineSegmentsList.remove(removedSegment);
      Assert.assertEquals(offlineSegmentsListReturn, offlineSegmentsList);
    }

  @Test
  public void deleteRealtimeSegmentFromDeleteAPI() throws Exception{
    String segmentList = sendGetRequest(
        ControllerRequestURLBuilder.baseUrl(CONTROLLER_BASE_API_URL).forListAllSegments(TABLE_NAME));
    JSONArray realtimeSegmentsList = getSegmentsFromJson(segmentList, "realtime");

    String removedSegment = realtimeSegmentsList.get(0).toString();

    sendDeleteRequest(ControllerRequestURLBuilder.baseUrl(CONTROLLER_BASE_API_URL).
        forSegmentDeleteAPI(TABLE_NAME, removedSegment, "realtime"));

    checkOneRowDeleted("REALTIME");

    String postDeleteSegmentList = sendGetRequest(
        ControllerRequestURLBuilder.baseUrl(CONTROLLER_BASE_API_URL).forListAllSegments(TABLE_NAME));
    JSONArray realtimeSegmentsListReturn = getSegmentsFromJson(postDeleteSegmentList, "realtime");
    realtimeSegmentsList.remove(removedSegment);
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

  @Override // Leaving this out because it is done in the superclass
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
