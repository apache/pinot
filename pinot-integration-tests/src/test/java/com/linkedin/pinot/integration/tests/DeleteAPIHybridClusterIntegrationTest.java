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
import com.linkedin.pinot.common.metadata.ZKMetadataProvider;
import com.linkedin.pinot.common.metadata.segment.OfflineSegmentZKMetadata;
import com.linkedin.pinot.common.metadata.segment.RealtimeSegmentZKMetadata;
import com.linkedin.pinot.common.utils.CommonConstants;
import com.linkedin.pinot.controller.ControllerConf;
import com.linkedin.pinot.controller.helix.ControllerRequestURLBuilder;
import com.linkedin.pinot.controller.helix.ControllerTestUtils;
import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
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
 * two month overlap) then deletes segments different ways to test delete APIs. Expects at least 3 realtime segments
 * and at least 3 offline segments.
 *
 */
public class DeleteAPIHybridClusterIntegrationTest extends HybridClusterIntegrationTest {
  private static final Logger LOGGER = LoggerFactory.getLogger(RealtimeClusterIntegrationTest.class);
  private String TABLE_NAME;
  private long nOfflineRows;
  private long nRealtimeRows;
  ControllerConf config;

  @BeforeClass
  public void setUp() throws Exception {
    super.setUp();
    TABLE_NAME = super.getTableName();
    nOfflineRows = numRowsReturned(CommonConstants.Helix.TableType.OFFLINE);
    nRealtimeRows = numRowsReturned(CommonConstants.Helix.TableType.REALTIME);
    config = ControllerTestUtils.getDefaultControllerConfiguration();
  }

  private long numRowsReturned(CommonConstants.Helix.TableType tableType) throws Exception {
    org.json.JSONObject response = postQuery("select count(*) from '" + TABLE_NAME + "_" + tableType + "'");
    if (response.get("numDocsScanned").equals(new Integer(0))) {
      return 0;
    }
    else {
      // Throws a null pointer exception when there are no rows because it can't find "aggregationResults"
      String pinotValue = ((org.json.JSONArray) response.get("aggregationResults")).getJSONObject(0).get("value").toString();
      return Long.parseLong(pinotValue);
    }
  }

  private boolean waitForNumRows(long numRows, CommonConstants.Helix.TableType tableType) throws Exception {
    long start = System.currentTimeMillis();
    long end = start + 60 * 1000;
    while (System.currentTimeMillis() < end) {
      if (numRowsReturned(tableType) == numRows) {
        return true;
      }
      Thread.sleep(600);
    }
    Assert.fail("Operation took too long");
    return false;
  }

  private long getNumRows(CommonConstants.Helix.TableType type) throws Exception{
    org.json.JSONObject response = postQuery("select count(*) from '" + TABLE_NAME + "_" + type + "'");

    long start = System.currentTimeMillis();
    long end = start + 60 * 1000;
    while (System.currentTimeMillis() < end) {
      if (!response.get("numDocsScanned").equals(new Integer(0))) {
        // Throws a null pointer exception when there are no rows because it can't find "aggregationResults"
        String pinotValue = ((org.json.JSONArray) response.get("aggregationResults")).getJSONObject(0).get("value").toString();
        return Long.parseLong(pinotValue);
      }
      Thread.sleep(200);
    }
    return 0L;
  }
  @Override // Leaving this out because it is done in the superclass
  public void testGeneratedQueriesWithMultiValues() {}

  @Override // Leaving this out because it is done in the superclass
  public void testHardcodedQuerySet() {}

  @Override // Leaving this out because it is done in the superclass
  public void testBrokerDebugOutput() {}

  @Test
  public void deleteRealtimeSegmentFromGetAPI() throws Exception {
    long currRealtimeRows = getNumRows(CommonConstants.Helix.TableType.REALTIME);

    String segmentList = sendGetRequest(ControllerRequestURLBuilder.baseUrl(CONTROLLER_BASE_API_URL).
        forSegmentListAPIWithTableType(TABLE_NAME, CommonConstants.Helix.TableType.REALTIME.toString()));
    JSONArray realtimeSegmentsList = getSegmentsFromJsonSegmentAPI(segmentList, CommonConstants.Helix.TableType.REALTIME.toString());

    String removedSegment = realtimeSegmentsList.get(0).toString();
    long removedSegmentRows = getNumRowsFromRealtimeMetadata(removedSegment);
    Assert.assertNotSame(removedSegmentRows, 0L);

    sendGetRequest(ControllerRequestURLBuilder.baseUrl(CONTROLLER_BASE_API_URL).
        forDeleteSegmentWithGetAPI(TABLE_NAME, removedSegment, CommonConstants.Helix.TableType.REALTIME.toString()));

    waitForNumRows(currRealtimeRows - removedSegmentRows, CommonConstants.Helix.TableType.REALTIME);

    String postDeleteSegmentList = sendGetRequest(
        ControllerRequestURLBuilder.baseUrl(CONTROLLER_BASE_API_URL).forSegmentListAPIWithTableType(TABLE_NAME, CommonConstants.Helix.TableType.REALTIME.toString()));
    JSONArray realtimeSegmentsListReturn = getSegmentsFromJsonSegmentAPI(postDeleteSegmentList, CommonConstants.Helix.TableType.REALTIME.toString());
    realtimeSegmentsList.remove(removedSegment);
    Assert.assertEquals(realtimeSegmentsListReturn, realtimeSegmentsList);
  }

  @Test
  public void deleteRealtimeSegmentFromDeleteAPI() throws Exception {
    long currRealtimeRows = getNumRows(CommonConstants.Helix.TableType.REALTIME);

    String segmentList = sendGetRequest(
        ControllerRequestURLBuilder.baseUrl(CONTROLLER_BASE_API_URL).forSegmentListAPIWithTableType(TABLE_NAME, CommonConstants.Helix.TableType.REALTIME.toString()));
    JSONArray realtimeSegmentsList =  getSegmentsFromJsonSegmentAPI(segmentList, CommonConstants.Helix.TableType.REALTIME.toString());

    String removedSegment = realtimeSegmentsList.get(0).toString();
    long removedSegmentRows = getNumRowsFromRealtimeMetadata(removedSegment);
    Assert.assertNotSame(removedSegmentRows, 0L);

    sendDeleteRequest(ControllerRequestURLBuilder.baseUrl(CONTROLLER_BASE_API_URL).
        forSegmentDeleteAPI(TABLE_NAME, removedSegment, CommonConstants.Helix.TableType.REALTIME.toString()));

    waitForNumRows(currRealtimeRows - removedSegmentRows, CommonConstants.Helix.TableType.REALTIME);

    String postDeleteSegmentList = sendGetRequest(
        ControllerRequestURLBuilder.baseUrl(CONTROLLER_BASE_API_URL).forSegmentListAPIWithTableType(TABLE_NAME,
            CommonConstants.Helix.TableType.REALTIME.toString()));
    JSONArray realtimeSegmentsListReturn = getSegmentsFromJsonSegmentAPI(postDeleteSegmentList, CommonConstants.Helix.TableType.REALTIME.toString());
    realtimeSegmentsList.remove(removedSegment);
    Assert.assertEquals(realtimeSegmentsListReturn, realtimeSegmentsList);
  }

  // @Test TODO: Add back when we use LLC only
  public void deleteAllRealtimeSegmentsFromGetAPI() throws Exception {
    sendGetRequest(ControllerRequestURLBuilder.baseUrl(CONTROLLER_BASE_API_URL).
        forDeleteAllSegmentsWithTypeWithGetAPI(TABLE_NAME, CommonConstants.Helix.TableType.REALTIME.toString()));

    waitForNumRows(0, CommonConstants.Helix.TableType.REALTIME);

    String postDeleteSegmentList = sendGetRequest(
        ControllerRequestURLBuilder.baseUrl(CONTROLLER_BASE_API_URL).forListAllSegments(TABLE_NAME));
    Assert.assertEquals(getSegmentsFromJsonSegmentAPI(postDeleteSegmentList, CommonConstants.Helix.TableType.REALTIME.toString()).size(), 1);
    Assert.assertNotSame(getSegmentsFromJsonSegmentAPI(postDeleteSegmentList, CommonConstants.Helix.TableType.OFFLINE.toString()).size(), 0);

    repushRealtimeSegments();
  }

  // @Test TODO: Add back when we use LLC only
  public void deleteAllRealtimeSegmentsFromDeleteAPI() throws Exception {
    sendDeleteRequest(ControllerRequestURLBuilder.baseUrl(CONTROLLER_BASE_API_URL).
        forSegmentDeleteAllAPI(TABLE_NAME, CommonConstants.Helix.TableType.REALTIME.toString()));

    waitForNumRows(0, CommonConstants.Helix.TableType.REALTIME);

    String postDeleteSegmentList = sendGetRequest(
        ControllerRequestURLBuilder.baseUrl(CONTROLLER_BASE_API_URL).forListAllSegments(TABLE_NAME));
    Assert.assertEquals(getSegmentsFromJsonSegmentAPI(postDeleteSegmentList, CommonConstants.Helix.TableType.REALTIME.toString()).size(), 1);
    Assert.assertNotSame(getSegmentsFromJsonSegmentAPI(postDeleteSegmentList, CommonConstants.Helix.TableType.OFFLINE.toString()).size(), 0);

    repushRealtimeSegments();
  }

  @Test
  public void deleteFromDeleteAPI() throws Exception {
    String segmentList = sendGetRequest(ControllerRequestURLBuilder.baseUrl(CONTROLLER_BASE_API_URL).
        forSegmentListAPIWithTableType(TABLE_NAME, CommonConstants.Helix.TableType.OFFLINE.toString()));
    JSONArray offlineSegmentsList = getSegmentsFromJsonSegmentAPI(segmentList, CommonConstants.Helix.TableType.OFFLINE.toString());
    Assert.assertNotNull(offlineSegmentsList);

    String removedSegment = offlineSegmentsList.get(0).toString();
    long removedSegmentRows = getNumRowsFromOfflineMetadata(removedSegment);
    Assert.assertNotSame(removedSegmentRows, 0L);

    sendDeleteRequest(ControllerRequestURLBuilder.baseUrl(CONTROLLER_BASE_API_URL).
        forSegmentDeleteAPI(TABLE_NAME, removedSegment, CommonConstants.Helix.TableType.OFFLINE.toString()));

    waitForNumRows(nOfflineRows - removedSegmentRows, CommonConstants.Helix.TableType.OFFLINE);

    String postDeleteSegmentList = sendGetRequest(ControllerRequestURLBuilder.baseUrl(CONTROLLER_BASE_API_URL).
        forSegmentListAPIWithTableType(TABLE_NAME, CommonConstants.Helix.TableType.OFFLINE.toString()));
    JSONArray offlineSegmentsListReturn = getSegmentsFromJsonSegmentAPI(postDeleteSegmentList, CommonConstants.Helix.TableType.OFFLINE.toString());
    offlineSegmentsList.remove(removedSegment);
    Assert.assertEquals(offlineSegmentsListReturn, offlineSegmentsList);

    sendDeleteRequest(ControllerRequestURLBuilder.baseUrl(CONTROLLER_BASE_API_URL).
        forSegmentDeleteAllAPI(TABLE_NAME, CommonConstants.Helix.TableType.OFFLINE.toString()));

    waitForNumRows(0, CommonConstants.Helix.TableType.OFFLINE);

    String postDeleteSegmentListAll = sendGetRequest(ControllerRequestURLBuilder.baseUrl(CONTROLLER_BASE_API_URL).
        forSegmentListAPIWithTableType(TABLE_NAME, CommonConstants.Helix.TableType.OFFLINE.toString()));

    Assert.assertEquals(getSegmentsFromJsonSegmentAPI(postDeleteSegmentListAll, CommonConstants.Helix.TableType.OFFLINE.toString()),
        Collections.emptyList());

    waitForSegmentsToBeInDeleteDirectory();
    repushOfflineSegments();
  }

  @Test
  public void deleteFromGetAPI() throws Exception {
    String segmentList = sendGetRequest(ControllerRequestURLBuilder.baseUrl(CONTROLLER_BASE_API_URL).
        forSegmentListAPIWithTableType(TABLE_NAME, CommonConstants.Helix.TableType.OFFLINE.toString()));

    JSONArray offlineSegmentsList = getSegmentsFromJsonSegmentAPI(segmentList, CommonConstants.Helix.TableType.OFFLINE.toString());

    String removedSegment = offlineSegmentsList.get(0).toString();

    long removedSegmentRows = getNumRowsFromOfflineMetadata(removedSegment);
    Assert.assertNotSame(removedSegmentRows, 0L);

    sendGetRequest(ControllerRequestURLBuilder.baseUrl(CONTROLLER_BASE_API_URL).
        forDeleteSegmentWithGetAPI(TABLE_NAME, removedSegment, CommonConstants.Helix.TableType.OFFLINE.toString()));

    waitForNumRows(nOfflineRows - removedSegmentRows, CommonConstants.Helix.TableType.OFFLINE);

    String postDeleteSegmentList = sendGetRequest(ControllerRequestURLBuilder.baseUrl(CONTROLLER_BASE_API_URL).
        forSegmentListAPIWithTableType(TABLE_NAME, CommonConstants.Helix.TableType.OFFLINE.toString()));
    JSONArray offlineSegmentsListReturn = getSegmentsFromJsonSegmentAPI(postDeleteSegmentList, CommonConstants.Helix.TableType.OFFLINE.toString());
    offlineSegmentsList.remove(removedSegment);
    Assert.assertEquals(offlineSegmentsListReturn, offlineSegmentsList);

    sendGetRequest(ControllerRequestURLBuilder.baseUrl(CONTROLLER_BASE_API_URL).
        forDeleteAllSegmentsWithTypeWithGetAPI(TABLE_NAME, CommonConstants.Helix.TableType.OFFLINE.toString()));

    waitForNumRows(0, CommonConstants.Helix.TableType.OFFLINE);

    String postDeleteSegmentListAll = sendGetRequest(ControllerRequestURLBuilder.baseUrl(CONTROLLER_BASE_API_URL).
        forSegmentListAPIWithTableType(TABLE_NAME, CommonConstants.Helix.TableType.OFFLINE.toString()));

    Assert.assertEquals(getSegmentsFromJsonSegmentAPI(postDeleteSegmentListAll, CommonConstants.Helix.TableType.OFFLINE.toString()), Collections.emptyList());

    waitForSegmentsToBeInDeleteDirectory();
    repushOfflineSegments();
  }

  private void waitForSegmentsToBeInDeleteDirectory() throws Exception{
    long start = System.currentTimeMillis();
    long end = start + 60 * 1000;
    while (System.currentTimeMillis() < end) {
      if (config.getDataDir().length() == 8) {
        break;
      }
      Thread.sleep(200);
    }
  }
  private long getNumRowsFromOfflineMetadata(String segmentName) throws Exception {
    OfflineSegmentZKMetadata segmentZKMetadata = ZKMetadataProvider.getOfflineSegmentZKMetadata(_propertyStore, TABLE_NAME, segmentName);
    return segmentZKMetadata.getTotalRawDocs();
  }

  private long getNumRowsFromRealtimeMetadata(String segmentName) {
    RealtimeSegmentZKMetadata segmentZKMetadata = ZKMetadataProvider.getRealtimeSegmentZKMetadata(_propertyStore, TABLE_NAME, segmentName);
    return segmentZKMetadata.getTotalRawDocs();
  }

  private com.alibaba.fastjson.JSONArray getSegmentsFromJsonSegmentAPI(String json, String type) throws Exception {
    JSONObject tableTypeAndSegments = (JSONObject)JSON.parseArray(json).get(0);
    long start = System.currentTimeMillis();
    long end = start + 60 * 1000;
    while (System.currentTimeMillis() < end) {
      if (tableTypeAndSegments.get(type) != null) {
        return (JSONArray) tableTypeAndSegments.get(type);
      }
      Thread.sleep(200);
    }

    return null;
  }

  private void repushOfflineSegments() throws Exception {
    for (String segmentName : _tarDir.list()) {
      File file = new File(_tarDir, segmentName);
      FileUploadUtils.sendSegmentFile("localhost", "8998", segmentName, file, file.length());
    }
    waitForNumRows(nOfflineRows, CommonConstants.Helix.TableType.OFFLINE);
  }

  private void repushRealtimeSegments() throws Exception {
    File schemaFile = getSchemaFile();
    Schema schema = Schema.fromFile(schemaFile);
    addSchema(schemaFile, schema.getSchemaName());
    final List<String> invertedIndexColumns = makeInvertedIndexColumns();
    final String sortedColumn = makeSortedColumn();
    final List<File> avroFiles = getAllAvroFiles();

    // delete the realtime table and re-create it
    sendDeleteRequest(ControllerRequestURLBuilder.baseUrl(CONTROLLER_BASE_API_URL).forDeleteTableWithType(TABLE_NAME, CommonConstants.Helix.TableType.REALTIME.toString()));
    addRealtimeTable("mytable", "DaysSinceEpoch", "daysSinceEpoch", 900, "Days", KafkaStarterUtils.DEFAULT_ZK_STR,
        KAFKA_TOPIC, schema.getSchemaName(), "TestTenant", "TestTenant",
        avroFiles.get(0), 1000000, sortedColumn, new ArrayList<String>(), null);

    waitForNumRows(nRealtimeRows, CommonConstants.Helix.TableType.REALTIME);
  }
}
