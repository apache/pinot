/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
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
import java.util.Collections;
import junit.framework.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


/**
 * Hybrid cluster integration test that uploads 8 months of data as offline and 6 months of data as realtime (with a
 * two month overlap) then deletes segments different ways to test delete APIs. Expects at least 3 realtime segments
 * and at least 3 offline segments.
 *
 */
// TODO: clean up this test
public class DeleteAPIHybridClusterIntegrationTest extends HybridClusterIntegrationTest {
  private String TABLE_NAME;
  private long nOfflineRows;

  @BeforeClass
  public void setUp() throws Exception {
    super.setUp();
    TABLE_NAME = super.getTableName();
    nOfflineRows = numRowsReturned(CommonConstants.Helix.TableType.OFFLINE);
  }

  private long numRowsReturned(CommonConstants.Helix.TableType tableType) throws Exception {
    org.json.JSONObject response = postQuery("select count(*) from '" + TABLE_NAME + "_" + tableType + "'");
    if (response.get("numDocsScanned").equals(new Integer(0))) {
      return 0;
    } else {
      // Throws a null pointer exception when there are no rows because it can't find "aggregationResults"
      String pinotValue =
          ((org.json.JSONArray) response.get("aggregationResults")).getJSONObject(0).get("value").toString();
      return Long.parseLong(pinotValue);
    }
  }

  // TODO: Find ways to refactor waitForNumRows and waitForSegmentsToBeInDeleteDirectory
  private void waitForNumRows(long numRows, CommonConstants.Helix.TableType tableType) throws Exception {
    long start = System.currentTimeMillis();
    long end = start + 60 * 1000;
    while (System.currentTimeMillis() < end) {
      if (numRowsReturned(tableType) == numRows) {
        return;
      }
      Thread.sleep(200);
    }
    Assert.fail("Operation took too long");
  }

  private void waitForSegmentsToBeInDeleteDirectory() throws Exception {
    long start = System.currentTimeMillis();
    long end = start + 60 * 1000;
    while (System.currentTimeMillis() < end) {
      if (ZKMetadataProvider.getOfflineSegmentZKMetadataListForTable(_propertyStore, TABLE_NAME).size() == 0) {
        // Wait for actual file to be deleted. This doesn't currently work because .tar.gz files don't get deleted.
        Thread.sleep(300);
        return;
      }
      Thread.sleep(200);
    }
    Assert.fail("Operation took too long");
  }

  @Override // Leaving this out because it is done in the superclass
  public void testGeneratedQueriesWithMultiValues() {
  }

  @Override // Leaving this out because it is done in the superclass
  public void testQueriesFromQueryFile() {
  }

  @Override // Leaving this out because it is done in the superclass
  public void testBrokerDebugOutput() {
  }

  @Test
  public void deleteRealtimeSegmentFromGetAPI() throws Exception {
    long currRealtimeRows = numRowsReturned(CommonConstants.Helix.TableType.REALTIME);

    String segmentList = sendGetRequest(_controllerRequestURLBuilder.
        forSegmentListAPIWithTableType(TABLE_NAME, CommonConstants.Helix.TableType.REALTIME.toString()));
    JSONArray realtimeSegmentsList =
        getSegmentsFromJsonSegmentAPI(segmentList, CommonConstants.Helix.TableType.REALTIME.toString());

    String removedSegment = realtimeSegmentsList.get(0).toString();
    long removedSegmentRows = getNumRowsFromRealtimeMetadata(removedSegment);
    Assert.assertNotSame(removedSegmentRows, 0L);

    sendGetRequest(_controllerRequestURLBuilder.
        forDeleteSegmentWithGetAPI(TABLE_NAME, removedSegment, CommonConstants.Helix.TableType.REALTIME.toString()));

    waitForNumRows(currRealtimeRows - removedSegmentRows, CommonConstants.Helix.TableType.REALTIME);

    String postDeleteSegmentList = sendGetRequest(
        _controllerRequestURLBuilder.forSegmentListAPIWithTableType(TABLE_NAME,
            CommonConstants.Helix.TableType.REALTIME.toString()));
    JSONArray realtimeSegmentsListReturn =
        getSegmentsFromJsonSegmentAPI(postDeleteSegmentList, CommonConstants.Helix.TableType.REALTIME.toString());
    realtimeSegmentsList.remove(removedSegment);
    Assert.assertEquals(realtimeSegmentsListReturn, realtimeSegmentsList);
  }

  @Test
  public void deleteRealtimeSegmentFromDeleteAPI() throws Exception {
    long currRealtimeRows = numRowsReturned(CommonConstants.Helix.TableType.REALTIME);

    String segmentList = sendGetRequest(_controllerRequestURLBuilder.forSegmentListAPIWithTableType(TABLE_NAME,
        CommonConstants.Helix.TableType.REALTIME.toString()));
    JSONArray realtimeSegmentsList =
        getSegmentsFromJsonSegmentAPI(segmentList, CommonConstants.Helix.TableType.REALTIME.toString());

    String removedSegment = realtimeSegmentsList.get(0).toString();
    long removedSegmentRows = getNumRowsFromRealtimeMetadata(removedSegment);
    Assert.assertNotSame(removedSegmentRows, 0L);

    sendDeleteRequest(_controllerRequestURLBuilder.
        forSegmentDeleteAPI(TABLE_NAME, removedSegment, CommonConstants.Helix.TableType.REALTIME.toString()));

    waitForNumRows(currRealtimeRows - removedSegmentRows, CommonConstants.Helix.TableType.REALTIME);

    String postDeleteSegmentList = sendGetRequest(
        _controllerRequestURLBuilder.forSegmentListAPIWithTableType(TABLE_NAME,
            CommonConstants.Helix.TableType.REALTIME.toString()));
    JSONArray realtimeSegmentsListReturn =
        getSegmentsFromJsonSegmentAPI(postDeleteSegmentList, CommonConstants.Helix.TableType.REALTIME.toString());
    realtimeSegmentsList.remove(removedSegment);
    Assert.assertEquals(realtimeSegmentsListReturn, realtimeSegmentsList);
  }

  // @Test TODO: Add back when we use LLC only
  public void deleteAllRealtimeSegmentsFromGetAPI() throws Exception {
  }

  // @Test TODO: Add back when we use LLC only
  public void deleteAllRealtimeSegmentsFromDeleteAPI() throws Exception {
  }

  @Test
  public void deleteFromDeleteAPI() throws Exception {
    String segmentList = sendGetRequest(_controllerRequestURLBuilder.
        forSegmentListAPIWithTableType(TABLE_NAME, CommonConstants.Helix.TableType.OFFLINE.toString()));
    JSONArray offlineSegmentsList =
        getSegmentsFromJsonSegmentAPI(segmentList, CommonConstants.Helix.TableType.OFFLINE.toString());
    Assert.assertNotNull(offlineSegmentsList);

    String removedSegment = offlineSegmentsList.get(0).toString();
    long removedSegmentRows = getNumRowsFromOfflineMetadata(removedSegment);
    Assert.assertNotSame(removedSegmentRows, 0L);

    sendDeleteRequest(_controllerRequestURLBuilder.
        forSegmentDeleteAPI(TABLE_NAME, removedSegment, CommonConstants.Helix.TableType.OFFLINE.toString()));

    waitForNumRows(nOfflineRows - removedSegmentRows, CommonConstants.Helix.TableType.OFFLINE);

    String postDeleteSegmentList = sendGetRequest(_controllerRequestURLBuilder.
        forSegmentListAPIWithTableType(TABLE_NAME, CommonConstants.Helix.TableType.OFFLINE.toString()));
    JSONArray offlineSegmentsListReturn =
        getSegmentsFromJsonSegmentAPI(postDeleteSegmentList, CommonConstants.Helix.TableType.OFFLINE.toString());
    offlineSegmentsList.remove(removedSegment);
    Assert.assertEquals(offlineSegmentsListReturn, offlineSegmentsList);

    // Testing Delete All API here
    sendDeleteRequest(_controllerRequestURLBuilder.
        forSegmentDeleteAllAPI(TABLE_NAME, CommonConstants.Helix.TableType.OFFLINE.toString()));

    waitForNumRows(0, CommonConstants.Helix.TableType.OFFLINE);

    String postDeleteSegmentListAll = sendGetRequest(_controllerRequestURLBuilder.
        forSegmentListAPIWithTableType(TABLE_NAME, CommonConstants.Helix.TableType.OFFLINE.toString()));

    Assert.assertEquals(
        getSegmentsFromJsonSegmentAPI(postDeleteSegmentListAll, CommonConstants.Helix.TableType.OFFLINE.toString()),
        Collections.emptyList());

    waitForSegmentsToBeInDeleteDirectory();
    repushOfflineSegments();
  }

  @Override
  @Test
  public void testSegmentListApi() {
    // Tested in HybridClusterIntegrationTest
  }

  @Test
  public void deleteFromGetAPI() throws Exception {
    String segmentList = sendGetRequest(_controllerRequestURLBuilder.
        forSegmentListAPIWithTableType(TABLE_NAME, CommonConstants.Helix.TableType.OFFLINE.toString()));

    JSONArray offlineSegmentsList =
        getSegmentsFromJsonSegmentAPI(segmentList, CommonConstants.Helix.TableType.OFFLINE.toString());

    String removedSegment = offlineSegmentsList.get(0).toString();

    long removedSegmentRows = getNumRowsFromOfflineMetadata(removedSegment);
    Assert.assertNotSame(removedSegmentRows, 0L);

    sendGetRequest(_controllerRequestURLBuilder.
        forDeleteSegmentWithGetAPI(TABLE_NAME, removedSegment, CommonConstants.Helix.TableType.OFFLINE.toString()));

    waitForNumRows(nOfflineRows - removedSegmentRows, CommonConstants.Helix.TableType.OFFLINE);

    String postDeleteSegmentList = sendGetRequest(_controllerRequestURLBuilder.
        forSegmentListAPIWithTableType(TABLE_NAME, CommonConstants.Helix.TableType.OFFLINE.toString()));
    JSONArray offlineSegmentsListReturn =
        getSegmentsFromJsonSegmentAPI(postDeleteSegmentList, CommonConstants.Helix.TableType.OFFLINE.toString());
    offlineSegmentsList.remove(removedSegment);
    Assert.assertEquals(offlineSegmentsListReturn, offlineSegmentsList);

    // Testing Delete All API here
    sendGetRequest(_controllerRequestURLBuilder.
        forDeleteAllSegmentsWithTypeWithGetAPI(TABLE_NAME, CommonConstants.Helix.TableType.OFFLINE.toString()));

    waitForNumRows(0, CommonConstants.Helix.TableType.OFFLINE);

    String postDeleteSegmentListAll = sendGetRequest(_controllerRequestURLBuilder.
        forSegmentListAPIWithTableType(TABLE_NAME, CommonConstants.Helix.TableType.OFFLINE.toString()));

    Assert.assertEquals(
        getSegmentsFromJsonSegmentAPI(postDeleteSegmentListAll, CommonConstants.Helix.TableType.OFFLINE.toString()),
        Collections.emptyList());

    waitForSegmentsToBeInDeleteDirectory();
    repushOfflineSegments();
  }

  private long getNumRowsFromOfflineMetadata(String segmentName) throws Exception {
    OfflineSegmentZKMetadata segmentZKMetadata =
        ZKMetadataProvider.getOfflineSegmentZKMetadata(_propertyStore, TABLE_NAME, segmentName);
    return segmentZKMetadata.getTotalRawDocs();
  }

  private long getNumRowsFromRealtimeMetadata(String segmentName) {
    RealtimeSegmentZKMetadata segmentZKMetadata =
        ZKMetadataProvider.getRealtimeSegmentZKMetadata(_propertyStore, TABLE_NAME, segmentName);
    return segmentZKMetadata.getTotalRawDocs();
  }

  private com.alibaba.fastjson.JSONArray getSegmentsFromJsonSegmentAPI(String json, String type) throws Exception {
    JSONObject tableTypeAndSegments = (JSONObject) JSON.parseArray(json).get(0);
    return (JSONArray) tableTypeAndSegments.get(type);
  }

  private void repushOfflineSegments() throws Exception {
    uploadSegments(_tarDir);
    waitForNumRows(nOfflineRows, CommonConstants.Helix.TableType.OFFLINE);
  }
}
