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
package com.linkedin.pinot.server.api.resources;

import com.linkedin.pinot.common.restlet.resources.TableSizeInfo;
import com.linkedin.pinot.core.indexsegment.IndexSegment;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class TableSizeResourceTest {
  public static final Logger LOGGER = LoggerFactory.getLogger(TableSizeResourceTest.class);
  private static final String TABLE_NAME = ResourceTestHelper.DEFAULT_TABLE_NAME;
  private static final String TABLE_SIZE_PATH = "/tables/" + TABLE_NAME + "/size";

  ResourceTestHelper testHelper = new ResourceTestHelper();
  WebTarget target;
  @BeforeClass
  public void setupTest()
      throws Exception {
    testHelper.setup();
    target = testHelper.target;
  }

  @AfterTest
  public void tearDownTest()
      throws Exception {
    testHelper.tearDown();
  }

  @Test
  public void testTableSizeNotFound() {
    Response response = target.path("table/unknownTable/size").request().get(Response.class);
    Assert.assertEquals(response.getStatus(), Response.Status.NOT_FOUND.getStatusCode());
  }


  @Test
  public void testTableSizeDetailed() {
    TableSizeInfo tableSizeInfo = target.path(TABLE_SIZE_PATH).request().get(TableSizeInfo.class);
    IndexSegment indexSegment = testHelper.indexSegment;

    Assert.assertEquals(tableSizeInfo.tableName, TABLE_NAME);
    Assert.assertEquals(tableSizeInfo.diskSizeInBytes, indexSegment.getDiskSizeBytes());
    Assert.assertEquals(tableSizeInfo.segments.size(), 1);
    Assert.assertEquals(tableSizeInfo.segments.get(0).segmentName, indexSegment.getSegmentName());
    Assert.assertEquals(tableSizeInfo.segments.get(0).diskSizeInBytes,
        indexSegment.getDiskSizeBytes());
    Assert.assertEquals(tableSizeInfo.diskSizeInBytes, indexSegment.getDiskSizeBytes());
  }

  @Test
  public void testTableSizeNoDetails() {
    TableSizeInfo tableSizeInfo = target.path(TABLE_SIZE_PATH).queryParam("detailed", "false")
        .request().get(TableSizeInfo.class);
    Assert.assertEquals(tableSizeInfo.tableName, TABLE_NAME);
    Assert.assertEquals(tableSizeInfo.diskSizeInBytes, testHelper.indexSegment.getDiskSizeBytes());
    Assert.assertEquals(tableSizeInfo.segments.size(), 0);
  }

  @Test
  public void testTableSizeOld() {
    TableSizeInfo tableSizeInfo = target.path("/table/" + TABLE_NAME + "/size").request().get(TableSizeInfo.class);

    Assert.assertEquals(tableSizeInfo.tableName, TABLE_NAME);
    IndexSegment indexSegment = testHelper.indexSegment;
    Assert.assertEquals(tableSizeInfo.diskSizeInBytes, indexSegment.getDiskSizeBytes());
    Assert.assertEquals(tableSizeInfo.segments.size(), 1);
    Assert.assertEquals(tableSizeInfo.segments.get(0).segmentName, indexSegment.getSegmentName());
    Assert.assertEquals(tableSizeInfo.segments.get(0).diskSizeInBytes,
        indexSegment.getDiskSizeBytes());
    Assert.assertEquals(tableSizeInfo.diskSizeInBytes, indexSegment.getDiskSizeBytes());
  }
}
