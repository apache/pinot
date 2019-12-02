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
package org.apache.pinot.server.api;

import javax.ws.rs.core.Response;
import org.apache.pinot.common.restlet.resources.TableSizeInfo;
import org.apache.pinot.core.indexsegment.immutable.ImmutableSegment;
import org.testng.Assert;
import org.testng.annotations.Test;


public class TableSizeResourceTest extends BaseResourceTest {
  private static final String TABLE_SIZE_PATH = "/tables/" + TABLE_NAME + "/size";

  @Test
  public void testTableSizeNotFound() {
    Response response = _webTarget.path("table/unknownTable/size").request().get(Response.class);
    Assert.assertEquals(response.getStatus(), Response.Status.NOT_FOUND.getStatusCode());
  }

  @Test
  public void testTableSizeDetailed() {
    TableSizeInfo tableSizeInfo = _webTarget.path(TABLE_SIZE_PATH).request().get(TableSizeInfo.class);
    ImmutableSegment defaultSegment = _indexSegments.get(0);

    Assert.assertEquals(tableSizeInfo.tableName, TABLE_NAME);
    Assert.assertEquals(tableSizeInfo.diskSizeInBytes, defaultSegment.getSegmentSizeBytes());
    Assert.assertEquals(tableSizeInfo.segments.size(), 1);
    Assert.assertEquals(tableSizeInfo.segments.get(0).segmentName, defaultSegment.getSegmentName());
    Assert.assertEquals(tableSizeInfo.segments.get(0).diskSizeInBytes, defaultSegment.getSegmentSizeBytes());
    Assert.assertEquals(tableSizeInfo.diskSizeInBytes, defaultSegment.getSegmentSizeBytes());
  }

  @Test
  public void testTableSizeNoDetails() {
    TableSizeInfo tableSizeInfo =
        _webTarget.path(TABLE_SIZE_PATH).queryParam("detailed", "false").request().get(TableSizeInfo.class);
    ImmutableSegment defaultSegment = _indexSegments.get(0);

    Assert.assertEquals(tableSizeInfo.tableName, TABLE_NAME);
    Assert.assertEquals(tableSizeInfo.diskSizeInBytes, defaultSegment.getSegmentSizeBytes());
    Assert.assertEquals(tableSizeInfo.segments.size(), 0);
  }

  @Test
  public void testTableSizeOld() {
    TableSizeInfo tableSizeInfo = _webTarget.path("/table/" + TABLE_NAME + "/size").request().get(TableSizeInfo.class);
    ImmutableSegment defaultSegment = _indexSegments.get(0);

    Assert.assertEquals(tableSizeInfo.tableName, TABLE_NAME);
    Assert.assertEquals(tableSizeInfo.diskSizeInBytes, defaultSegment.getSegmentSizeBytes());
    Assert.assertEquals(tableSizeInfo.segments.size(), 1);
    Assert.assertEquals(tableSizeInfo.segments.get(0).segmentName, defaultSegment.getSegmentName());
    Assert.assertEquals(tableSizeInfo.segments.get(0).diskSizeInBytes, defaultSegment.getSegmentSizeBytes());
    Assert.assertEquals(tableSizeInfo.diskSizeInBytes, defaultSegment.getSegmentSizeBytes());
  }
}
