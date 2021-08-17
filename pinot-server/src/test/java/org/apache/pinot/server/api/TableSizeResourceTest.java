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
import org.apache.pinot.segment.spi.ImmutableSegment;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.testng.Assert;
import org.testng.annotations.Test;


public class TableSizeResourceTest extends BaseResourceTest {
  @Test
  public void testTableSizeNotFound() {
    Response response = _webTarget.path("table/unknownTable/size").request().get(Response.class);
    Assert.assertEquals(response.getStatus(), Response.Status.NOT_FOUND.getStatusCode());
  }

  @Test
  public void testTableSizeDetailed() {
    verifyTableSizeDetailedImpl(TableNameBuilder.REALTIME.tableNameWithType(TABLE_NAME), _realtimeIndexSegments.get(0));
    verifyTableSizeDetailedImpl(TableNameBuilder.OFFLINE.tableNameWithType(TABLE_NAME), _offlineIndexSegments.get(0));
  }

  private void verifyTableSizeDetailedImpl(String expectedTableName, ImmutableSegment segment) {
    String path = "/tables/" + expectedTableName + "/size";
    TableSizeInfo tableSizeInfo = _webTarget.path(path).request().get(TableSizeInfo.class);

    Assert.assertEquals(tableSizeInfo._tableName, expectedTableName);
    Assert.assertEquals(tableSizeInfo._diskSizeInBytes, segment.getSegmentSizeBytes());
    Assert.assertEquals(tableSizeInfo._segments.size(), 1);
    Assert.assertEquals(tableSizeInfo._segments.get(0)._segmentName, segment.getSegmentName());
    Assert.assertEquals(tableSizeInfo._segments.get(0)._diskSizeInBytes, segment.getSegmentSizeBytes());
    Assert.assertEquals(tableSizeInfo._diskSizeInBytes, segment.getSegmentSizeBytes());
  }

  @Test
  public void testTableSizeNoDetails() {
    verifyTableSizeNoDetailsImpl(TableNameBuilder.REALTIME.tableNameWithType(TABLE_NAME),
        _realtimeIndexSegments.get(0));
    verifyTableSizeNoDetailsImpl(TableNameBuilder.OFFLINE.tableNameWithType(TABLE_NAME), _offlineIndexSegments.get(0));
  }

  private void verifyTableSizeNoDetailsImpl(String expectedTableName, ImmutableSegment segment) {
    String path = "/tables/" + expectedTableName + "/size";
    TableSizeInfo tableSizeInfo =
        _webTarget.path(path).queryParam("detailed", "false").request().get(TableSizeInfo.class);

    Assert.assertEquals(tableSizeInfo._tableName, expectedTableName);
    Assert.assertEquals(tableSizeInfo._diskSizeInBytes, segment.getSegmentSizeBytes());
    Assert.assertEquals(tableSizeInfo._segments.size(), 0);
  }

  @Test
  public void testTableSizeOld() {
    verifyTableSizeOldImpl(TableNameBuilder.REALTIME.tableNameWithType(TABLE_NAME), _realtimeIndexSegments.get(0));
    verifyTableSizeOldImpl(TableNameBuilder.OFFLINE.tableNameWithType(TABLE_NAME), _offlineIndexSegments.get(0));
  }

  private void verifyTableSizeOldImpl(String expectedTableName, ImmutableSegment segment) {
    String path = "/table/" + expectedTableName + "/size";
    TableSizeInfo tableSizeInfo = _webTarget.path(path).request().get(TableSizeInfo.class);

    Assert.assertEquals(tableSizeInfo._tableName, expectedTableName);
    Assert.assertEquals(tableSizeInfo._diskSizeInBytes, segment.getSegmentSizeBytes());
    Assert.assertEquals(tableSizeInfo._segments.size(), 1);
    Assert.assertEquals(tableSizeInfo._segments.get(0)._segmentName, segment.getSegmentName());
    Assert.assertEquals(tableSizeInfo._segments.get(0)._diskSizeInBytes, segment.getSegmentSizeBytes());
    Assert.assertEquals(tableSizeInfo._diskSizeInBytes, segment.getSegmentSizeBytes());
  }
}
