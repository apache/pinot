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

import com.fasterxml.jackson.databind.JsonNode;
import java.util.ArrayList;
import java.util.List;
import javax.ws.rs.core.Response;
import org.apache.pinot.common.restlet.resources.SegmentSizeInfo;
import org.apache.pinot.common.restlet.resources.TableSizeInfo;
import org.apache.pinot.segment.local.data.manager.TableDataManager;
import org.apache.pinot.segment.spi.ImmutableSegment;
import org.apache.pinot.segment.spi.SegmentMetadata;
import org.apache.pinot.spi.utils.JsonUtils;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;


public class TableSizeResourceTest extends BaseResourceTest {
  @Test
  public void testTableSizeNotFound() {
    Response response = _webTarget.path("table/unknownTable/size").request().get(Response.class);
    Assert.assertEquals(response.getStatus(), Response.Status.NOT_FOUND.getStatusCode());
  }

  @Test
  public void testTableSizeDetailed() {
    verifyTableSizeDetailedImpl(REALTIME_TABLE_NAME, _realtimeIndexSegments.get(0));
    verifyTableSizeDetailedImpl(OFFLINE_TABLE_NAME, _offlineIndexSegments.get(0));
  }

  private void verifyTableSizeDetailedImpl(String expectedTableName, ImmutableSegment segment) {
    String path = "/tables/" + expectedTableName + "/size";
    TableSizeInfo tableSizeInfo = _webTarget.path(path).request().get(TableSizeInfo.class);

    Assert.assertEquals(tableSizeInfo.getTableName(), expectedTableName);
    Assert.assertEquals(tableSizeInfo.getDiskSizeInBytes(), segment.getSegmentSizeBytes());
    Assert.assertEquals(tableSizeInfo.getSegments().size(), 1);
    Assert.assertEquals(tableSizeInfo.getSegments().get(0).getSegmentName(), segment.getSegmentName());
    Assert.assertEquals(tableSizeInfo.getSegments().get(0).getDiskSizeInBytes(), segment.getSegmentSizeBytes());
    Assert.assertEquals(tableSizeInfo.getDiskSizeInBytes(), segment.getSegmentSizeBytes());
  }

  @Test
  public void testTableSizeNoDetails() {
    verifyTableSizeNoDetailsImpl(REALTIME_TABLE_NAME, _realtimeIndexSegments.get(0));
    verifyTableSizeNoDetailsImpl(OFFLINE_TABLE_NAME, _offlineIndexSegments.get(0));
  }

  private void verifyTableSizeNoDetailsImpl(String expectedTableName, ImmutableSegment segment) {
    String path = "/tables/" + expectedTableName + "/size";
    TableSizeInfo tableSizeInfo =
        _webTarget.path(path).queryParam("detailed", "false")
            .queryParam("includeCompressionStats", "true")
            .queryParam("includeColumnCompressionStats", "true")
            .request().get(TableSizeInfo.class);

    Assert.assertEquals(tableSizeInfo.getTableName(), expectedTableName);
    Assert.assertEquals(tableSizeInfo.getDiskSizeInBytes(), segment.getSegmentSizeBytes());
    Assert.assertEquals(tableSizeInfo.getSegments().size(), 0);
  }

  @Test
  public void testTableSizeNoDetailsSkipsCompressionMetadata()
      throws Exception {
    String tableName = "noDetails_OFFLINE";
    addTable(tableName);
    TableDataManager tableDataManager = _tableDataManagerMap.get(tableName);
    tableDataManager.getCachedTableConfigAndSchema().getLeft().getIndexingConfig()
        .setCompressionStatsEnabled(true);
    ImmutableSegment segment = mock(ImmutableSegment.class);
    SegmentMetadata segmentMetadata = mock(SegmentMetadata.class);
    when(segment.getSegmentName()).thenReturn("noDetailsSegment");
    when(segment.getSegmentSizeBytes()).thenReturn(123L);
    when(segment.getSegmentMetadata()).thenReturn(segmentMetadata);
    when(segmentMetadata.getTotalDocs()).thenReturn(1);
    tableDataManager.addSegment(segment);
    clearInvocations(segment, segmentMetadata);

    try {
      TableSizeInfo tableSizeInfo = _webTarget.path("/tables/" + tableName + "/size")
          .queryParam("detailed", "false")
          .queryParam("includeCompressionStats", "true")
          .queryParam("includeColumnCompressionStats", "true")
          .request().get(TableSizeInfo.class);

      Assert.assertEquals(tableSizeInfo.getDiskSizeInBytes(), 123L);
      Assert.assertTrue(tableSizeInfo.getSegments().isEmpty());
      verify(segment, never()).getSegmentMetadata();
    } finally {
      tableDataManager.offloadSegment(segment.getSegmentName());
    }
  }

  @Test
  public void testTableSizeOld() {
    verifyTableSizeOldImpl(REALTIME_TABLE_NAME, _realtimeIndexSegments.get(0));
    verifyTableSizeOldImpl(OFFLINE_TABLE_NAME, _offlineIndexSegments.get(0));
  }

  private void verifyTableSizeOldImpl(String expectedTableName, ImmutableSegment segment) {
    String path = "/table/" + expectedTableName + "/size";
    TableSizeInfo tableSizeInfo = _webTarget.path(path).request().get(TableSizeInfo.class);

    Assert.assertEquals(tableSizeInfo.getTableName(), expectedTableName);
    Assert.assertEquals(tableSizeInfo.getDiskSizeInBytes(), segment.getSegmentSizeBytes());
    Assert.assertEquals(tableSizeInfo.getSegments().size(), 1);
    Assert.assertEquals(tableSizeInfo.getSegments().get(0).getSegmentName(), segment.getSegmentName());
    Assert.assertEquals(tableSizeInfo.getSegments().get(0).getDiskSizeInBytes(), segment.getSegmentSizeBytes());
    Assert.assertEquals(tableSizeInfo.getDiskSizeInBytes(), segment.getSegmentSizeBytes());
  }

  @Test
  public void testTableSizeDetailedCompressionStatsDisabled()
      throws Exception {
    String tableName = "compressionStatsDisabled_OFFLINE";
    List<ImmutableSegment> segments = new ArrayList<>();
    addTable(tableName);
    TableDataManager tableDataManager = _tableDataManagerMap.get(tableName);
    ImmutableSegment trackedSegment = setUpSegment(tableName, null, "tracked", segments, true);
    Assert.assertTrue(trackedSegment.getSegmentMetadata().getColumnMetadataMap().values().stream()
        .anyMatch(column -> column.getRawForwardIndexUncompressedValueSizeInBytes() >= 0
            || column.getDictionaryEncodedUncompressedValueSizeInBytes() >= 0));

    try {
      String response = _webTarget.path("/tables/" + tableName + "/size")
          .queryParam("includeCompressionStats", "true")
          .queryParam("includeColumnCompressionStats", "true")
          .request()
          .get(String.class);
      TableSizeInfo tableSizeInfo = JsonUtils.stringToObject(response, TableSizeInfo.class);

      Assert.assertNotNull(tableSizeInfo);
      Assert.assertTrue(tableSizeInfo.getDiskSizeInBytes() > 0);
      Assert.assertEquals(tableSizeInfo.getSegments().size(), 1);
      SegmentSizeInfo segmentSizeInfo = tableSizeInfo.getSegments().get(0);
      Assert.assertEquals(segmentSizeInfo.getSegmentName(), trackedSegment.getSegmentName());
      Assert.assertEquals(segmentSizeInfo.getCompressionStatsUncompressedValueSizeInBytes(), -1L);
      Assert.assertEquals(segmentSizeInfo.getCompressionStatsForwardIndexAndDictionaryStorageSizeInBytes(), -1L);
      Assert.assertNull(segmentSizeInfo.getColumnCompressionStats());

      JsonNode segment = JsonUtils.stringToJsonNode(response).get("segments").get(0);
      Assert.assertFalse(segment.has("compressionStatsUncompressedValueSizeInBytes"));
      Assert.assertFalse(segment.has("compressionStatsForwardIndexAndDictionaryStorageSizeInBytes"));
      Assert.assertFalse(segment.has("columnCompressionStats"));
    } finally {
      tableDataManager.offloadSegment(trackedSegment.getSegmentName());
      tableDataManager.shutDown();
      _tableDataManagerMap.remove(tableName);
    }
  }
}
