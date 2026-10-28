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
package org.apache.pinot.server.api.resources;

import java.util.List;
import org.apache.pinot.common.response.server.ServerReloadStatusResponse;
import org.apache.pinot.core.data.manager.InstanceDataManager;
import org.apache.pinot.segment.local.data.manager.SegmentDataManager;
import org.apache.pinot.segment.local.data.manager.TableDataManager;
import org.apache.pinot.segment.local.utils.ServerReloadJobStatusCache;
import org.apache.pinot.server.starter.ServerInstance;
import org.apache.pinot.spi.utils.JsonUtils;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;


public class ControllerJobStatusResourceTest {
  private static final String TABLE_NAME = "testTable_OFFLINE";
  private static final long JOB_TIMESTAMP = 1000L;

  private TableDataManager _tableDataManager;
  private ServerReloadJobStatusCache _jobStatusCache;
  private ControllerJobStatusResource _resource;

  @BeforeMethod
  public void setUp() {
    _tableDataManager = mock(TableDataManager.class);
    InstanceDataManager instanceDataManager = mock(InstanceDataManager.class);
    when(instanceDataManager.getTableDataManager(TABLE_NAME)).thenReturn(_tableDataManager);
    ServerInstance serverInstance = mock(ServerInstance.class);
    when(serverInstance.getInstanceDataManager()).thenReturn(instanceDataManager);
    _jobStatusCache = new ServerReloadJobStatusCache("testServer");
    _resource = new ControllerJobStatusResource(serverInstance, _jobStatusCache);
  }

  @Test
  public void testSkippedSegmentsCountedAsSuccessOnce()
      throws Exception {
    // reloaded: loaded after the job; skipped: left in place; skippedThenReloaded: skipped but reloaded again later
    // (counted once); untouched: loaded before the job and not skipped (still pending)
    List<SegmentDataManager> segments = List.of(segment("reloaded", JOB_TIMESTAMP + 1), segment("skipped", 1L),
        segment("skippedThenReloaded", JOB_TIMESTAMP), segment("untouched", 1L));
    when(_tableDataManager.acquireAllSegments()).thenReturn(segments);
    // A skipped segment that has since been dropped from the server is not counted
    _jobStatusCache.recordSkipped("job", List.of("skipped", "skippedThenReloaded", "dropped"));

    ServerReloadStatusResponse response = getStatus("job");
    assertEquals(response.getTotalSegmentCount(), 4);
    assertEquals(response.getSuccessCount(), 3);
    assertEquals(response.getFailureCount(), Long.valueOf(0));
  }

  @Test
  public void testNoSkippedSegmentsCountsOnlyReloaded()
      throws Exception {
    List<SegmentDataManager> segments = List.of(segment("reloaded", JOB_TIMESTAMP + 1), segment("untouched", 1L));
    when(_tableDataManager.acquireAllSegments()).thenReturn(segments);
    _jobStatusCache.recordFailure("job", "untouched", new Exception("failed"));

    ServerReloadStatusResponse response = getStatus("job");
    assertEquals(response.getSuccessCount(), 1);
    assertEquals(response.getFailureCount(), Long.valueOf(1));
  }

  private ServerReloadStatusResponse getStatus(String jobId)
      throws Exception {
    return JsonUtils.stringToObject(_resource.reloadJobStatus(TABLE_NAME, JOB_TIMESTAMP, null, jobId, null),
        ServerReloadStatusResponse.class);
  }

  private static SegmentDataManager segment(String segmentName, long loadTimeMs) {
    SegmentDataManager segmentDataManager = mock(SegmentDataManager.class);
    when(segmentDataManager.getSegmentName()).thenReturn(segmentName);
    when(segmentDataManager.getLoadTimeMs()).thenReturn(loadTimeMs);
    return segmentDataManager;
  }
}
