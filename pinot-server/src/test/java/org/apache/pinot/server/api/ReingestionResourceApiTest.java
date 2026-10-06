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

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.Response;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.common.utils.LLCSegmentName;
import org.apache.pinot.core.data.manager.realtime.RealtimeTableDataManager;
import org.testng.annotations.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


/// Tests that the `ReingestionResource` is served by the admin API with its injected dependencies, and keeps track of
/// the running re-ingestion jobs across requests.
public class ReingestionResourceApiTest extends BaseResourceTest {
  private static final String REINGESTION_RAW_TABLE_NAME = "reingestionTable";
  private static final String REINGESTION_REALTIME_TABLE_NAME = REINGESTION_RAW_TABLE_NAME + "_REALTIME";

  @Test
  public void testResourceDependenciesAreInjected() {
    // Any request to the resource fails if one of its injected dependencies is not bound
    Response response = _webTarget.path("/reingestSegment/jobs").request().get(Response.class);
    assertThat(response.getStatus()).isEqualTo(Response.Status.OK.getStatusCode());
  }

  @Test
  public void testRunningReingestionIsTrackedAcrossRequests()
      throws Exception {
    String segmentName =
        new LLCSegmentName(REINGESTION_RAW_TABLE_NAME, 0, 0, System.currentTimeMillis()).getSegmentName();
    SegmentZKMetadata segmentZKMetadata = new SegmentZKMetadata(segmentName);
    segmentZKMetadata.setStartOffset("0");
    segmentZKMetadata.setEndOffset("100");

    // Hold the job on the re-ingestion thread, where the segment build semaphore is fetched
    CountDownLatch jobStarted = new CountDownLatch(1);
    CountDownLatch releaseJob = new CountDownLatch(1);
    RealtimeTableDataManager tableDataManager = mock(RealtimeTableDataManager.class);
    when(tableDataManager.fetchZKMetadata(segmentName)).thenReturn(segmentZKMetadata);
    when(tableDataManager.getSegmentBuildSemaphore()).thenAnswer(invocation -> {
      jobStarted.countDown();
      releaseJob.await();
      return null;
    });
    _tableDataManagerMap.put(REINGESTION_REALTIME_TABLE_NAME, tableDataManager);

    try {
      String path = "/reingestSegment/" + segmentName;
      assertThat(postStatus(path)).isEqualTo(Response.Status.OK.getStatusCode());
      assertThat(jobStarted.await(10, TimeUnit.SECONDS)).isTrue();

      // Only visible to the next requests when they are served by the same resource instance
      assertThat(postStatus(path)).isEqualTo(Response.Status.CONFLICT.getStatusCode());
      assertThat(_webTarget.path("/reingestSegment/jobs").request().get(String.class)).contains(segmentName);
    } finally {
      releaseJob.countDown();
      _tableDataManagerMap.remove(REINGESTION_REALTIME_TABLE_NAME);
    }
  }

  private int postStatus(String path) {
    try (Response response = _webTarget.path(path).request().post(Entity.json(""))) {
      return response.getStatus();
    }
  }
}
