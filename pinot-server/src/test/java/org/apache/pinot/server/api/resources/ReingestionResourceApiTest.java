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

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.Response;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.common.utils.LLCSegmentName;
import org.apache.pinot.core.data.manager.realtime.RealtimeTableDataManager;
import org.apache.pinot.server.api.BaseResourceTest;
import org.mockito.stubbing.Answer;
import org.testng.annotations.Test;

import static org.apache.pinot.server.api.resources.ReingestionResource.MAX_PARALLEL_REINGESTIONS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


/// Tests that the [ReingestionResource] is served by the admin API with its injected dependencies, keeps track of the
/// re-ingestion jobs across requests, and stops them when the admin API stops.
public class ReingestionResourceApiTest extends BaseResourceTest {
  private static final String REINGESTION_RAW_TABLE_NAME = "reingestionTable";
  private static final String REINGESTION_REALTIME_TABLE_NAME = REINGESTION_RAW_TABLE_NAME + "_REALTIME";
  // Makes the segment names unique across tests, since a segment is held until its previous job finishes
  private static final AtomicInteger SEQUENCE_NUMBER = new AtomicInteger();

  @Test
  public void testResourceDependenciesAreInjected() {
    // Any request to the resource fails if one of its injected dependencies is not bound
    Response response = _webTarget.path("/reingestSegment/jobs").request().get(Response.class);
    assertThat(response.getStatus()).isEqualTo(Response.Status.OK.getStatusCode());
  }

  @Test
  public void testRunningReingestionIsTrackedAcrossRequests()
      throws Exception {
    // Hold the job on the re-ingestion thread, where the segment build semaphore is fetched
    CountDownLatch jobStarted = new CountDownLatch(1);
    CountDownLatch releaseJob = new CountDownLatch(1);
    addTableDataManager(invocation -> {
      jobStarted.countDown();
      releaseJob.await();
      return null;
    });

    try {
      String segmentName = segmentName(0);
      assertThat(postReingestion(segmentName)).isEqualTo(Response.Status.OK.getStatusCode());
      assertThat(jobStarted.await(10, TimeUnit.SECONDS)).isTrue();

      // Only visible to the next requests when they are served by the same resource instance
      assertThat(postReingestion(segmentName)).isEqualTo(Response.Status.CONFLICT.getStatusCode());
      assertThat(getJobs()).contains(segmentName);
    } finally {
      releaseJob.countDown();
      _tableDataManagerMap.remove(REINGESTION_REALTIME_TABLE_NAME);
    }
  }

  @Test
  public void testQueuedReingestionIsListed() {
    // Hold every re-ingestion thread so that the last job waits in the queue
    CountDownLatch releaseJobs = new CountDownLatch(1);
    addTableDataManager(invocation -> {
      releaseJobs.await();
      return null;
    });

    try {
      String queuedSegmentName = null;
      for (int partitionId = 0; partitionId <= MAX_PARALLEL_REINGESTIONS; partitionId++) {
        queuedSegmentName = segmentName(partitionId);
        assertThat(postReingestion(queuedSegmentName)).isEqualTo(Response.Status.OK.getStatusCode());
      }
      assertThat(getJobs()).contains(queuedSegmentName);
    } finally {
      releaseJobs.countDown();
      _tableDataManagerMap.remove(REINGESTION_REALTIME_TABLE_NAME);
    }
  }

  // Runs after the other tests since it stops the admin API
  @Test(priority = 1)
  public void testRunningReingestionIsStoppedOnShutdown()
      throws Exception {
    // Hold the job on the re-ingestion thread until it is interrupted
    AtomicReference<Thread> reingestionThread = new AtomicReference<>();
    CountDownLatch jobStarted = new CountDownLatch(1);
    addTableDataManager(invocation -> {
      reingestionThread.set(Thread.currentThread());
      jobStarted.countDown();
      new CountDownLatch(1).await();
      return null;
    });
    assertThat(postReingestion(segmentName(0))).isEqualTo(Response.Status.OK.getStatusCode());
    assertThat(jobStarted.await(10, TimeUnit.SECONDS)).isTrue();

    _adminApiApplication.stop();

    reingestionThread.get().join(10_000);
    assertThat(reingestionThread.get().isAlive()).isFalse();
  }

  /// Adds a table data manager whose segments are pending re-ingestion, and which answers the segment build semaphore
  /// fetched on the re-ingestion thread with the given answer.
  private void addTableDataManager(Answer<Semaphore> segmentBuildSemaphoreAnswer) {
    RealtimeTableDataManager tableDataManager = mock(RealtimeTableDataManager.class);
    when(tableDataManager.fetchZKMetadata(anyString())).thenAnswer(invocation -> {
      SegmentZKMetadata segmentZKMetadata = new SegmentZKMetadata((String) invocation.getArgument(0));
      segmentZKMetadata.setStartOffset("0");
      segmentZKMetadata.setEndOffset("100");
      return segmentZKMetadata;
    });
    when(tableDataManager.getSegmentBuildSemaphore()).thenAnswer(segmentBuildSemaphoreAnswer);
    _tableDataManagerMap.put(REINGESTION_REALTIME_TABLE_NAME, tableDataManager);
  }

  private static String segmentName(int partitionId) {
    return new LLCSegmentName(REINGESTION_RAW_TABLE_NAME, partitionId, SEQUENCE_NUMBER.getAndIncrement(),
        System.currentTimeMillis()).getSegmentName();
  }

  private int postReingestion(String segmentName) {
    try (Response response = _webTarget.path("/reingestSegment/" + segmentName).request().post(Entity.json(""))) {
      return response.getStatus();
    }
  }

  private String getJobs() {
    return _webTarget.path("/reingestSegment/jobs").request().get(String.class);
  }
}
