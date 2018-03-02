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

import com.google.common.base.Function;
import com.linkedin.pinot.common.restlet.resources.ServerSegmentInfo;
import com.linkedin.pinot.common.utils.CommonConstants;
import com.linkedin.pinot.common.utils.FileUploadDownloadClient;
import com.linkedin.pinot.controller.util.ServerPerfMetricsReader;
import com.linkedin.pinot.util.TestUtils;
import java.io.File;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.httpclient.HttpConnectionManager;
import org.apache.commons.httpclient.MultiThreadedHttpConnectionManager;
import org.apache.http.HttpStatus;
import org.testng.Assert;
import org.testng.annotations.Test;


/*
This class extends offline cluster integration test while it uses BalancedSegmentSizeSegmentAssignmentStrategy
for segment assignment strategy. It then verifies that the segments are distributed properly.
 */
public class BalancedSegmentSizeSegmentAssignmentStrategyIntegrationTest extends OfflineClusterIntegrationTest {
  private static final int NUM_BROKERS = 1;
  private static final int NUM_SERVERS = 6;

  @Override
  protected int getNumBrokers() {
    return NUM_BROKERS;
  }

  @Override
  protected int getNumServers() {
    return NUM_SERVERS;
  }

  @Override
  protected String getSegmentAssignmentStrategy() {

    return "BalancedSegmentSizeSegmentAssignmentStrategy";
   // return "BalancedLatencyBasedSegmentAssignmentStrategy";
  }

  @Override
  protected int getNumReplicas() {
    return 1;
  }

  /*
  This function is a helper function that waits until count(*) query to a default table returns result which is more
  than a previous result. We can use this function to make sure that a new segment uploaded completely.
   */
  protected void waitForAllDocsLoaded(long prevCount, long timeoutMs) throws Exception {
    final long countStarResult = prevCount;
    TestUtils.waitForCondition(new Function<Void, Boolean>() {
      @Nullable
      @Override
      public Boolean apply(@Nullable Void aVoid) {
        try {
          return getCurrentCountStarResult() > countStarResult;
        } catch (Exception e) {
          return null;
        }
      }
    }, timeoutMs, "Failed to load " + countStarResult + " documents");
  }

  /*
  We override uploadSegments to make sure that a current segment upload is completed before starting a new segment upload.
  This is a necessary to verify that BalancedSegmentSizeSegmentAssignmentStrategy logic is correct.
   */
  @Override
  protected void uploadSegments(@Nonnull File segmentDir) {
    String[] segmentNames = segmentDir.list();
    Assert.assertNotNull(segmentNames);
    int count = 0;
    for (final String segmentName : segmentNames) {
      count++;
      long prevCount;
      try {
        if (count > 1) {
          prevCount = getCurrentCountStarResult();
        } else {
          prevCount = 0;
        }
        final File segmentFile = new File(segmentDir, segmentName);
        final URI uploadSegmentHttpURI = FileUploadDownloadClient.getUploadSegmentHttpURI(LOCAL_HOST, _controllerPort);
        final FileUploadDownloadClient fileUploadDownloadClient = new FileUploadDownloadClient();

        // Upload all segments in parallel
        int numSegments = segmentNames.length;
        ExecutorService executor = Executors.newFixedThreadPool(1);
        List<Future<Integer>> tasks = new ArrayList<>(numSegments);
        tasks.add(executor.submit(new Callable<Integer>() {
            @Override
            public Integer call() throws Exception {
              return fileUploadDownloadClient.uploadSegment(uploadSegmentHttpURI, segmentName, segmentFile);
            }
          }));
          // Wait for new segment loaded
        waitForAllDocsLoaded(prevCount, 600_000L);
        executor.shutdown();
      } catch (Exception e) {
        e.printStackTrace();
        continue;
      }
    }
  }



  private long getCurrentTotalSegmentsAssigned() {
    long currentTotalSegmentAssigned = 0;
    HttpConnectionManager connectionManager = new MultiThreadedHttpConnectionManager();
    Executor executor = Executors.newFixedThreadPool(1);
    ServerPerfMetricsReader serverPerfMetricsReader = new ServerPerfMetricsReader(executor, connectionManager, null);
    for (int i = 0; i < NUM_SERVERS; i++) {
      String serverEndpoint = "localhost:" + (CommonConstants.Server.DEFAULT_ADMIN_API_PORT - i);
      ServerSegmentInfo serverSegmentInfo = serverPerfMetricsReader.getServerPerfMetrics(serverEndpoint, false, 5000);
      currentTotalSegmentAssigned = serverSegmentInfo.getSegmentCount();
    }
    return currentTotalSegmentAssigned;
  }

  @Test
  public void testSegmentAssignment() throws ConfigurationException {
    HttpConnectionManager connectionManager = new MultiThreadedHttpConnectionManager();
    Executor executor = Executors.newFixedThreadPool(1);
    Map<String, Long> possibleAssignmentSegmentsSize = whatCouldBeSegmentsSizeForCorrectStrategyImplementation();
    ServerPerfMetricsReader serverPerfMetricsReader = new ServerPerfMetricsReader(executor, connectionManager, null);
    for (int i = 0; i < NUM_SERVERS; i++) {
      String serverEndpoint = "localhost:" + (CommonConstants.Server.DEFAULT_ADMIN_API_PORT - i);
      ServerSegmentInfo serverSegmentInfo = serverPerfMetricsReader.getServerPerfMetrics(serverEndpoint, false, 5000);
      //Check that every server gets 2 segments
      Assert.assertEquals(serverSegmentInfo.getSegmentCount(), 2);
      /*Ideally the reportedSegmentsSize should be one of possible values, but it seems there is difference between
      segment size before and after upload. TODO is to find a accurate estimation of segment size after upload.
      */
      //Assert.assertEquals(possibleAssignmentSegmentsSize.containsValue(serverSegmentInfo.getSegmentSizeInBytes()),true);
    }
  }

  /*Since replication factor is 2 and there are 12 segments with close sizes, every server should get 2 segments
  This functions returns sum of size of every pair of segments
  */
  private Map<String, Long> whatCouldBeSegmentsSizeForCorrectStrategyImplementation() {
    Map<String, Long> possibleAssignmentSegmentsSize = new HashMap<>();
    List<Long> uploadedSegmentsSize = getUploadedSegmentsSize(_tarDir);
    for (int i = 0; i < uploadedSegmentsSize.size(); i++) {
      for (int j = i + 1; j < uploadedSegmentsSize.size(); j++) {
        possibleAssignmentSegmentsSize.put("Segment" + i + "_Segment" + j,
            uploadedSegmentsSize.get(i) + uploadedSegmentsSize.get(j));
      }
    }
    return possibleAssignmentSegmentsSize;
  }

  private List<Long> getUploadedSegmentsSize(@Nonnull File segmentDir) {
    List<Long> uploadedSegmentsSize = new ArrayList<>();
    String[] segmentNames = segmentDir.list();
    Assert.assertNotNull(segmentNames);
    for (String segmentName : segmentNames) {
      File segmentFile = new File(segmentDir, segmentName);
      uploadedSegmentsSize.add(segmentFile.length());
    }
    return uploadedSegmentsSize;
  }
}
