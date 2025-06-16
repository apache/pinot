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
package org.apache.pinot.controller.helix.core.util;

import java.util.Map;
import java.util.Set;
import org.apache.pinot.controller.ControllerConf;
import org.apache.pinot.controller.helix.core.controllerjob.ControllerJobTypes;
import org.apache.pinot.controller.helix.core.rebalance.RebalanceJobConstants;
import org.apache.pinot.controller.helix.core.rebalance.RebalanceResult;
import org.apache.pinot.controller.helix.core.rebalance.TableRebalanceProgressStats;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.JsonUtils;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;


public class ControllerZkHelixUtilsTest {

  @Test
  public void testControllerJobZkMetadataExpiryWithInProgressJobs()
      throws Exception {
    // Setup job limits
    ControllerConf controllerConf = mock(ControllerConf.class);
    when(controllerConf.getMaxTableRebalanceZkJobs()).thenReturn(2);
    ControllerJobTypes.init(controllerConf);

    TableRebalanceProgressStats inProgressStats = new TableRebalanceProgressStats();
    inProgressStats.setStatus(RebalanceResult.Status.IN_PROGRESS);
    TableRebalanceProgressStats completedStats = new TableRebalanceProgressStats();
    completedStats.setStatus(RebalanceResult.Status.DONE);
    TableRebalanceProgressStats abortedStats = new TableRebalanceProgressStats();
    abortedStats.setStatus(RebalanceResult.Status.ABORTED);

    Map<String, Map<String, String>> jobMetadataMap = Map.of(
        "job1", Map.of(CommonConstants.ControllerJob.SUBMISSION_TIME_MS, "1000",
            CommonConstants.ControllerJob.JOB_TYPE, ControllerJobTypes.TABLE_REBALANCE.name(),
            RebalanceJobConstants.JOB_METADATA_KEY_REBALANCE_PROGRESS_STATS, JsonUtils.objectToString(inProgressStats)),
        "job2", Map.of(CommonConstants.ControllerJob.SUBMISSION_TIME_MS, "3000",
            CommonConstants.ControllerJob.JOB_TYPE, ControllerJobTypes.TABLE_REBALANCE.name(),
            RebalanceJobConstants.JOB_METADATA_KEY_REBALANCE_PROGRESS_STATS, JsonUtils.objectToString(completedStats)),
        "job3", Map.of(CommonConstants.ControllerJob.SUBMISSION_TIME_MS, "2000",
            CommonConstants.ControllerJob.JOB_TYPE, ControllerJobTypes.TABLE_REBALANCE.name(),
            RebalanceJobConstants.JOB_METADATA_KEY_REBALANCE_PROGRESS_STATS, JsonUtils.objectToString(abortedStats)),
        "job4", Map.of(CommonConstants.ControllerJob.SUBMISSION_TIME_MS, "4000",
            CommonConstants.ControllerJob.JOB_TYPE, ControllerJobTypes.TABLE_REBALANCE.name(),
            RebalanceJobConstants.JOB_METADATA_KEY_REBALANCE_PROGRESS_STATS, JsonUtils.objectToString(inProgressStats)),
        "job5", Map.of(CommonConstants.ControllerJob.SUBMISSION_TIME_MS, "5000",
            CommonConstants.ControllerJob.JOB_TYPE, ControllerJobTypes.TABLE_REBALANCE.name(),
            RebalanceJobConstants.JOB_METADATA_KEY_REBALANCE_PROGRESS_STATS, JsonUtils.objectToString(inProgressStats))
    );

    Map<String, Map<String, String>> updatedJobMetadataMap =
        ControllerZkHelixUtils.expireControllerJobsInZk(jobMetadataMap, ControllerJobTypes.TABLE_REBALANCE);
    // Even though the limit is 2, we should not delete the in-progress jobs
    assertEquals(updatedJobMetadataMap.size(), 3);
    assertEquals(updatedJobMetadataMap.keySet(), Set.of("job1", "job4", "job5"));
  }

  @Test
  public void testControllerJobZkMetadataExpirySubmissionTime()
      throws Exception {
    // Setup job limits
    ControllerConf controllerConf = mock(ControllerConf.class);
    when(controllerConf.getMaxTableRebalanceZkJobs()).thenReturn(2);
    ControllerJobTypes.init(controllerConf);

    TableRebalanceProgressStats completedStats = new TableRebalanceProgressStats();
    completedStats.setStatus(RebalanceResult.Status.DONE);
    TableRebalanceProgressStats abortedStats = new TableRebalanceProgressStats();
    abortedStats.setStatus(RebalanceResult.Status.ABORTED);

    Map<String, Map<String, String>> jobMetadataMap = Map.of(
        "job1", Map.of(CommonConstants.ControllerJob.SUBMISSION_TIME_MS, "1000",
            CommonConstants.ControllerJob.JOB_TYPE, ControllerJobTypes.TABLE_REBALANCE.name(),
            RebalanceJobConstants.JOB_METADATA_KEY_REBALANCE_PROGRESS_STATS, JsonUtils.objectToString(completedStats)),
        "job2", Map.of(CommonConstants.ControllerJob.SUBMISSION_TIME_MS, "5000",
            CommonConstants.ControllerJob.JOB_TYPE, ControllerJobTypes.TABLE_REBALANCE.name(),
            RebalanceJobConstants.JOB_METADATA_KEY_REBALANCE_PROGRESS_STATS, JsonUtils.objectToString(completedStats)),
        "job3", Map.of(CommonConstants.ControllerJob.SUBMISSION_TIME_MS, "3000",
            CommonConstants.ControllerJob.JOB_TYPE, ControllerJobTypes.TABLE_REBALANCE.name(),
            RebalanceJobConstants.JOB_METADATA_KEY_REBALANCE_PROGRESS_STATS, JsonUtils.objectToString(abortedStats)),
        "job4", Map.of(CommonConstants.ControllerJob.SUBMISSION_TIME_MS, "2000",
            CommonConstants.ControllerJob.JOB_TYPE, ControllerJobTypes.TABLE_REBALANCE.name(),
            RebalanceJobConstants.JOB_METADATA_KEY_REBALANCE_PROGRESS_STATS, JsonUtils.objectToString(completedStats)),
        "job5", Map.of(CommonConstants.ControllerJob.SUBMISSION_TIME_MS, "4000",
            CommonConstants.ControllerJob.JOB_TYPE, ControllerJobTypes.TABLE_REBALANCE.name(),
            RebalanceJobConstants.JOB_METADATA_KEY_REBALANCE_PROGRESS_STATS, JsonUtils.objectToString(abortedStats))
    );

    Map<String, Map<String, String>> updatedJobMetadataMap =
        ControllerZkHelixUtils.expireControllerJobsInZk(jobMetadataMap, ControllerJobTypes.TABLE_REBALANCE);
    assertEquals(updatedJobMetadataMap.size(), 2);
    // Retain the two most recent jobs based on submission time
    assertEquals(updatedJobMetadataMap.keySet(), Set.of("job2", "job5"));
  }
}
