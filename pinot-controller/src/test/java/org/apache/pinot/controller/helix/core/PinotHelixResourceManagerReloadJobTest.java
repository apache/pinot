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
package org.apache.pinot.controller.helix.core;

import java.util.HashMap;
import java.util.Map;
import org.apache.pinot.controller.ControllerConf;
import org.apache.pinot.controller.helix.core.controllerjob.ControllerJobTypes;
import org.apache.pinot.spi.controller.ControllerJobType;
import org.apache.pinot.spi.utils.CommonConstants;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

public class PinotHelixResourceManagerReloadJobTest {

  private static class CapturingHelixResourceManager extends PinotHelixResourceManager {
    private String _jobId;
    private Map<String, String> _jobMetadata;
    private ControllerJobType _jobType;

    CapturingHelixResourceManager() {
      super(new ControllerConf());
    }

    @Override
    public boolean addControllerJobToZK(String jobId, Map<String, String> jobMetadata, ControllerJobType jobType) {
      _jobId = jobId;
      _jobMetadata = new HashMap<>(jobMetadata);
      _jobType = jobType;
      return true;
    }
  }

  @Test
  public void testAddNewReloadSegmentJobCapturesMetadataWithInstance() {
    CapturingHelixResourceManager manager = new CapturingHelixResourceManager();

    boolean result =
        manager.addNewReloadSegmentJob("table_OFFLINE", "seg_1,seg_2", "server1", "job-1", 123L, 4);

    assertTrue(result);
    assertEquals(manager._jobId, "job-1");
    assertEquals(manager._jobType, ControllerJobTypes.RELOAD_SEGMENT);
    assertEquals(manager._jobMetadata.get(CommonConstants.ControllerJob.JOB_ID), "job-1");
    assertEquals(manager._jobMetadata.get(CommonConstants.ControllerJob.TABLE_NAME_WITH_TYPE), "table_OFFLINE");
    assertEquals(manager._jobMetadata.get(CommonConstants.ControllerJob.JOB_TYPE),
        ControllerJobTypes.RELOAD_SEGMENT.name());
    assertEquals(manager._jobMetadata.get(CommonConstants.ControllerJob.SUBMISSION_TIME_MS), "123");
    assertEquals(manager._jobMetadata.get(CommonConstants.ControllerJob.MESSAGE_COUNT), "4");
    assertEquals(manager._jobMetadata.get(CommonConstants.ControllerJob.SEGMENT_RELOAD_JOB_SEGMENT_NAME),
        "seg_1,seg_2");
    assertEquals(manager._jobMetadata.get(CommonConstants.ControllerJob.SEGMENT_RELOAD_JOB_INSTANCE_NAME), "server1");
  }

  @Test
  public void testAddNewReloadSegmentJobOmitsInstanceWhenNull() {
    CapturingHelixResourceManager manager = new CapturingHelixResourceManager();

    boolean result =
        manager.addNewReloadSegmentJob("table_OFFLINE", "seg_1", null, "job-2", 456L, 1);

    assertTrue(result);
    assertNull(manager._jobMetadata.get(CommonConstants.ControllerJob.SEGMENT_RELOAD_JOB_INSTANCE_NAME));
  }
}
