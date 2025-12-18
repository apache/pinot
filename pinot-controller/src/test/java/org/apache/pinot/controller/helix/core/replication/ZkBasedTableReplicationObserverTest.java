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
package org.apache.pinot.controller.helix.core.replication;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.controller.helix.core.controllerjob.ControllerJobTypes;
import org.apache.pinot.spi.utils.CommonConstants;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyMap;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

public class ZkBasedTableReplicationObserverTest {

  @Mock
  private PinotHelixResourceManager _pinotHelixResourceManager;

  private AutoCloseable _mocks;

  @BeforeMethod
  public void setUp() {
    _mocks = MockitoAnnotations.openMocks(this);
  }

  @AfterMethod
  public void tearDown() throws Exception {
    _mocks.close();
  }

  @Test
  public void testObserver() {
    String jobId = "job1";
    String tableName = "table1";
    List<String> segments = Arrays.asList("seg1", "seg2", "seg3");

    ZkBasedTableReplicationObserver observer =
        new ZkBasedTableReplicationObserver(jobId, tableName, segments, _pinotHelixResourceManager);

    // Trigger completion (1st segment) - no ZK update (only every 100 or error)
    // Total 3. remaining starts at 3.
    // complete seg1 -> remaining 2. 2 % 100 != 0.
    // complete seg2 -> remaining 1.
    // complete seg3 -> remaining 0. 0 % 100 == 0 -> ZK update.

    observer.onTrigger(TableReplicationObserver.Trigger.SEGMENT_REPLICATE_COMPLETED_TRIGGER, "seg1");
    verify(_pinotHelixResourceManager, never()).addControllerJobToZK(anyString(), anyMap(), any());

    observer.onTrigger(TableReplicationObserver.Trigger.SEGMENT_REPLICATE_COMPLETED_TRIGGER, "seg2");
    verify(_pinotHelixResourceManager, never()).addControllerJobToZK(anyString(), anyMap(), any());

    observer.onTrigger(TableReplicationObserver.Trigger.SEGMENT_REPLICATE_COMPLETED_TRIGGER, "seg3");

    ArgumentCaptor<Map<String, String>> metadataCaptor = ArgumentCaptor.forClass(Map.class);
    verify(_pinotHelixResourceManager).addControllerJobToZK(eq(jobId), metadataCaptor.capture(),
        eq(ControllerJobTypes.TABLE_REPLICATION));

    Map<String, String> metadata = metadataCaptor.getValue();
    Assert.assertEquals(metadata.get(CommonConstants.ControllerJob.JOB_ID), jobId);
    Assert.assertEquals(metadata.get(CommonConstants.ControllerJob.TABLE_NAME_WITH_TYPE), tableName);
  }

  @Test
  public void testObserverError() {
    String jobId = "job1";
    String tableName = "table1";
    List<String> segments = Arrays.asList("seg1");

    ZkBasedTableReplicationObserver observer =
        new ZkBasedTableReplicationObserver(jobId, tableName, segments, _pinotHelixResourceManager);

    observer.onTrigger(TableReplicationObserver.Trigger.SEGMENT_REPLICATE_ERRORED_TRIGGER, "seg1");

    verify(_pinotHelixResourceManager).addControllerJobToZK(eq(jobId), anyMap(),
        eq(ControllerJobTypes.TABLE_REPLICATION));
  }
}
