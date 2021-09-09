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
package org.apache.pinot.core.data.manager.realtime;

import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.common.utils.HLCSegmentName;
import org.apache.pinot.common.utils.LLCSegmentName;
import org.testng.annotations.Test;

import static org.apache.pinot.spi.utils.CommonConstants.Segment.Realtime.Status;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;


public class RealtimeTableDataManagerTest {
  @Test
  public void testAllowDownload() {
    RealtimeTableDataManager mgr = new RealtimeTableDataManager(null);

    final String groupId = "myTable_REALTIME_1234567_0";
    final String partitionRange = "ALL";
    final String sequenceNumber = "1234567";
    HLCSegmentName hlc = new HLCSegmentName(groupId, partitionRange, sequenceNumber);
    assertFalse(mgr.allowDownload(hlc.getSegmentName(), null));

    LLCSegmentName llc = new LLCSegmentName("tbl01", 0, 1000000, System.currentTimeMillis());
    SegmentZKMetadata zkmd = mock(SegmentZKMetadata.class);
    when(zkmd.getStatus()).thenReturn(Status.IN_PROGRESS);
    assertFalse(mgr.allowDownload(llc.getSegmentName(), zkmd));

    when(zkmd.getStatus()).thenReturn(Status.DONE);
    when(zkmd.getDownloadUrl()).thenReturn("");
    assertFalse(mgr.allowDownload(llc.getSegmentName(), zkmd));

    when(zkmd.getDownloadUrl()).thenReturn("remote");
    assertTrue(mgr.allowDownload(llc.getSegmentName(), zkmd));
  }
}
