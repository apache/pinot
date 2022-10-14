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
package org.apache.pinot.controller.helix.core.relocation;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.common.tier.FixedTierSegmentSelector;
import org.apache.pinot.common.tier.Tier;
import org.apache.pinot.controller.ControllerConf;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.spi.utils.CommonConstants;
import org.mockito.ArgumentCaptor;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;


public class SegmentTierAssignerTest {
  @Test
  public void testUpdateSegmentTierBackToDefault() {
    String tableName = "table01_OFFLINE";
    String segmentName = "seg01";
    PinotHelixResourceManager helixMgrMock = mock(PinotHelixResourceManager.class);
    when(helixMgrMock.getSegmentMetadataZnRecord(tableName, segmentName))
        .thenReturn(createSegmentMetadataZNRecord(segmentName, "hotTier"));
    SegmentTierAssigner assigner = new SegmentTierAssigner(helixMgrMock, null, new ControllerConf(), null);

    // Move back to default as not tier configs.
    List<Tier> sortedTiers = new ArrayList<>();
    assigner.updateSegmentTier(tableName, "seg01", sortedTiers);
    ArgumentCaptor<SegmentZKMetadata> recordCapture = ArgumentCaptor.forClass(SegmentZKMetadata.class);
    verify(helixMgrMock).updateZkMetadata(eq(tableName), recordCapture.capture(), eq(10));
    SegmentZKMetadata record = recordCapture.getValue();
    assertNull(record.getTier());
  }

  @Test
  public void testUpdateSegmentTierToNewTier() {
    String tableName = "table01_OFFLINE";
    String segmentName = "seg01";
    PinotHelixResourceManager helixMgrMock = mock(PinotHelixResourceManager.class);
    when(helixMgrMock.getSegmentMetadataZnRecord(tableName, segmentName))
        .thenReturn(createSegmentMetadataZNRecord(segmentName, "hotTier"));
    SegmentTierAssigner assigner = new SegmentTierAssigner(helixMgrMock, null, new ControllerConf(), null);

    // Move back to default as not tier configs.
    List<Tier> sortedTiers = new ArrayList<>();
    sortedTiers.add(new Tier("coldTier", new FixedTierSegmentSelector(null, Collections.singleton("seg01")), null));
    assigner.updateSegmentTier(tableName, "seg01", sortedTiers);
    ArgumentCaptor<SegmentZKMetadata> recordCapture = ArgumentCaptor.forClass(SegmentZKMetadata.class);
    verify(helixMgrMock).updateZkMetadata(eq(tableName), recordCapture.capture(), eq(10));
    SegmentZKMetadata record = recordCapture.getValue();
    assertEquals(record.getTier(), "coldTier");
  }

  private static ZNRecord createSegmentMetadataZNRecord(String segmentName, String tierName) {
    ZNRecord segmentMetadataZNRecord = new ZNRecord(segmentName);
    segmentMetadataZNRecord.setVersion(10);
    segmentMetadataZNRecord.setSimpleField(CommonConstants.Segment.TIER, tierName);
    return segmentMetadataZNRecord;
  }
}
