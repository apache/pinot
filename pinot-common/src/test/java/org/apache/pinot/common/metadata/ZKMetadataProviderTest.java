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
package org.apache.pinot.common.metadata;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.helix.AccessOption;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.zookeeper.data.Stat;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * Tests for {@link ZKMetadataProvider}.
 */
public class ZKMetadataProviderTest {
  private static final String TABLE_NAME_WITH_TYPE = "testTable_OFFLINE";

  @Test
  public void testForEachSegmentZKMetadataEmptySegments() {
    ZkHelixPropertyStore<ZNRecord> mockPropertyStore = Mockito.mock(ZkHelixPropertyStore.class);
    String segmentsPath = ZKMetadataProvider.constructPropertyStorePathForResource(TABLE_NAME_WITH_TYPE);
    Mockito.when(mockPropertyStore.exists(segmentsPath, AccessOption.PERSISTENT)).thenReturn(true);
    Mockito.when(
        mockPropertyStore.getChildNames(segmentsPath, AccessOption.PERSISTENT)).thenReturn(Collections.emptyList());

    List<String> segmentNames = new ArrayList<>();
    ZKMetadataProvider.forEachSegmentZKMetadata(mockPropertyStore, TABLE_NAME_WITH_TYPE, 2,
        segmentZKMetadata -> segmentNames.add(segmentZKMetadata.getSegmentName()));

    Assert.assertTrue(segmentNames.isEmpty());
    Mockito.verify(mockPropertyStore, Mockito.never())
        .get(Mockito.<String>anyList(), Mockito.<Stat>anyList(),
            ArgumentMatchers.eq(AccessOption.PERSISTENT));
  }

  @Test
  public void testForEachSegmentZKMetadataBatchesAndNullRecords() {
    ZkHelixPropertyStore<ZNRecord> mockPropertyStore = Mockito.mock(ZkHelixPropertyStore.class);
    String segmentsPath = ZKMetadataProvider.constructPropertyStorePathForResource(TABLE_NAME_WITH_TYPE);
    List<String> segmentNames = Arrays.asList("segment-1", "segment-2", "segment-3", "segment-4");
    Mockito.when(mockPropertyStore.exists(segmentsPath, AccessOption.PERSISTENT)).thenReturn(true);
    Mockito.when(mockPropertyStore.getChildNames(segmentsPath, AccessOption.PERSISTENT)).thenReturn(segmentNames);

    List<List<String>> requestedBatches = new ArrayList<>();
    Mockito.when(mockPropertyStore.get(Mockito.<String>anyList(), Mockito.isNull(),
        ArgumentMatchers.eq(AccessOption.PERSISTENT))).thenAnswer(invocation -> {
      List<String> requestedSegments = invocation.getArgument(0);
      requestedBatches.add(new ArrayList<>(requestedSegments));
      if (requestedSegments.equals(Arrays.asList(constructSegmentMetadataPath("segment-1"),
          constructSegmentMetadataPath("segment-2")))) {
        return Arrays.asList(createSegmentMetadata("segment-1"), null);
      }
      if (requestedSegments.equals(Arrays.asList(constructSegmentMetadataPath("segment-3"),
          constructSegmentMetadataPath("segment-4")))) {
        return Collections.singletonList(createSegmentMetadata("segment-3"));
      }
      return Collections.emptyList();
    });

    List<String> consumedSegments = new ArrayList<>();
    ZKMetadataProvider.forEachSegmentZKMetadata(mockPropertyStore, TABLE_NAME_WITH_TYPE, 2,
        segmentZKMetadata -> consumedSegments.add(segmentZKMetadata.getSegmentName()));

    Assert.assertEquals(consumedSegments, Arrays.asList("segment-1", "segment-3"));
    Assert.assertEquals(requestedBatches, Arrays.asList(
        Arrays.asList(constructSegmentMetadataPath("segment-1"), constructSegmentMetadataPath("segment-2")),
        Arrays.asList(constructSegmentMetadataPath("segment-3"), constructSegmentMetadataPath("segment-4"))));
  }

  @Test
  public void testForEachSegmentZKMetadataRequiresPositiveBatchSize() {
    ZkHelixPropertyStore<ZNRecord> mockPropertyStore = Mockito.mock(ZkHelixPropertyStore.class);
    Assert.assertThrows(IllegalArgumentException.class,
        () -> ZKMetadataProvider.forEachSegmentZKMetadata(mockPropertyStore, TABLE_NAME_WITH_TYPE, 0,
            segmentZKMetadata -> {
            }));
  }

  private ZNRecord createSegmentMetadata(String segmentName) {
    return new ZNRecord(segmentName);
  }

  private String constructSegmentMetadataPath(String segmentName) {
    return ZKMetadataProvider.constructPropertyStorePathForSegment(TABLE_NAME_WITH_TYPE, segmentName);
  }
}
