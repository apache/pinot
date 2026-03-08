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
package org.apache.pinot.spi.stream;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertSame;


public class StreamMetadataTest {

  @Test
  public void testGetters() {
    StreamConfig streamConfig = mock(StreamConfig.class);
    when(streamConfig.getTopicName()).thenReturn("test-topic");

    PartitionGroupMetadata pg0 = new PartitionGroupMetadata(0, mock(StreamPartitionMsgOffset.class));
    PartitionGroupMetadata pg1 = new PartitionGroupMetadata(1, mock(StreamPartitionMsgOffset.class), 5);
    List<PartitionGroupMetadata> pgList = Arrays.asList(pg0, pg1);

    StreamMetadata sm = new StreamMetadata(streamConfig, 10, pgList);

    assertSame(sm.getStreamConfig(), streamConfig);
    assertEquals(sm.getTopicName(), "test-topic");
    assertEquals(sm.getNumPartitions(), 10);
    assertEquals(sm.getPartitionGroupMetadataList().size(), 2);
    assertSame(sm.getPartitionGroupMetadataList().get(0), pg0);
    assertSame(sm.getPartitionGroupMetadataList().get(1), pg1);
  }

  @Test
  public void testNumPartitionsCanExceedListSize() {
    StreamConfig streamConfig = mock(StreamConfig.class);
    PartitionGroupMetadata pg = new PartitionGroupMetadata(0, mock(StreamPartitionMsgOffset.class));

    StreamMetadata sm = new StreamMetadata(streamConfig, 100, Collections.singletonList(pg));

    assertEquals(sm.getNumPartitions(), 100);
    assertEquals(sm.getPartitionGroupMetadataList().size(), 1);
  }

  @Test
  public void testDefensiveCopy() {
    StreamConfig streamConfig = mock(StreamConfig.class);
    PartitionGroupMetadata pg0 = new PartitionGroupMetadata(0, mock(StreamPartitionMsgOffset.class));
    List<PartitionGroupMetadata> mutableList = new ArrayList<>();
    mutableList.add(pg0);

    StreamMetadata sm = new StreamMetadata(streamConfig, 1, mutableList);
    assertEquals(sm.getPartitionGroupMetadataList().size(), 1);

    // Mutating the original list should not affect StreamMetadata
    mutableList.add(new PartitionGroupMetadata(1, mock(StreamPartitionMsgOffset.class)));
    assertEquals(sm.getPartitionGroupMetadataList().size(), 1);
  }

  @Test(expectedExceptions = UnsupportedOperationException.class)
  public void testPartitionGroupMetadataListIsUnmodifiable() {
    StreamConfig streamConfig = mock(StreamConfig.class);
    PartitionGroupMetadata pg = new PartitionGroupMetadata(0, mock(StreamPartitionMsgOffset.class));

    StreamMetadata sm = new StreamMetadata(streamConfig, 1, Collections.singletonList(pg));
    sm.getPartitionGroupMetadataList().add(new PartitionGroupMetadata(1, mock(StreamPartitionMsgOffset.class)));
  }

  @Test
  public void testEmptyPartitionGroupMetadataList() {
    StreamConfig streamConfig = mock(StreamConfig.class);

    StreamMetadata sm = new StreamMetadata(streamConfig, 5, Collections.emptyList());

    assertEquals(sm.getNumPartitions(), 5);
    assertEquals(sm.getPartitionGroupMetadataList().size(), 0);
  }
}
