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
package org.apache.pinot.broker.routing.segmentmetadata;

import com.google.common.collect.ImmutableSet;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.pinot.broker.routing.segmentpruner.SegmentPruner;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.controller.helix.ControllerTest;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.CommonConstants.Segment.Realtime.Status;
import org.mockito.ArgumentMatcher;
import org.mockito.Mockito;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;


public class SegmentZkMetadataFetcherTest extends ControllerTest {
  private static final String OFFLINE_TABLE_NAME = "testTable_OFFLINE";

  @Test
  public void testSegmentZkMetadataFetcherShouldNotAllowIncorrectRegisterOrInitBehavior() {
    ZkHelixPropertyStore<ZNRecord> mockPropertyStore = Mockito.mock(ZkHelixPropertyStore.class);
    IdealState idealState = Mockito.mock(IdealState.class);
    ExternalView externalView = Mockito.mock(ExternalView.class);

    // empty listener at beginning
    SegmentZkMetadataFetcher segmentZkMetadataFetcher =
        new SegmentZkMetadataFetcher(OFFLINE_TABLE_NAME, mockPropertyStore);
    assertEquals(segmentZkMetadataFetcher.getListeners().size(), 0);

    // should allow register new listener
    segmentZkMetadataFetcher.register(mock(SegmentZkMetadataFetchListener.class));
    assertEquals(segmentZkMetadataFetcher.getListeners().size(), 1);

    // should not allow register new listener once initialized
    segmentZkMetadataFetcher.init(idealState, externalView, Set.of());
    try {
      segmentZkMetadataFetcher.register(mock(SegmentZkMetadataFetchListener.class));
      fail();
    } catch (RuntimeException rte) {
      assertTrue(rte.getMessage().contains("has already been initialized"));
    }

    // should not allow duplicate init either
    try {
      segmentZkMetadataFetcher.init(idealState, externalView, Set.of());
      fail();
    } catch (RuntimeException rte) {
      assertTrue(rte.getMessage().contains("has already been initialized"));
    }
  }

  @Test
  public void testSegmentZkMetadataFetcherShouldNotPullZkWhenNoPrunerRegistered() {
    ZkHelixPropertyStore<ZNRecord> mockPropertyStore = Mockito.mock(ZkHelixPropertyStore.class);
    SegmentZkMetadataFetcher segmentZkMetadataFetcher =
        new SegmentZkMetadataFetcher(OFFLINE_TABLE_NAME, mockPropertyStore);
    // NOTE: Ideal state and external view are not used in the current implementation
    IdealState idealState = Mockito.mock(IdealState.class);
    ExternalView externalView = Mockito.mock(ExternalView.class);

    assertEquals(segmentZkMetadataFetcher.getListeners().size(), 0);
    segmentZkMetadataFetcher.init(idealState, externalView, Set.of("foo"));
    Mockito.verify(mockPropertyStore, times(0)).get(any(), any(), anyInt(), anyBoolean());
    segmentZkMetadataFetcher.onAssignmentChange(idealState, externalView, Set.of("foo"));
    Mockito.verify(mockPropertyStore, times(0)).get(any(), any(), anyInt(), anyBoolean());
    segmentZkMetadataFetcher.refreshSegment("foo");
    Mockito.verify(mockPropertyStore, times(0)).get(any(), any(), anyInt(), anyBoolean());
  }

  @Test
  public void testSegmentZkMetadataFetcherShouldPullZkOnlyOncePerSegmentWhenMultiplePrunersRegistered() {
    ZkHelixPropertyStore<ZNRecord> mockPropertyStore = mock(ZkHelixPropertyStore.class);
    when(mockPropertyStore.get(any(), any(), anyInt(), anyBoolean())).thenAnswer(inv -> {
      List<String> pathList = inv.getArgument(0);
      List<ZNRecord> result = new ArrayList<>(pathList.size());
      for (String path : pathList) {
        String[] pathParts = path.split("/");
        String segmentName = pathParts[pathParts.length - 1];
        SegmentZKMetadata fakeSegmentZkMetadata = new SegmentZKMetadata(segmentName);
        result.add(fakeSegmentZkMetadata.toZNRecord());
      }
      return result;
    });
    SegmentPruner pruner1 = mock(SegmentPruner.class);
    SegmentPruner pruner2 = mock(SegmentPruner.class);
    SegmentZkMetadataFetcher segmentZkMetadataFetcher =
        new SegmentZkMetadataFetcher(OFFLINE_TABLE_NAME, mockPropertyStore);
    segmentZkMetadataFetcher.register(pruner1);
    segmentZkMetadataFetcher.register(pruner2);
    // NOTE: Ideal state and external view are not used in the current implementation
    IdealState idealState = mock(IdealState.class);
    ExternalView externalView = mock(ExternalView.class);

    assertEquals(segmentZkMetadataFetcher.getListeners().size(), 2);
    // should call property store once for "foo" and "bar" as a batch
    segmentZkMetadataFetcher.init(idealState, externalView, ImmutableSet.of("foo", "bar"));
    verify(mockPropertyStore, times(1)).get(argThat(new ListMatcher("foo", "bar")), any(), anyInt(), anyBoolean());
    verify(pruner1, times(1)).init(any(), any(), argThat(new ListMatcher("foo", "bar")), any());
    verify(pruner2, times(1)).init(any(), any(), argThat(new ListMatcher("foo", "bar")), any());

    // should call property store only once b/c "alice" was missing
    segmentZkMetadataFetcher.onAssignmentChange(idealState, externalView, ImmutableSet.of("bar", "alice"));
    verify(mockPropertyStore, times(1)).get(argThat(new ListMatcher("alice")), any(), anyInt(), anyBoolean());
    verify(pruner1, times(1)).onAssignmentChange(any(), any(), any(), argThat(new ListMatcher("alice")), any());
    verify(pruner2, times(1)).onAssignmentChange(any(), any(), any(), argThat(new ListMatcher("alice")), any());

    // should call property store once more b/c "foo" was cleared when onAssignmentChange called with "bar" and "alice"
    segmentZkMetadataFetcher.refreshSegment("foo");
    verify(mockPropertyStore, times(1)).get(endsWith("foo"), any(), anyInt());
    verify(pruner1, times(1)).refreshSegment(eq("foo"), any());
    verify(pruner2, times(1)).refreshSegment(eq("foo"), any());
    clearInvocations(mockPropertyStore, pruner1, pruner2);

    // update with all existing segments will call into property store and pruner with empty list
    segmentZkMetadataFetcher.onAssignmentChange(idealState, externalView, ImmutableSet.of("bar", "alice"));
    verify(mockPropertyStore, times(1)).get(argThat(new ListMatcher()), any(), anyInt(), anyBoolean());
    verify(pruner1, times(1)).onAssignmentChange(any(), any(), any(), argThat(new ListMatcher()), any());
    verify(pruner2, times(1)).onAssignmentChange(any(), any(), any(), argThat(new ListMatcher()), any());

    // calling refresh will still force pull from property store
    segmentZkMetadataFetcher.refreshSegment("foo");
    verify(mockPropertyStore, times(1)).get(endsWith("foo"), any(), anyInt());
    verify(pruner1, times(1)).refreshSegment(eq("foo"), any());
    verify(pruner2, times(1)).refreshSegment(eq("foo"), any());
  }

  @Test
  public void testSegmentZkMetadataFetcherShouldFetchFirstSightEvenWhenConsuming() {
    // Status of the ZNRecord returned for "segment"; mutated across calls to simulate its lifecycle.
    Status[] segmentStatus = {Status.COMMITTING};
    ZkHelixPropertyStore<ZNRecord> mockPropertyStore = mock(ZkHelixPropertyStore.class);
    when(mockPropertyStore.get(any(), any(), anyInt(), anyBoolean())).thenAnswer(inv -> {
      List<String> pathList = inv.getArgument(0);
      List<ZNRecord> result = new ArrayList<>(pathList.size());
      for (String path : pathList) {
        String[] pathParts = path.split("/");
        String segmentName = pathParts[pathParts.length - 1];
        SegmentZKMetadata segmentZKMetadata = new SegmentZKMetadata(segmentName);
        segmentZKMetadata.setStatus(segmentStatus[0]);
        result.add(segmentZKMetadata.toZNRecord());
      }
      return result;
    });
    SegmentPruner pruner = mock(SegmentPruner.class);
    SegmentZkMetadataFetcher segmentZkMetadataFetcher =
        new SegmentZkMetadataFetcher(OFFLINE_TABLE_NAME, mockPropertyStore);
    segmentZkMetadataFetcher.register(pruner);
    IdealState idealState = mock(IdealState.class);
    segmentZkMetadataFetcher.init(idealState, mock(ExternalView.class), Set.of());
    clearInvocations(mockPropertyStore);

    // Segment is CONSUMING from the very first time it is observed via onAssignmentChange (not init): it must
    // still be fetched and listeners notified -- a segment must never be silently dropped just because it is
    // already CONSUMING in the ExternalView the first time it is seen.
    ExternalView consumingExternalView = mock(ExternalView.class);
    when(consumingExternalView.getStateMap("segment")).thenReturn(
        Map.of("server0", CommonConstants.Helix.StateModel.SegmentStateModel.CONSUMING));
    segmentZkMetadataFetcher.onAssignmentChange(idealState, consumingExternalView, Set.of("segment"));
    verify(mockPropertyStore, times(1)).get(argThat(new ListMatcher("segment")), any(), anyInt(), anyBoolean());
    verify(pruner, times(1)).onAssignmentChange(any(), any(), any(), argThat(new ListMatcher("segment")), any());

    // Still CONSUMING according to the ExternalView, and already seen once: this is now a cheap-skip case, so no
    // ZK read or listener notification should happen again while nothing has changed.
    segmentZkMetadataFetcher.onAssignmentChange(idealState, consumingExternalView, Set.of("segment"));
    verify(mockPropertyStore, times(1)).get(argThat(new ListMatcher("segment")), any(), anyInt(), anyBoolean());
    verify(pruner, times(1)).onAssignmentChange(any(), any(), any(), argThat(new ListMatcher("segment")), any());

    // ExternalView no longer reports CONSUMING (e.g. moved to ONLINE): should re-fetch to check the real status,
    // even though the segment isn't committed yet (pauseless COMMITTING window) and thus still isn't cached.
    ExternalView onlineExternalView = mock(ExternalView.class);
    when(onlineExternalView.getStateMap("segment")).thenReturn(
        Map.of("server0", CommonConstants.Helix.StateModel.SegmentStateModel.ONLINE));
    segmentZkMetadataFetcher.onAssignmentChange(idealState, onlineExternalView, Set.of("segment"));
    verify(mockPropertyStore, times(2)).get(argThat(new ListMatcher("segment")), any(), anyInt(), anyBoolean());
    verify(pruner, times(2)).onAssignmentChange(any(), any(), any(), argThat(new ListMatcher("segment")), any());

    // Segment commits (status becomes DONE): should be fetched, notified, and now cached
    segmentStatus[0] = Status.DONE;
    segmentZkMetadataFetcher.onAssignmentChange(idealState, onlineExternalView, Set.of("segment"));
    verify(mockPropertyStore, times(3)).get(argThat(new ListMatcher("segment")), any(), anyInt(), anyBoolean());
    verify(pruner, times(3)).onAssignmentChange(any(), any(), any(), argThat(new ListMatcher("segment")), any());

    // Already cached: subsequent assignment changes should not re-fetch or re-notify
    segmentZkMetadataFetcher.onAssignmentChange(idealState, onlineExternalView, Set.of("segment"));
    verify(mockPropertyStore, times(3)).get(argThat(new ListMatcher("segment")), any(), anyInt(), anyBoolean());
    verify(pruner, times(3)).onAssignmentChange(any(), any(), any(), argThat(new ListMatcher("segment")), any());
  }

  @Test
  public void testSegmentZkMetadataFetcherShouldNotCachePreCommitZNRecordEvenWithoutExternalViewEntry() {
    // Regression test for the event-ordering race: onlineSegments is derived from IdealState, so a brand-new
    // segment can be observed here before its ExternalView entry exists (getStateMap returns null). The caching
    // decision must not treat that as "not consuming" and cache the segment before it actually commits.
    ZkHelixPropertyStore<ZNRecord> mockPropertyStore = mock(ZkHelixPropertyStore.class);
    when(mockPropertyStore.get(any(), any(), anyInt(), anyBoolean())).thenAnswer(inv -> {
      List<String> pathList = inv.getArgument(0);
      List<ZNRecord> result = new ArrayList<>(pathList.size());
      for (String path : pathList) {
        String[] pathParts = path.split("/");
        String segmentName = pathParts[pathParts.length - 1];
        SegmentZKMetadata segmentZKMetadata = new SegmentZKMetadata(segmentName);
        segmentZKMetadata.setStatus(Status.COMMITTING);
        result.add(segmentZKMetadata.toZNRecord());
      }
      return result;
    });
    SegmentPruner pruner = mock(SegmentPruner.class);
    SegmentZkMetadataFetcher segmentZkMetadataFetcher =
        new SegmentZkMetadataFetcher(OFFLINE_TABLE_NAME, mockPropertyStore);
    segmentZkMetadataFetcher.register(pruner);
    IdealState idealState = mock(IdealState.class);
    segmentZkMetadataFetcher.init(idealState, mock(ExternalView.class), Set.of());
    clearInvocations(mockPropertyStore);

    // ExternalView has no entry at all for "segment" (as if IdealState discovered it first)
    ExternalView externalViewMissingEntry = mock(ExternalView.class);
    when(externalViewMissingEntry.getStateMap("segment")).thenReturn(null);
    segmentZkMetadataFetcher.onAssignmentChange(idealState, externalViewMissingEntry, Set.of("segment"));
    verify(mockPropertyStore, times(1)).get(argThat(new ListMatcher("segment")), any(), anyInt(), anyBoolean());

    // Since the ZNRecord itself still reports COMMITTING, the segment must not have been cached: a subsequent
    // call must fetch it again instead of treating it as already-committed.
    segmentZkMetadataFetcher.onAssignmentChange(idealState, externalViewMissingEntry, Set.of("segment"));
    verify(mockPropertyStore, times(2)).get(argThat(new ListMatcher("segment")), any(), anyInt(), anyBoolean());
  }

  private static class ListMatcher implements ArgumentMatcher<List<String>> {
    private final List<String> _valueToMatch;

    private ListMatcher(String... values) {
      _valueToMatch = Arrays.asList(values);
    }

    @Override
    public boolean matches(List<String> arg) {
      if (arg.size() != _valueToMatch.size()) {
        return false;
      }
      for (int i = 0; i < arg.size(); i++) {
        if (!arg.get(i).endsWith(_valueToMatch.get(i))) {
          return false;
        }
      }
      return true;
    }
  }
}
