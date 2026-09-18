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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.helix.AccessOption;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.pinot.common.metadata.ZKMetadataProvider;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.spi.utils.CommonConstants;


/// `SegmentZkMetadataFetcher` is used to cache [ZNRecord] stored in [ZkHelixPropertyStore] for
/// segments.
public class SegmentZkMetadataFetcher {
  private final String _tableNameWithType;
  private final ZkHelixPropertyStore<ZNRecord> _propertyStore;
  private final String _segmentZKMetadataPathPrefix;
  private final List<SegmentZkMetadataFetchListener> _listeners;
  private final Set<String> _onlineSegmentsCached;
  private final Set<String> _segmentsSeen;

  private boolean _initialized;

  public SegmentZkMetadataFetcher(String tableNameWithType, ZkHelixPropertyStore<ZNRecord> propertyStore) {
    _tableNameWithType = tableNameWithType;
    _propertyStore = propertyStore;
    _segmentZKMetadataPathPrefix = ZKMetadataProvider.constructPropertyStorePathForResource(tableNameWithType) + "/";
    _listeners = new ArrayList<>();
    _onlineSegmentsCached = new HashSet<>();
    _segmentsSeen = new HashSet<>();
    _initialized = false;
  }

  public void register(SegmentZkMetadataFetchListener listener) {
    if (!_initialized) {
      _listeners.add(listener);
    } else {
      throw new RuntimeException(
          "Segment ZK metadata fetcher has already been initialized! Unable to register more listeners.");
    }
  }

  public List<SegmentZkMetadataFetchListener> getListeners() {
    return _listeners;
  }

  public void init(IdealState idealState, ExternalView externalView, Set<String> onlineSegments) {
    if (!_initialized) {
      _initialized = true;
      if (!_listeners.isEmpty()) {
        // Bulk load partition info for all online segments
        int numSegments = onlineSegments.size();
        List<String> segments = new ArrayList<>(numSegments);
        List<String> segmentZKMetadataPaths = new ArrayList<>(numSegments);
        for (String segment : onlineSegments) {
          segments.add(segment);
          segmentZKMetadataPaths.add(_segmentZKMetadataPathPrefix + segment);
        }
        List<ZNRecord> znRecords = _propertyStore.get(segmentZKMetadataPaths, null, AccessOption.PERSISTENT, false);
        for (SegmentZkMetadataFetchListener listener : _listeners) {
          listener.init(idealState, externalView, segments, znRecords);
        }
        for (int i = 0; i < numSegments; i++) {
          _segmentsSeen.add(segments.get(i));
          // Only cache segments whose ZK metadata reports a terminal (committed) status. Segments still being
          // consumed or committed are left uncached so they are re-fetched on the next onAssignmentChange.
          if (isSegmentCommitted(znRecords.get(i))) {
            _onlineSegmentsCached.add(segments.get(i));
          }
        }
      }
    } else {
      throw new RuntimeException("Segment ZK metadata fetcher has already been initialized!");
    }
  }

  public synchronized void onAssignmentChange(IdealState idealState, ExternalView externalView,
      Set<String> onlineSegments) {
    if (!_listeners.isEmpty()) {
      List<String> segments = new ArrayList<>();
      List<String> segmentZKMetadataPaths = new ArrayList<>();
      for (String segment : onlineSegments) {
        if (_onlineSegmentsCached.contains(segment)) {
          continue;
        }
        // A segment must always be fetched (and listeners notified) the first time it is observed, even if it is
        // already CONSUMING in the ExternalView -- listeners like the time/partition pruners need at least one
        // notification to record a (possibly default/uncommitted) entry for it. Once a segment has been seen at
        // least once, ExternalView state becomes a cheap pre-filter to avoid re-fetching ZK metadata on every
        // assignment-change cycle while it is still CONSUMING: skip the re-fetch only when EV confirms CONSUMING,
        // and re-fetch whenever EV shows otherwise (e.g. ONLINE) or its entry is missing (EV can lag IdealState),
        // so the segment's actual (possibly still non-terminal) ZK status is re-checked instead of assumed.
        if (_segmentsSeen.contains(segment) && isConsumingInExternalView(externalView, segment)) {
          continue;
        }
        segments.add(segment);
        segmentZKMetadataPaths.add(_segmentZKMetadataPathPrefix + segment);
      }
      List<ZNRecord> znRecords = _propertyStore.get(segmentZKMetadataPaths, null, AccessOption.PERSISTENT, false);
      for (SegmentZkMetadataFetchListener listener : _listeners) {
        listener.onAssignmentChange(idealState, externalView, onlineSegments, segments, znRecords);
      }
      int numSegments = segments.size();
      for (int i = 0; i < numSegments; i++) {
        _segmentsSeen.add(segments.get(i));
        if (isSegmentCommitted(znRecords.get(i))) {
          _onlineSegmentsCached.add(segments.get(i));
        }
      }
      _onlineSegmentsCached.retainAll(onlineSegments);
      _segmentsSeen.retainAll(onlineSegments);
    }
  }

  public synchronized void refreshSegment(String segment) {
    if (!_listeners.isEmpty()) {
      ZNRecord znRecord = _propertyStore.get(_segmentZKMetadataPathPrefix + segment, null, AccessOption.PERSISTENT);
      for (SegmentZkMetadataFetchListener listener : _listeners) {
        listener.refreshSegment(segment, znRecord);
      }
      _segmentsSeen.add(segment);
      if (isSegmentCommitted(znRecord)) {
        _onlineSegmentsCached.add(segment);
      } else {
        _onlineSegmentsCached.remove(segment);
      }
    }
  }

  /// Returns true if the given ZK metadata record represents a segment that has reached a terminal (committed)
  /// status, i.e. its metadata is final and safe to cache. Segments that are still consuming or committing report
  /// this as `false` so they keep getting re-fetched until their final metadata (e.g. time range, partition info)
  /// is available.
  private static boolean isSegmentCommitted(@Nullable ZNRecord znRecord) {
    return znRecord != null && new SegmentZKMetadata(znRecord).getStatus().isCompleted();
  }

  /// Returns true if the segment is in CONSUMING state on any server in the ExternalView. This is used only as a
  /// cheap pre-filter to skip re-fetching ZK metadata for a segment that has already been fetched at least once and
  /// is still being consumed -- it must never be relied on to skip the *first* fetch of a segment, since
  /// ExternalView can lag IdealState and a missing/stale entry must not be mistaken for "not consuming".
  private static boolean isConsumingInExternalView(ExternalView externalView, String segment) {
    Map<String, String> stateMap = externalView.getStateMap(segment);
    return stateMap != null && stateMap.containsValue(CommonConstants.Helix.StateModel.SegmentStateModel.CONSUMING);
  }
}
