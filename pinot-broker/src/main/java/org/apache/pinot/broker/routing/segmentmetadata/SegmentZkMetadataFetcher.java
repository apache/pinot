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
import java.util.Set;
import org.apache.helix.AccessOption;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.pinot.common.metadata.ZKMetadataProvider;


/**
 * {@code SegmentZkMetadataFetcher} is used to cache {@link ZNRecord} stored in {@link ZkHelixPropertyStore} for
 * segments.
 */
public class SegmentZkMetadataFetcher {
  private final String _tableNameWithType;
  private final ZkHelixPropertyStore<ZNRecord> _propertyStore;
  private final String _segmentZKMetadataPathPrefix;
  private final List<SegmentZkMetadataFetchListener> _listeners;
  private final Set<String> _onlineSegmentsCached;

  private boolean _initialized;

  public SegmentZkMetadataFetcher(String tableNameWithType, ZkHelixPropertyStore<ZNRecord> propertyStore) {
    _tableNameWithType = tableNameWithType;
    _propertyStore = propertyStore;
    _segmentZKMetadataPathPrefix = ZKMetadataProvider.constructPropertyStorePathForResource(tableNameWithType) + "/";
    _listeners = new ArrayList<>();
    _onlineSegmentsCached = new HashSet<>();
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
          if (znRecords.get(i) != null) {
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
        if (!_onlineSegmentsCached.contains(segment)) {
          segments.add(segment);
          segmentZKMetadataPaths.add(_segmentZKMetadataPathPrefix + segment);
        }
      }
      List<ZNRecord> znRecords = _propertyStore.get(segmentZKMetadataPaths, null, AccessOption.PERSISTENT, false);
      for (SegmentZkMetadataFetchListener listener : _listeners) {
        listener.onAssignmentChange(idealState, externalView, onlineSegments, segments, znRecords);
      }
      int numSegments = segments.size();
      for (int i = 0; i < numSegments; i++) {
        if (znRecords.get(i) != null) {
          _onlineSegmentsCached.add(segments.get(i));
        }
      }
      _onlineSegmentsCached.retainAll(onlineSegments);
    }
  }

  public synchronized void refreshSegment(String segment) {
    if (!_listeners.isEmpty()) {
      ZNRecord znRecord = _propertyStore.get(_segmentZKMetadataPathPrefix + segment, null, AccessOption.PERSISTENT);
      for (SegmentZkMetadataFetchListener listener : _listeners) {
        listener.refreshSegment(segment, znRecord);
      }
      if (znRecord != null) {
        _onlineSegmentsCached.add(segment);
      } else {
        _onlineSegmentsCached.remove(segment);
      }
    }
  }
}
