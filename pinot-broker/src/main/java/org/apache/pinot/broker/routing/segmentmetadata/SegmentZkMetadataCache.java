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
import java.util.List;
import java.util.Set;
import org.apache.helix.AccessOption;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.pinot.broker.routing.segmentpruner.SegmentPruner;
import org.apache.pinot.common.metadata.ZKMetadataProvider;


/**
 *
 */
public class SegmentZkMetadataCache {
  private final String _tableNameWithType;
  private final ZkHelixPropertyStore<ZNRecord> _propertyStore;
  private final String _segmentZKMetadataPathPrefix;
  private final List<SegmentPruner> _registeredPruners;

  public SegmentZkMetadataCache(String tableNameWithType, ZkHelixPropertyStore<ZNRecord> propertyStore,
      List<SegmentPruner> segmentPruners) {
    _tableNameWithType = tableNameWithType;
    _propertyStore = propertyStore;
    _segmentZKMetadataPathPrefix = ZKMetadataProvider.constructPropertyStorePathForResource(tableNameWithType) + "/";
    _registeredPruners = segmentPruners;
  }

  public void init(IdealState idealState, ExternalView externalView, Set<String> onlineSegments) {
    // Bulk load partition info for all online segments
    int numSegments = onlineSegments.size();
    List<String> segments = new ArrayList<>(numSegments);
    List<String> segmentZKMetadataPaths = new ArrayList<>(numSegments);
    for (String segment : onlineSegments) {
      segments.add(segment);
      segmentZKMetadataPaths.add(_segmentZKMetadataPathPrefix + segment);
    }
    List<ZNRecord> znRecords = _propertyStore.get(segmentZKMetadataPaths, null, AccessOption.PERSISTENT, false);
    for (SegmentPruner pruner : _registeredPruners) {
      pruner.init(idealState, externalView, onlineSegments, znRecords);
    }
  }

  public List<SegmentPruner> getPruners() {
    return _registeredPruners;
  }

  public synchronized void onAssignmentChange(IdealState idealState, ExternalView externalView,
      Set<String> onlineSegments) {
    int numSegments = onlineSegments.size();
    List<String> segments = new ArrayList<>(numSegments);
    List<String> segmentZKMetadataPaths = new ArrayList<>(numSegments);
    for (String segment : onlineSegments) {
      segments.add(segment);
      segmentZKMetadataPaths.add(_segmentZKMetadataPathPrefix + segment);
    }
    List<ZNRecord> znRecords = _propertyStore.get(segmentZKMetadataPaths, null, AccessOption.PERSISTENT, false);
    for (SegmentPruner segmentPruner : _registeredPruners) {
      segmentPruner.onAssignmentChange(idealState, externalView, onlineSegments, znRecords);
    }
  }

  public synchronized void refreshSegment(String segment) {
    ZNRecord znRecord = _propertyStore.get(_segmentZKMetadataPathPrefix + segment, null, AccessOption.PERSISTENT);
    for (SegmentPruner segmentPruner : _registeredPruners) {
      segmentPruner.refreshSegment(segment, znRecord);
    }
  }
}
