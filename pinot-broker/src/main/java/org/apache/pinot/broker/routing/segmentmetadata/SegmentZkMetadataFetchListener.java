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

import java.util.List;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState;
import org.apache.helix.zookeeper.datamodel.ZNRecord;


/// Interface to register with [SegmentZkMetadataFetcher].
///
/// When registered, SegmentZKMetadataFetcher will fetch [ZNRecord] for associated `onlineSegments` list
/// or refreshed `segment`. Thus batch up all ZK access for segment metadata.
public interface SegmentZkMetadataFetchListener {

  /// Initializes the segment pruner with the ideal state, external view and online segments (segments with
  /// ONLINE/CONSUMING instances in the ideal state and pre-selected by the
  /// [org.apache.pinot.broker.routing.segmentpreselector.SegmentPreSelector]). Should be called only once before
  /// calling other methods.
  void init(IdealState idealState, ExternalView externalView, List<String> onlineSegments, List<ZNRecord> znRecords);

  /// Processes the segment assignment (ideal state or external view) change based on the given online segments
  /// (segments with ONLINE/CONSUMING instances in the ideal state and pre-selected by the
  /// [org.apache.pinot.broker.routing.segmentpreselector.SegmentPreSelector]).
  void onAssignmentChange(IdealState idealState, ExternalView externalView, Set<String> onlineSegments,
      List<String> pulledSegments, List<ZNRecord> znRecords);

  /// Refreshes the metadata for the given segment (called when segment is getting refreshed).
  void refreshSegment(String segment, @Nullable ZNRecord znRecord);

  /// Called once when this listener's routing entry is removed, so listeners holding per-table
  /// state outside that entry (caches, persisted data) can release it.
  ///
  /// NOTE: the table itself may still exist. Routing is also removed when this broker stops serving
  /// a table it still knows about — an `ONLINE -> OFFLINE` broker-resource transition (untagging,
  /// broker-resource rebalance) or an operator calling the routing-delete endpoint. Treat this as
  /// "stop tracking this table here", not "this table is gone".
  ///
  /// Implementations should not throw; the caller logs and continues so one listener cannot block
  /// routing removal. The default implementation does nothing.
  default void onRoutingRemoved() {
  }
}
