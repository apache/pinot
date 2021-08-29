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
package org.apache.pinot.broker.routing.instanceselector;

import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState;
import org.apache.pinot.broker.routing.segmentmetadata.SegmentBrokerView;
import org.apache.pinot.common.request.BrokerRequest;


/**
 * The instance selector selects server instances to serve the query based on the selected segments.
 */
public interface InstanceSelector {

  /**
   * Initializes the instance selector with the enabled instances, external view, ideal state and online segments
   * (segments with ONLINE/CONSUMING instances in the ideal state and selected by the pre-selector). Should be called
   * only once before calling other methods.
   */
  void init(Set<String> enabledInstances, ExternalView externalView, IdealState idealState,
      Set<SegmentBrokerView> onlineSegments);

  /**
   * Processes the instances change. Changed instances are pre-computed based on the current and previous enabled
   * instances only once on the caller side and passed to all the instance selectors.
   */
  void onInstancesChange(Set<String> enabledInstances, List<String> changedInstances);

  /**
   * Processes the external view change based on the given ideal state and online segments (segments with
   * ONLINE/CONSUMING instances in the ideal state and selected by the pre-selector).
   */
  void onExternalViewChange(ExternalView externalView, IdealState idealState, Set<SegmentBrokerView> onlineSegments);

  /**
   * Selects the server instances for the given segments queried by the given broker request, returns a map from segment
   * to selected server instance hosting the segment and a set of unavailable segments (no enabled instance or all
   * enabled instances are in ERROR state).
   */
  SelectionResult select(BrokerRequest brokerRequest, List<SegmentBrokerView> segments);

  class SelectionResult {
    private final Map<SegmentBrokerView, String> _segmentToInstanceMap;
    private final List<SegmentBrokerView> _unavailableSegments;

    public SelectionResult(Map<SegmentBrokerView, String> segmentToInstanceMap,
        List<SegmentBrokerView> unavailableSegments) {
      _segmentToInstanceMap = segmentToInstanceMap;
      _unavailableSegments = unavailableSegments;
    }

    /**
     * Returns the map from segment to selected server instance hosting the segment.
     */
    public Map<SegmentBrokerView, String> getSegmentToInstanceMap() {
      return _segmentToInstanceMap;
    }

    /**
     * Returns the unavailable segments (no enabled instance or all enabled instances are in ERROR state).
     */
    public List<SegmentBrokerView> getUnavailableSegments() {
      return _unavailableSegments;
    }
  }
}
