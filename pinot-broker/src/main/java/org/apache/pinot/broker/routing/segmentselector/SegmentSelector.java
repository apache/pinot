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
package org.apache.pinot.broker.routing.segmentselector;

import java.util.Set;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState;
import org.apache.pinot.broker.routing.segmentpreselector.SegmentPreSelector;
import org.apache.pinot.common.request.BrokerRequest;


/**
 * The segment selector selects the segments for the query. The segments selected should cover the whole dataset (table)
 * without overlap.
 */
public interface SegmentSelector {

  /**
   * Initializes the segment selector with the ideal state, external view and online segments (segments with
   * ONLINE/CONSUMING instances in the ideal state and pre-selected by the {@link SegmentPreSelector}). Should be called
   * only once before calling other methods.
   */
  void init(IdealState idealState, ExternalView externalView, Set<String> onlineSegments);

  /**
   * Processes the segment assignment (ideal state or external view) change based on the given online segments (segments
   * with ONLINE/CONSUMING instances in the ideal state and pre-selected by the {@link SegmentPreSelector}).
   */
  void onAssignmentChange(IdealState idealState, ExternalView externalView, Set<String> onlineSegments);

  /**
   * Selects the segments queried by the given broker request. The segments selected should cover the whole dataset
   * (table) without overlap.
   */
  Set<String> select(BrokerRequest brokerRequest);
}
