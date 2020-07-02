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
package org.apache.pinot.broker.routing.segmentpruner;

import java.util.List;
import java.util.Set;
import org.apache.helix.model.ExternalView;
import org.apache.pinot.common.request.BrokerRequest;


/**
 * The segment pruner prunes the selected segments based on the query.
 */
public interface SegmentPruner {

  /**
   * Initializes the segment pruner with the external view and online segments (segments with ONLINE/CONSUMING instances
   * in ideal state). Should be called only once before calling other methods.
   * <p>NOTE: {@code externalView} is unused, but intentionally passed in as argument in case it is needed in the
   * future.
   */
  void init(ExternalView externalView, Set<String> onlineSegments);

  /**
   * Processes the external view change based on the given online segments (segments with ONLINE/CONSUMING instances in
   * ideal state).
   * <p>NOTE: {@code externalView} is unused, but intentionally passed in as argument in case it is needed in the
   * future.
   */
  void onExternalViewChange(ExternalView externalView, Set<String> onlineSegments);

  /**
   * Refreshes the metadata for the given segment (called when segment is getting refreshed).
   */
  void refreshSegment(String segment);

  /**
   * Prunes the segments queried by the given broker request, returns the selected segments to be queried.
   */
  List<String> prune(BrokerRequest brokerRequest, List<String> segments);
}
