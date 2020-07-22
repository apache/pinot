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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import org.apache.helix.model.ExternalView;
import org.apache.pinot.common.request.BrokerRequest;


/**
 * Segment selector for offline table.
 */
public class OfflineSegmentSelector implements SegmentSelector {
  private final SegmentLineageBasedSegmentSelector _segmentLineageBasedSegmentSelector;

  private volatile List<String> _segments;

  public OfflineSegmentSelector(SegmentLineageBasedSegmentSelector segmentLineageBasedSegmentSelector) {
    _segmentLineageBasedSegmentSelector = segmentLineageBasedSegmentSelector;
  }

  @Override
  public void init(ExternalView externalView, Set<String> onlineSegments) {
    _segmentLineageBasedSegmentSelector.init();
    onExternalViewChange(externalView, onlineSegments);
  }

  @Override
  public void onExternalViewChange(ExternalView externalView, Set<String> onlineSegments) {
    // TODO: for new added segments, before all replicas are up, consider not selecting them to avoid causing
    //       hotspot servers


    // Update segment lineage based segment selector
    _segmentLineageBasedSegmentSelector.onExternalViewChange();

    // Compute the intersection of segments to process from both offline and segment lineage based segment selectors.
    List<String> segmentsToProcess =
        new ArrayList<>(_segmentLineageBasedSegmentSelector.computeSegmentsToProcess(onlineSegments));
    _segments = Collections.unmodifiableList(segmentsToProcess);
  }

  @Override
  public List<String> select(BrokerRequest brokerRequest) {
    return _segments;
  }
}
