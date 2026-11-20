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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.pinot.core.routing.SegmentsToQuery;


/// Converts an [InstanceSelector.SelectionResult] into maps from segment to the instance it is selected on, to assert
/// on the selection of each segment. Fails when a segment is selected more than once, as a required or an optional
/// segment.
public class SelectionResultTestUtils {
  private SelectionResultTestUtils() {
  }

  /// Returns the map from each selected segment to its instance, optional segments excluded.
  public static Map<String, String> getSegmentToInstanceMap(InstanceSelector.SelectionResult selectionResult) {
    return getSegmentToInstanceMap(selectionResult, false);
  }

  /// Returns the map from each selected optional segment to its instance.
  public static Map<String, String> getOptionalSegmentToInstanceMap(InstanceSelector.SelectionResult selectionResult) {
    return getSegmentToInstanceMap(selectionResult, true);
  }

  private static Map<String, String> getSegmentToInstanceMap(InstanceSelector.SelectionResult selectionResult,
      boolean optional) {
    Map<String, String> selectedSegmentToInstanceMap = new HashMap<>();
    Map<String, String> segmentToInstanceMap = new HashMap<>();
    for (Map.Entry<String, SegmentsToQuery> entry : selectionResult.getInstanceToSegmentsMap().entrySet()) {
      String instance = entry.getKey();
      SegmentsToQuery segmentsToQuery = entry.getValue();
      putSegments(selectedSegmentToInstanceMap, segmentsToQuery.getSegments(), instance);
      putSegments(selectedSegmentToInstanceMap, segmentsToQuery.getOptionalSegments(), instance);
      for (String segment : optional ? segmentsToQuery.getOptionalSegments() : segmentsToQuery.getSegments()) {
        segmentToInstanceMap.put(segment, instance);
      }
    }
    return segmentToInstanceMap;
  }

  private static void putSegments(Map<String, String> segmentToInstanceMap, List<String> segments, String instance) {
    for (String segment : segments) {
      String previousInstance = segmentToInstanceMap.put(segment, instance);
      if (previousInstance != null) {
        throw new IllegalStateException(
            "Segment: " + segment + " is selected on both: " + previousInstance + " and: " + instance);
      }
    }
  }
}
