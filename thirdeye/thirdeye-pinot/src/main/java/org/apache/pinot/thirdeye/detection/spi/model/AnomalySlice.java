/*
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

package org.apache.pinot.thirdeye.detection.spi.model;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import java.util.List;
import org.apache.pinot.thirdeye.datalayer.dto.MergedAnomalyResultDTO;

import static org.apache.pinot.thirdeye.detection.wrapper.GrouperWrapper.*;


/**
 * Selector for anomalies based on (optionally) detector id, start time, end time, and
 * dimension filters.
 */
public class AnomalySlice {
  private final long start;
  private final long end;
  private  Multimap<String, String> filters;
  private final List<String> detectionComponentNames;
  private final boolean isTaggedAsChild;

  public AnomalySlice() {
    this(-1, -1, ArrayListMultimap.create(), null, false);
  }

  private AnomalySlice(long start, long end, Multimap<String, String> filters, List<String> detectionComponentName, boolean isTaggedAsChild) {
    this.start = start;
    this.end = end;
    this.filters = filters;
    this.detectionComponentNames = detectionComponentName;
    this.isTaggedAsChild = isTaggedAsChild;
  }

  public AnomalySlice(long start, long end, Multimap<String, String> filters) {
    this(start, end, filters, null, false);
  }

  public long getStart() {
    return start;
  }

  public long getEnd() {
    return end;
  }

  public Multimap<String, String> getFilters() {
    return filters;
  }

  public AnomalySlice withStart(long start) {
    return new AnomalySlice(start, this.end, this.filters, this.detectionComponentNames, this.isTaggedAsChild);
  }

  public AnomalySlice withEnd(long end) {
    return new AnomalySlice(this.start, end, this.filters, this.detectionComponentNames, this.isTaggedAsChild);
  }

  public AnomalySlice withFilters(Multimap<String, String> filters) {
    return new AnomalySlice(this.start, this.end, filters, this.detectionComponentNames, this.isTaggedAsChild);
  }

  public AnomalySlice withDetectionCompNames(List<String> detectionComponentNames) {
    return new AnomalySlice(this.start, this.end, this.filters, detectionComponentNames, this.isTaggedAsChild);
  }

  public AnomalySlice withIsTaggedAsChild(boolean isTaggedAsChild) {
    return new AnomalySlice(this.start, this.end, this.filters, this.detectionComponentNames, isTaggedAsChild);
  }

  public boolean match(MergedAnomalyResultDTO anomaly) {
    if (anomaly == null) {
      return false;
    }

    if (this.start >= 0 && anomaly.getEndTime() <= this.start)
      return false;
    if (this.end >= 0 && anomaly.getStartTime() >= this.end)
      return false;

    for (String dimName : this.filters.keySet()) {
      if (anomaly.getDimensions().containsKey(dimName)) {
        String dimValue = anomaly.getDimensions().get(dimName);
        if (!this.filters.get(dimName).contains(dimValue))
          return false;
      }
    }

    // Note:
    // Entity Anomalies with detectorComponentName can act as both child (sub-entity) and non-child
    // (root entity) anomaly. Therefore, when matching based on detectionComponentNames, do not consider
    // isTaggedAsChild filter as it can take either of the values (true or false).
    if (this.detectionComponentNames != null && !this.detectionComponentNames.isEmpty()) {
      return anomaly.getProperties().containsKey(PROP_DETECTOR_COMPONENT_NAME) && this.detectionComponentNames.contains(
          anomaly.getProperties().get(PROP_DETECTOR_COMPONENT_NAME));
    } else {
      return isTaggedAsChild == anomaly.isChild();
    }
  }
}
