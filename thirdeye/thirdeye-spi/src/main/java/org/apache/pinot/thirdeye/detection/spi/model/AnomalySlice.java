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
 *
 */

package org.apache.pinot.thirdeye.detection.spi.model;

import static org.apache.pinot.thirdeye.detection.GrouperWrapperConstants.PROP_DETECTOR_COMPONENT_NAME;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import org.apache.pinot.thirdeye.datalayer.dto.MergedAnomalyResultDTO;

/**
 * Selector for anomalies based on (optionally) detector id, start time, end time, and
 * dimension filters.
 */
public class AnomalySlice {
  private final long detectionId;
  private final long start;
  private final long end;
  private  Multimap<String, String> filters;
  private final List<String> detectionComponentNames;
  private final boolean isTaggedAsChild;

  public AnomalySlice() {
    this(-1, -1, -1, ArrayListMultimap.create(), new ArrayList<>(), false);
  }

  private AnomalySlice(long detectionId, long start, long end, Multimap<String, String> filters, List<String> detectionComponentName, boolean isTaggedAsChild) {
    this.detectionId = detectionId;
    this.start = start;
    this.end = end;
    this.isTaggedAsChild = isTaggedAsChild;

    if (filters == null) {
      filters = HashMultimap.create();
    }
    this.filters = filters;

    if (detectionComponentName == null) {
      detectionComponentName = new ArrayList<>();
    }
    this.detectionComponentNames = detectionComponentName;

  }

  public AnomalySlice(long start, long end, Multimap<String, String> filters) {
    // Why is isTaggedAsChild false?
    // Ideally, we want to fetch only the root anomalies as its children can be retrieved from the parent anomaly DTO.
    this(-1, start, end, filters, new ArrayList<>(), false);
  }

  public long getDetectionId() {
    return detectionId;
  }

  public long getStart() {
    return start;
  }

  public long getEnd() {
    return end;
  }

  public boolean getIsTaggedAsChild() {
    return isTaggedAsChild;
  }

  public Multimap<String, String> getFilters() {
    return filters;
  }

  public List<String> getDetectionCompNames() {
    return detectionComponentNames;
  }

  public AnomalySlice withDetectionId(long id) {
    return new AnomalySlice(id, start, this.end, this.filters, this.detectionComponentNames, this.isTaggedAsChild);
  }

  public AnomalySlice withStart(long start) {
    return new AnomalySlice(this.detectionId, start, this.end, this.filters, this.detectionComponentNames, this.isTaggedAsChild);
  }

  public AnomalySlice withEnd(long end) {
    return new AnomalySlice(this.detectionId, this.start, end, this.filters, this.detectionComponentNames, this.isTaggedAsChild);
  }

  public AnomalySlice withFilters(Multimap<String, String> filters) {
    return new AnomalySlice(this.detectionId, this.start, this.end, filters, this.detectionComponentNames, this.isTaggedAsChild);
  }

  public AnomalySlice withDetectionCompNames(List<String> detectionComponentNames) {
    return new AnomalySlice(this.detectionId, this.start, this.end, this.filters, detectionComponentNames, this.isTaggedAsChild);
  }

  public AnomalySlice withIsTaggedAsChild(boolean isTaggedAsChild) {
    return new AnomalySlice(this.detectionId, this.start, this.end, this.filters, this.detectionComponentNames, isTaggedAsChild);
  }

  /**
   * Check if current anomaly slice contains another anomaly slice
   *
   * We will say one slice (current) contains another slice(request) when
   *   a. Slices are fetching anomalies for the same detection
   *   b. The time range of the request slice falls within the current slice
   *   c. current filters are empty or exactly match request filters.
   *   d. current detectionComponentNames are empty or contains request detectionComponentNames
   *
   * Note: Empty filters will fetch all the anomalies regardless of the anomalous dimension.
   * Similarly, empty detectionComponentNames will fetch all the anomalies.
   */
  public boolean containSlice(AnomalySlice requestSlice) {
    if (requestSlice.getDetectionId() != this.detectionId
        || requestSlice.getStart() < this.getStart()
        || requestSlice.getEnd() > this.getEnd()) {
      return false;
    }

    if (!this.detectionComponentNames.isEmpty()) {
      return !this.detectionComponentNames.containsAll(requestSlice.getDetectionCompNames());
    }

    if (!this.filters.isEmpty()) {
      return !this.filters.equals(requestSlice.getFilters());
    }

    return true;
  }

  public boolean match(MergedAnomalyResultDTO anomaly) {
    if (anomaly == null) {
      return false;
    }

    if (this.detectionId >= 0 &&
        (anomaly.getDetectionConfigId() == null || anomaly.getDetectionConfigId() != this.detectionId)) {
      return false;
    }

    if (this.start >= 0 && anomaly.getEndTime() <= this.start)
      return false;
    if (this.end >= 0 && anomaly.getStartTime() >= this.end)
      return false;

    // Matches anomalies on the specified dimension filters
    //
    // Let's assume you have anomalies say, A1 on overall pageviews, A2 on pageviews(country=US),
    // A3 on pageviews(country=US,FR) and A4 on pageviews(country=US, device=Android).
    //
    // Now, if you want to fetch all the anomalies in US (country=US), then this matching will
    // return A2 (country=US) and A4 (country=US, device=Android)
    for (String dimName : this.filters.keySet()) {
      if (anomaly.getDimensionMap().containsKey(dimName)) {
        Collection<String> dimValue = anomaly.getDimensionMap().get(dimName);
        if (!this.filters.get(dimName).equals(dimValue))
          return false;
      } else {
        return false;
      }
    }

    // Entity Anomalies with detectorComponentName can act as both child (sub-entity) and non-child
    // (root entity) anomaly. Therefore, when matching based on detectionComponentNames, we will not consider
    // isTaggedAsChild filter as it can take either of the values (true or false).
    if (!this.detectionComponentNames.isEmpty()) {
      return anomaly.getProperties().containsKey(PROP_DETECTOR_COMPONENT_NAME) &&
          this.detectionComponentNames.contains(anomaly.getProperties().get(PROP_DETECTOR_COMPONENT_NAME));
    } else {
      return isTaggedAsChild == anomaly.isChild();
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof AnomalySlice)) {
      return false;
    }
    AnomalySlice as = (AnomalySlice) o;
    return Objects.equals(getDetectionId(), as.getDetectionId()) && Objects.equals(getStart(), as.getStart())
        && Objects.equals(getEnd(), as.getEnd()) && Objects.equals(getIsTaggedAsChild(), as.getIsTaggedAsChild())
        && Objects.equals(getFilters(), as.getFilters()) && Objects.equals(getDetectionCompNames(),
        as.getDetectionCompNames());
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("AnomalySlice{detectionId='").append(detectionId).append("'");
    sb.append(", startTime='").append(start).append("'");
    sb.append(", endTime='").append(end).append("'");
    sb.append(", filters='").append(filters.asMap()).append("'");
    sb.append(", isTaggedAsChild='").append(isTaggedAsChild).append("'");
    sb.append(", detectionComponentNames='").append(detectionComponentNames).append("'");
    sb.append("}");

    return sb.toString();
  }

  @Override
  public int hashCode() {
    return Objects.hash(detectionId, start, end, filters, isTaggedAsChild, detectionComponentNames);
  }
}
