/*
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.linkedin.thirdeye.detection.spi.model;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import com.linkedin.thirdeye.datalayer.dto.MergedAnomalyResultDTO;


/**
 * Selector for anomalies based on (optionally) detector id, start time, end time, and
 * dimension filters.
 */
public class AnomalySlice {
  final long start;
  final long end;
  final Multimap<String, String> filters;

  public AnomalySlice() {
    this.start = -1;
    this.end = -1;
    this.filters = ArrayListMultimap.create();
  }

  public AnomalySlice(long start, long end, Multimap<String, String> filters) {
    this.start = start;
    this.end = end;
    this.filters = filters;
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
    return new AnomalySlice(start, this.end, this.filters);
  }

  public AnomalySlice withEnd(long end) {
    return new AnomalySlice(this.start, end, this.filters);
  }

  public AnomalySlice withFilters(Multimap<String, String> filters) {
    return new AnomalySlice(this.start, this.end, filters);
  }

  public boolean match(MergedAnomalyResultDTO anomaly) {
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

    return true;
  }
}
