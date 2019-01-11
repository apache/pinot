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
import org.apache.pinot.thirdeye.datalayer.dto.EventDTO;


/**
 * Selector for events based on (optionally) start time, end time, and dimension filters.
 */
public class EventSlice {
  final long start;
  final long end;
  final Multimap<String, String> filters;

  public EventSlice() {
    this.start = -1;
    this.end = -1;
    this.filters = ArrayListMultimap.create();
  }

  public EventSlice(long start, long end, Multimap<String, String> filters) {
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

  public EventSlice withStart(long start) {
    return new EventSlice(start, this.end, this.filters);
  }

  public EventSlice withEnd(long end) {
    return new EventSlice(this.start, end, this.filters);
  }

  public EventSlice withFilters(Multimap<String, String> filters) {
    return new EventSlice(this.start, this.end, filters);
  }

  public boolean match(EventDTO event) {
    if (this.start >= 0 && event.getEndTime() <= this.start)
      return false;
    if (this.end >= 0 && event.getStartTime() >= this.end)
      return false;

    for (String dimName : this.filters.keySet()) {
      if (event.getTargetDimensionMap().containsKey(dimName)) {
        boolean anyMatch = false;
        for (String dimValue : event.getTargetDimensionMap().get(dimName)) {
          anyMatch |= this.filters.get(dimName).contains(dimValue);
        }
        if (!anyMatch)
          return false;
      }
    }

    return true;
  }
}
