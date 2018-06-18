package com.linkedin.thirdeye.detection;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import com.linkedin.thirdeye.datalayer.dto.EventDTO;


/**
 * Selector for events based on (optionally) start time, end time, and dimension filters.
 */
class EventSlice {
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
