package com.linkedin.pinot.query.pruner;

import org.apache.commons.configuration.Configuration;
import org.joda.time.Interval;

import com.linkedin.pinot.index.segment.IndexSegment;
import com.linkedin.pinot.query.request.Query;


/**
 * An implementation of SegmentPruner.
 * Pruner will prune segment if there is no overlapping of segment time interval and query 
 * time interval.
 * 
 * @author xiafu
 *
 */
public class TimeSegmentPruner implements SegmentPruner {

  @Override
  public boolean prune(IndexSegment segment, Query query) {
    Interval interval = segment.getSegmentMetadata().getTimeInterval();
    if (query.getTimeInterval() != null && !query.getTimeInterval().contains(interval)) {
      return true;
    }
    return false;
  }

  @Override
  public void init(Configuration config) {

  }
}
