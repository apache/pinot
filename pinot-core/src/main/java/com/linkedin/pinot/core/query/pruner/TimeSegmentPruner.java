package com.linkedin.pinot.core.query.pruner;

import org.apache.commons.configuration.Configuration;
import org.joda.time.Interval;

import com.linkedin.pinot.common.request.BrokerRequest;
import com.linkedin.pinot.core.indexsegment.IndexSegment;


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
  public boolean prune(IndexSegment segment, BrokerRequest brokerRequest) {
    Interval interval = segment.getSegmentMetadata().getTimeInterval();
    if (interval != null && brokerRequest.getTimeInterval() != null && !new Interval(brokerRequest.getTimeInterval()).contains(interval)) {
      return true;
    }
    return false;
  }

  @Override
  public void init(Configuration config) {

  }
}
