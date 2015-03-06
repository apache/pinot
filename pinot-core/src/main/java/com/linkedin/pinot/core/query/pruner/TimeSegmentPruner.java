/**
 * Copyright (C) 2014-2015 LinkedIn Corp. (pinot-core@linkedin.com)
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
