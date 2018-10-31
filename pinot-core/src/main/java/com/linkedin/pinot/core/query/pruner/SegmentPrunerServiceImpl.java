/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
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

import com.linkedin.pinot.common.query.ServerQueryRequest;
import java.util.HashSet;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.pinot.core.indexsegment.IndexSegment;
import com.linkedin.pinot.core.query.config.SegmentPrunerConfig;


/**
 * An implementation of SegmentPrunerService.
 *
 *
 */
public class SegmentPrunerServiceImpl implements SegmentPrunerService {

  private static final Logger LOGGER = LoggerFactory.getLogger(SegmentPrunerServiceImpl.class);
  private Set<SegmentPruner> _segmentPrunerSet;

  public SegmentPrunerServiceImpl(Set<SegmentPruner> segmentPrunerSet) {
    _segmentPrunerSet = segmentPrunerSet;
  }

  public SegmentPrunerServiceImpl(SegmentPrunerConfig prunerSetConfig) {
    _segmentPrunerSet = new HashSet<SegmentPruner>();
    if (prunerSetConfig != null) {
      for (int i = 0; i < prunerSetConfig.numberOfSegmentPruner(); ++i) {
        LOGGER.info("Adding SegmentPruner : " + prunerSetConfig.getSegmentPrunerName(i));
        _segmentPrunerSet.add(SegmentPrunerProvider.getSegmentPruner(prunerSetConfig.getSegmentPrunerName(i),
            prunerSetConfig.getSegmentPrunerConfig(i)));
      }
    }
  }

  @Override
  public boolean prune(IndexSegment segment, ServerQueryRequest queryRequest) {
    if (_segmentPrunerSet == null || _segmentPrunerSet.size() == 0) {
      return false;
    }
    for (SegmentPruner pruner : _segmentPrunerSet) {
      if (pruner.prune(segment, queryRequest)) {
        LOGGER.debug("pruned segment: {}", segment.getSegmentName());
        return true;
      }
    }
    return false;
  }
}
