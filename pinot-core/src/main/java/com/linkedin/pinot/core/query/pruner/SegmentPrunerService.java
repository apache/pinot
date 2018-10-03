/**
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
package com.linkedin.pinot.core.query.pruner;

import com.linkedin.pinot.core.indexsegment.IndexSegment;
import com.linkedin.pinot.core.query.config.SegmentPrunerConfig;
import com.linkedin.pinot.core.query.request.ServerQueryRequest;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * The <code>SegmentPrunerService</code> class contains multiple segment pruners and provides service to prune segments
 * against all pruners.
 */
public class SegmentPrunerService {
  private static final Logger LOGGER = LoggerFactory.getLogger(SegmentPrunerService.class);

  private final List<SegmentPruner> _segmentPruners;

  public SegmentPrunerService(SegmentPrunerConfig config) {
    int numPruners = config.numberOfSegmentPruner();
    _segmentPruners = new ArrayList<>(numPruners);
    for (int i = 0; i < numPruners; i++) {
      LOGGER.info("Adding segment pruner: " + config.getSegmentPrunerName(i));
      _segmentPruners.add(
          SegmentPrunerProvider.getSegmentPruner(config.getSegmentPrunerName(i), config.getSegmentPrunerConfig(i)));
    }
  }

  /**
   * Returns <code>true</code> if the segment can be pruned based on the query request.
   */
  public boolean prune(IndexSegment segment, ServerQueryRequest queryRequest) {
    for (SegmentPruner segmentPruner : _segmentPruners) {
      if (segmentPruner.prune(segment, queryRequest)) {
        LOGGER.debug("Pruned segment: {}", segment.getSegmentName());
        return true;
      }
    }
    return false;
  }
}
