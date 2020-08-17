/**
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
package org.apache.pinot.core.query.pruner;

import java.util.ArrayList;
import java.util.List;
import org.apache.pinot.core.data.manager.SegmentDataManager;
import org.apache.pinot.core.data.manager.TableDataManager;
import org.apache.pinot.core.query.config.SegmentPrunerConfig;
import org.apache.pinot.core.query.request.ServerQueryRequest;
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
    int numPruners = config.numSegmentPruners();
    _segmentPruners = new ArrayList<>(numPruners);
    for (int i = 0; i < numPruners; i++) {
      LOGGER.info("Adding segment pruner: " + config.getSegmentPrunerName(i));
      _segmentPruners.add(
          SegmentPrunerProvider.getSegmentPruner(config.getSegmentPrunerName(i), config.getSegmentPrunerConfig(i)));
    }
  }

  /**
   * Prunes the segments based on the query request, returns the segments that are not pruned.
   */
  public List<SegmentDataManager> prune(TableDataManager tableDataManager, List<SegmentDataManager> segmentDataManagers,
      ServerQueryRequest queryRequest) {
    for (SegmentPruner segmentPruner : _segmentPruners) {
      segmentDataManagers = segmentPruner.prune(tableDataManager, segmentDataManagers, queryRequest);
    }
    return segmentDataManagers;
  }
}
