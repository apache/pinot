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
import java.util.Collections;
import java.util.List;
import org.apache.pinot.core.data.manager.SegmentDataManager;
import org.apache.pinot.core.data.manager.TableDataManager;
import org.apache.pinot.core.indexsegment.IndexSegment;
import org.apache.pinot.core.query.request.ServerQueryRequest;
import org.apache.pinot.spi.env.PinotConfiguration;


public interface SegmentPruner {

  /**
   * Initializes the segment pruner.
   */
  void init(PinotConfiguration config);

  /**
   * Prunes the segments based on the query request, returns the segments that are not pruned. The pruned segments need
   * to be released by calling {@link TableDataManager#releaseSegment(SegmentDataManager)}.
   * <p>Override this method or {@link #prune(IndexSegment, ServerQueryRequest)} for the pruner logic.
   */
  default List<SegmentDataManager> prune(TableDataManager tableDataManager,
      List<SegmentDataManager> segmentDataManagers, ServerQueryRequest queryRequest) {
    if (segmentDataManagers.isEmpty()) {
      return Collections.emptyList();
    }
    List<SegmentDataManager> remainingSegmentDataManagers = new ArrayList<>(segmentDataManagers.size());
    for (SegmentDataManager segmentDataManager : segmentDataManagers) {
      if (prune(segmentDataManager.getSegment(), queryRequest)) {
        tableDataManager.releaseSegment(segmentDataManager);
      } else {
        remainingSegmentDataManagers.add(segmentDataManager);
      }
    }
    return remainingSegmentDataManagers;
  }

  /**
   * Returns {@code true} if the segment can be pruned based on the query request.
   * <p>Override this method or {@link #prune(TableDataManager, List, ServerQueryRequest)} for the pruner logic.
   */
  default boolean prune(IndexSegment segment, ServerQueryRequest queryRequest) {
    throw new UnsupportedOperationException();
  }
}
