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
import org.apache.pinot.core.indexsegment.IndexSegment;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.spi.env.PinotConfiguration;


public interface SegmentPruner {

  /**
   * Initializes the segment pruner.
   */
  void init(PinotConfiguration config);

  /**
   * Prunes the segments based on the query, returns the segments that are not pruned.
   * <p>Override this method or {@link #prune(IndexSegment, QueryContext)} for the pruner logic.
   */
  default List<IndexSegment> prune(List<IndexSegment> segments, QueryContext query) {
    if (segments.isEmpty()) {
      return segments;
    }
    List<IndexSegment> selectedSegments = new ArrayList<>(segments.size());
    for (IndexSegment segment : segments) {
      if (!prune(segment, query)) {
        selectedSegments.add(segment);
      }
    }
    return selectedSegments;
  }

  /**
   * Returns {@code true} if the segment can be pruned based on the query.
   * <p>Override this method or {@link #prune(List, QueryContext)} for the pruner logic.
   */
  default boolean prune(IndexSegment segment, QueryContext query) {
    throw new UnsupportedOperationException();
  }
}
