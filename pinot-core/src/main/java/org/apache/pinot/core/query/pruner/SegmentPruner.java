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

import java.util.List;
import java.util.concurrent.ExecutorService;
import javax.annotation.Nullable;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.spi.env.PinotConfiguration;


public interface SegmentPruner {

  /**
   * Initializes the segment pruner.
   */
  void init(PinotConfiguration config);

  /**
   * Inspect the query context to determine if the pruner should be applied
   * @return true if the pruner applies to the query
   */
  boolean isApplicableTo(QueryContext query);

  /**
   * Prunes the segments based on the query, returns the segments that are not pruned.
   * <p>Override this method for the pruner logic.
   *
   * @param segments The list of segments to be pruned. Implementations must not modify the list.
   *                 TODO: Revisit this because the caller doesn't require not changing the input segments
   */
  List<IndexSegment> prune(List<IndexSegment> segments, QueryContext query);

  default List<IndexSegment> prune(List<IndexSegment> segments, QueryContext query,
      @Nullable ExecutorService executorService) {
    return prune(segments, query);
  }
}
