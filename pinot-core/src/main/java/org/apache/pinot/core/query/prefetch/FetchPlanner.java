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
package org.apache.pinot.core.query.prefetch;

import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.segment.spi.FetchContext;
import org.apache.pinot.segment.spi.IndexSegment;


/**
 * FetchPlanner decides what data to prefetch for the given query to reduce the time waiting for data, which might be
 * read from some slow storage backend, to reduce the query latency. Although the plan methods take in an indexSegment
 * object, it should mainly access the segment metadata like what types of index it has, as the index data may not be
 * available, but leaving this to the implementations to check and handle per their own need.
 * TODO: this interface and the dependent classes like QueryContext should be moved to query-spi pkg, yet to be added.
 */
public interface FetchPlanner {
  /**
   * Plan what index data to prefetch to help prune the segment before processing it. For example, one can fetch bloom
   * filter to prune the segment based on the column values in query predicates like IN or EQ.
   *
   * @param indexSegment     segment to be pruned.
   * @param queryContext     context extracted from the query.
   * @return context to guide data prefetching.
   */
  FetchContext planFetchForPruning(IndexSegment indexSegment, QueryContext queryContext);

  /**
   * Plan what index data to prefetch to process it. For example, one can fetch all types of index for columns tracked
   * in QueryContext.
   *
   * @param indexSegment segment to be processed.
   * @param queryContext context extracted from the query.
   * @return context to guide data prefetching.
   */
  FetchContext planFetchForProcessing(IndexSegment indexSegment, QueryContext queryContext);
}
