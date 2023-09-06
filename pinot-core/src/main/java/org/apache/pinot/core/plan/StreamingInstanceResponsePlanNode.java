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
package org.apache.pinot.core.plan;

import java.util.List;
import org.apache.pinot.core.operator.InstanceResponseOperator;
import org.apache.pinot.core.operator.streaming.StreamingInstanceResponseOperator;
import org.apache.pinot.core.query.executor.ResultsBlockStreamer;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.segment.spi.FetchContext;
import org.apache.pinot.segment.spi.IndexSegment;


public class StreamingInstanceResponsePlanNode extends InstanceResponsePlanNode {
  private final ResultsBlockStreamer _streamer;

  public StreamingInstanceResponsePlanNode(CombinePlanNode combinePlanNode, List<IndexSegment> indexSegments,
      List<FetchContext> fetchContexts, QueryContext queryContext, ResultsBlockStreamer streamer) {
    super(combinePlanNode, indexSegments, fetchContexts, queryContext);
    _streamer = streamer;
  }

  @Override
  public InstanceResponseOperator run() {
    return new StreamingInstanceResponseOperator(_combinePlanNode.run(), _indexSegments, _fetchContexts, _streamer,
        _queryContext);
  }
}
