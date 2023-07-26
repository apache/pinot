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
package org.apache.pinot.core.plan.maker;

import io.grpc.stub.StreamObserver;
import java.util.List;
import java.util.concurrent.ExecutorService;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.common.proto.Server;
import org.apache.pinot.core.plan.Plan;
import org.apache.pinot.core.plan.PlanNode;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.spi.annotations.InterfaceAudience;
import org.apache.pinot.spi.env.PinotConfiguration;


/**
 * The {@code PlanMaker} makes logical execution plan for the queries.
 */
@InterfaceAudience.Private
public interface PlanMaker {

  /**
   * Initializes the plan maker.
   */
  void init(PinotConfiguration queryExecutorConfig);

  /**
   * Returns an instance level {@link Plan} which contains the logical execution plan for multiple segments.
   */
  Plan makeInstancePlan(List<IndexSegment> indexSegments, QueryContext queryContext, ExecutorService executorService,
      ServerMetrics serverMetrics);

  /**
   * Returns a segment level {@link PlanNode} which contains the logical execution plan for one segment.
   */
  PlanNode makeSegmentPlanNode(IndexSegment indexSegment, QueryContext queryContext);

  /**
   * Returns an instance level {@link Plan} for a streaming query which contains the logical execution plan for multiple
   * segments.
   */
  Plan makeStreamingInstancePlan(List<IndexSegment> indexSegments, QueryContext queryContext,
      ExecutorService executorService, StreamObserver<Server.ServerResponse> streamObserver,
      ServerMetrics serverMetrics);

  /**
   * Returns a segment level {@link PlanNode} for a streaming query which contains the logical execution plan for one
   * segment.
   */
  PlanNode makeStreamingSegmentPlanNode(IndexSegment indexSegment, QueryContext queryContext);
}
