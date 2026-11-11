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
package org.apache.pinot.query.service.dispatch.timeseries;

import com.google.common.base.Preconditions;
import io.grpc.Deadline;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import org.apache.pinot.common.proto.Worker;
import org.apache.pinot.query.runtime.timeseries.PhysicalTimeSeriesBrokerPlanVisitor;
import org.apache.pinot.query.runtime.timeseries.TimeSeriesExecutionContext;
import org.apache.pinot.spi.trace.RequestContext;
import org.apache.pinot.spi.utils.CommonConstants.Query.Request.MetadataKeys;
import org.apache.pinot.tsdb.planner.TimeSeriesExchangeNode;
import org.apache.pinot.tsdb.planner.physical.TimeSeriesDispatchablePlan;
import org.apache.pinot.tsdb.planner.physical.TimeSeriesQueryServerInstance;
import org.apache.pinot.tsdb.spi.TimeBuckets;
import org.apache.pinot.tsdb.spi.operator.BaseTimeSeriesOperator;
import org.apache.pinot.tsdb.spi.plan.BaseTimeSeriesPlanNode;
import org.apache.pinot.tsdb.spi.series.TimeSeriesBlock;


/// Query dispatcher for time-series queries.
public class TimeSeriesQueryDispatcher {
  private final PhysicalTimeSeriesBrokerPlanVisitor _planVisitor = new PhysicalTimeSeriesBrokerPlanVisitor();
  private final Map<String, TimeSeriesDispatchClient> _dispatchClientMap = new ConcurrentHashMap<>();

  public void start() {
    // No-op by default.
  }

  public void shutdown() {
    for (TimeSeriesDispatchClient dispatchClient : _dispatchClientMap.values()) {
      dispatchClient.getChannel().shutdown();
    }
    _dispatchClientMap.clear();
  }

  /// Submits a time-series dispatchable plan to servers and returns the broker-side result block.
  public TimeSeriesBlock submitAndGet(long requestId, TimeSeriesDispatchablePlan plan, long timeoutMs,
      RequestContext requestContext) {
    long deadlineMs = System.currentTimeMillis() + timeoutMs;
    BaseTimeSeriesPlanNode brokerFragment = plan.getBrokerFragment();
    Map<String, BlockingQueue<Object>> receiversByPlanId = new HashMap<>();
    populateConsumers(brokerFragment, receiversByPlanId);
    TimeSeriesExecutionContext brokerExecutionContext =
        new TimeSeriesExecutionContext(plan.getLanguage(), plan.getTimeBuckets(), deadlineMs, Map.of(), Map.of(),
            receiversByPlanId);
    BaseTimeSeriesOperator brokerOperator =
        _planVisitor.compile(brokerFragment, brokerExecutionContext, plan.getNumInputServersForExchangePlanNode());
    for (TimeSeriesQueryServerInstance serverInstance : plan.getQueryServerInstances()) {
      String serverId = serverInstance.getInstanceId();
      Deadline deadline = Deadline.after(deadlineMs - System.currentTimeMillis(), TimeUnit.MILLISECONDS);
      Preconditions.checkState(!deadline.isExpired(), "Deadline expired before query could be sent to servers");
      Worker.TimeSeriesQueryRequest request = Worker.TimeSeriesQueryRequest.newBuilder()
          .addAllDispatchPlan(plan.getSerializedServerFragments())
          .putAllMetadata(initializeTimeSeriesMetadataMap(plan, deadlineMs, requestContext, serverId))
          .putMetadata(MetadataKeys.REQUEST_ID, Long.toString(requestId))
          .build();
      TimeSeriesDispatchObserver dispatchObserver = new TimeSeriesDispatchObserver(receiversByPlanId);
      getOrCreateTimeSeriesDispatchClient(serverInstance).submit(request, deadline, dispatchObserver);
    }
    return brokerOperator.nextBlock();
  }

  private void populateConsumers(BaseTimeSeriesPlanNode planNode, Map<String, BlockingQueue<Object>> receiverMap) {
    if (planNode instanceof TimeSeriesExchangeNode) {
      receiverMap.put(planNode.getId(), new ArrayBlockingQueue<>(TimeSeriesDispatchObserver.MAX_QUEUE_CAPACITY));
    }
    for (BaseTimeSeriesPlanNode childNode : planNode.getInputs()) {
      populateConsumers(childNode, receiverMap);
    }
  }

  private Map<String, String> initializeTimeSeriesMetadataMap(TimeSeriesDispatchablePlan dispatchablePlan,
      long deadlineMs, RequestContext requestContext, String instanceId) {
    Map<String, String> result = new HashMap<>();
    TimeBuckets timeBuckets = dispatchablePlan.getTimeBuckets();
    result.put(MetadataKeys.TimeSeries.LANGUAGE, dispatchablePlan.getLanguage());
    result.put(MetadataKeys.TimeSeries.START_TIME_SECONDS, Long.toString(timeBuckets.getTimeBuckets()[0]));
    result.put(MetadataKeys.TimeSeries.WINDOW_SECONDS, Long.toString(timeBuckets.getBucketSize().getSeconds()));
    result.put(MetadataKeys.TimeSeries.NUM_ELEMENTS, Long.toString(timeBuckets.getTimeBuckets().length));
    result.put(MetadataKeys.TimeSeries.DEADLINE_MS, Long.toString(deadlineMs));
    Map<String, List<String>> leafIdToSegments = dispatchablePlan.getLeafIdToSegmentsByInstanceId().get(instanceId);
    for (Map.Entry<String, List<String>> entry : leafIdToSegments.entrySet()) {
      result.put(MetadataKeys.TimeSeries.encodeSegmentListKey(entry.getKey()), String.join(",", entry.getValue()));
    }
    result.put(MetadataKeys.REQUEST_ID, Long.toString(requestContext.getRequestId()));
    result.put(MetadataKeys.BROKER_ID, requestContext.getBrokerId());
    return result;
  }

  private TimeSeriesDispatchClient getOrCreateTimeSeriesDispatchClient(
      TimeSeriesQueryServerInstance queryServerInstance) {
    String hostname = queryServerInstance.getHostname();
    int port = queryServerInstance.getQueryServicePort();
    String key = hostname + "_" + port;
    return _dispatchClientMap.computeIfAbsent(key, k -> new TimeSeriesDispatchClient(hostname, port));
  }
}
