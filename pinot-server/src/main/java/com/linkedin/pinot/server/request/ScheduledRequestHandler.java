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
package com.linkedin.pinot.server.request;

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.linkedin.pinot.common.exception.QueryException;
import com.linkedin.pinot.common.metrics.*;
import com.linkedin.pinot.common.query.ServerQueryRequest;
import com.linkedin.pinot.common.query.context.TimerContext;
import com.linkedin.pinot.common.request.InstanceRequest;
import com.linkedin.pinot.common.restlet.resources.ServerLatencyMetric;
import com.linkedin.pinot.common.utils.DataTable;
import com.linkedin.pinot.core.common.datatable.DataTableImplV2;
import com.linkedin.pinot.common.metrics.ServerMeter;
import com.linkedin.pinot.common.metrics.ServerMetrics;
import com.linkedin.pinot.common.metrics.ServerQueryPhase;
import com.linkedin.pinot.core.query.scheduler.QueryScheduler;
import com.linkedin.pinot.serde.SerDe;
import com.linkedin.pinot.transport.netty.NettyServer;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import java.net.InetSocketAddress;

import org.apache.thrift.protocol.TCompactProtocol;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ScheduledRequestHandler implements NettyServer.RequestHandler {
  private static final Logger LOGGER = LoggerFactory.getLogger(ScheduledRequestHandler.class);

  private final ServerMetrics serverMetrics;
  private QueryScheduler queryScheduler;

  public ScheduledRequestHandler(QueryScheduler queryScheduler, ServerMetrics serverMetrics) {
    Preconditions.checkNotNull(queryScheduler);
    Preconditions.checkNotNull(serverMetrics);
    this.queryScheduler = queryScheduler;
    this.serverMetrics = serverMetrics;
  }

  @Override
  public ListenableFuture<byte[]> processRequest(ChannelHandlerContext channelHandlerContext, ByteBuf request) {
    final long queryStartTimeNs = System.nanoTime();
    serverMetrics.addMeteredGlobalValue(ServerMeter.QUERIES, 1);

    LOGGER.debug("Processing request : {}", request);

    byte[] byteArray = new byte[request.readableBytes()];
    request.readBytes(byteArray);
    SerDe serDe = new SerDe(new TCompactProtocol.Factory());
    final InstanceRequest instanceRequest = new InstanceRequest();

    if (!serDe.deserialize(instanceRequest, byteArray)) {
      LOGGER.error("Failed to deserialize query request from broker ip: {}",
          ((InetSocketAddress) channelHandlerContext.channel().remoteAddress()).getAddress().getHostAddress());
      serverMetrics.addMeteredGlobalValue(ServerMeter.REQUEST_DESERIALIZATION_EXCEPTIONS, 1);
      return Futures.immediateFuture(null);
    }

    final ServerQueryRequest queryRequest = new ServerQueryRequest(instanceRequest, serverMetrics);
    final TimerContext timerContext = queryRequest.getTimerContext();
    timerContext.setQueryArrivalTimeNs(queryStartTimeNs);
    timerContext.startNewPhaseTimerAtNs(ServerQueryPhase.REQUEST_DESERIALIZATION, queryStartTimeNs).stopAndRecord();

    LOGGER.debug("Processing requestId:{},request={}", instanceRequest.getRequestId(), instanceRequest);
    return queryScheduler.submit(queryRequest);
  }

  @Override
  public ListenableFuture<byte[]> processRequest(ChannelHandlerContext channelHandlerContext,
                                                 ByteBuf request, ServerLatencyMetric metric) {

    final long queryStartTimeNs = System.nanoTime();
    final long queryStartTime = System.currentTimeMillis();
    //final MetricsHelper.TimerContext requestProcessingLatency = MetricsHelper.startTimer();
    serverMetrics.addMeteredGlobalValue(ServerMeter.QUERIES, 1);

    LOGGER.debug("Processing request : {}", request);

    byte[] byteArray = new byte[request.readableBytes()];
    request.readBytes(byteArray);
    SerDe serDe = new SerDe(new TCompactProtocol.Factory());
    final InstanceRequest instanceRequest = new InstanceRequest();

    if (! serDe.deserialize(instanceRequest, byteArray)) {
      LOGGER.error("Failed to deserialize query request from broker ip: {}",
              ((InetSocketAddress) channelHandlerContext.channel().remoteAddress()).getAddress().getHostAddress());
      DataTable result = new DataTableImplV2();
      result.addException(QueryException.INTERNAL_ERROR);
      serverMetrics.addMeteredGlobalValue(ServerMeter.REQUEST_DESERIALIZATION_EXCEPTIONS, 1);
      ServerQueryRequest queryRequest = new ServerQueryRequest(null, serverMetrics);
      queryRequest.getTimerContext().setQueryArrivalTimeNs(queryStartTimeNs);
      return Futures.immediateFuture(QueryScheduler.serializeDataTable(queryRequest, result));
    }
    final ServerQueryRequest queryRequest = new ServerQueryRequest(instanceRequest, serverMetrics);
    final TimerContext timerContext = queryRequest.getTimerContext();
    timerContext.setQueryArrivalTimeNs(queryStartTimeNs);
    TimerContext.Timer deserializationTimer =
            timerContext.startNewPhaseTimerAtNs(ServerQueryPhase.REQUEST_DESERIALIZATION, queryStartTimeNs);
    deserializationTimer.stopAndRecord();

    LOGGER.debug("Processing requestId:{},request={}", instanceRequest.getRequestId(), instanceRequest);
    ListenableFuture<byte[]> queryResponse = queryScheduler.submit(queryRequest);

    String tableName = instanceRequest.getQuery().getQuerySource().getTableName();
    metric.setNumRequests(1);
    metric.setTimestamp(queryStartTime);
    //metric.setLatency(requestProcessingLatency.getLatencyNs());
    metric.setSegmentCount(serverMetrics.getValueOfTableGauge(tableName, ServerGauge.SEGMENT_COUNT));
    metric.setDocuments(serverMetrics.getValueOfTableGauge(tableName, ServerGauge.DOCUMENT_COUNT));
    metric.setSegmentSize(serverMetrics.getValueOfTableGauge(tableName, ServerGauge.SEGMENT_SIZE));
    metric.set_tableName(tableName);

    return queryResponse;
  }

  public void setScheduler(QueryScheduler scheduler) {
    Preconditions.checkNotNull(scheduler);
    LOGGER.info("Setting scheduler to {}", scheduler.name());
    queryScheduler = scheduler;
  }
}
