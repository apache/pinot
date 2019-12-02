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
package org.apache.pinot.server.request;

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.pinot.common.metrics.ServerMeter;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.common.metrics.ServerQueryPhase;
import org.apache.pinot.common.request.InstanceRequest;
import org.apache.pinot.spi.utils.BytesUtils;
import org.apache.pinot.core.query.request.ServerQueryRequest;
import org.apache.pinot.core.query.scheduler.QueryScheduler;
import org.apache.pinot.serde.SerDe;
import org.apache.pinot.transport.netty.NettyServer;
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
  public ListenableFuture<byte[]> processRequest(byte[] request) {
    long queryArrivalTimeMs = System.currentTimeMillis();
    serverMetrics.addMeteredGlobalValue(ServerMeter.QUERIES, 1);

    SerDe serDe = new SerDe(new TCompactProtocol.Factory());
    InstanceRequest instanceRequest = new InstanceRequest();
    if (!serDe.deserialize(instanceRequest, request)) {
      LOGGER.error("Failed to deserialize query request: {}", BytesUtils.toHexString(request));
      serverMetrics.addMeteredGlobalValue(ServerMeter.REQUEST_DESERIALIZATION_EXCEPTIONS, 1);
      return Futures.immediateFuture(null);
    }

    ServerQueryRequest queryRequest = new ServerQueryRequest(instanceRequest, serverMetrics, queryArrivalTimeMs);
    queryRequest.getTimerContext().startNewPhaseTimer(ServerQueryPhase.REQUEST_DESERIALIZATION, queryArrivalTimeMs)
        .stopAndRecord();

    LOGGER.debug("Processing requestId:{},request={}", instanceRequest.getRequestId(), instanceRequest);
    return queryScheduler.submit(queryRequest);
  }

  public void setScheduler(QueryScheduler scheduler) {
    Preconditions.checkNotNull(scheduler);
    LOGGER.info("Setting scheduler to {}", scheduler.name());
    queryScheduler = scheduler;
  }
}
