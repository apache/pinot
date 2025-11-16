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
package org.apache.pinot.core.transport;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import java.util.Map;
import org.apache.pinot.common.datatable.DataTable;
import org.apache.pinot.common.datatable.DataTableFactory;
import org.apache.pinot.common.metrics.BrokerMeter;
import org.apache.pinot.common.metrics.BrokerMetrics;
import org.apache.pinot.spi.accounting.ThreadAccountant;
import org.apache.pinot.spi.accounting.ThreadResourceSnapshot;
import org.apache.pinot.spi.accounting.TrackingScope;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * The {@code DataTableHandler} is the Netty inbound handler on Pinot Broker side to handle the serialized data table
 * responses sent from Pinot Server.
 */
public class DataTableHandler extends SimpleChannelInboundHandler<ByteBuf> {
  private static final Logger LOGGER = LoggerFactory.getLogger(DataTableHandler.class);

  private final QueryRouter _queryRouter;
  private final ThreadAccountant _threadAccountant;
  private final ServerRoutingInstance _serverRoutingInstance;
  private final BrokerMetrics _brokerMetrics = BrokerMetrics.get();

  public DataTableHandler(QueryRouter queryRouter, ThreadAccountant threadAccountant,
      ServerRoutingInstance serverRoutingInstance) {
    _queryRouter = queryRouter;
    _threadAccountant = threadAccountant;
    _serverRoutingInstance = serverRoutingInstance;
  }

  @Override
  public void channelActive(ChannelHandlerContext ctx) {
    LOGGER.info("Channel for server: {} is now active", _serverRoutingInstance);
  }

  @Override
  public void channelInactive(ChannelHandlerContext ctx) {
    LOGGER.error("Channel for server: {} is now inactive, marking server down", _serverRoutingInstance);
    _queryRouter.markServerDown(_serverRoutingInstance,
        new RuntimeException(String.format("Channel for server: %s is inactive", _serverRoutingInstance)));
  }

  @Override
  protected void channelRead0(ChannelHandlerContext ctx, ByteBuf msg) {
    int responseSize = msg.readableBytes();
    _brokerMetrics.addMeteredGlobalValue(BrokerMeter.NETTY_CONNECTION_BYTES_RECEIVED, responseSize);
    try {
      long deserializationStartTimeMs = System.currentTimeMillis();
      ThreadResourceSnapshot resourceSnapshot = new ThreadResourceSnapshot();
      DataTable dataTable = DataTableFactory.getDataTable(msg.nioBuffer());
      _queryRouter.receiveDataTable(_serverRoutingInstance, dataTable, responseSize,
          (int) (System.currentTimeMillis() - deserializationStartTimeMs));
      long cpuTimeNs = resourceSnapshot.getCpuTimeNs();
      long allocatedBytes = resourceSnapshot.getAllocatedBytes();
      // QUERY scope - keyed by query id
      Map<String, String> metadata = dataTable.getMetadata();
      String queryId = metadata.get(DataTable.MetadataKey.QUERY_ID.getName());
      // NOTE: Fallback to REQUEST_ID for backwards compatibility. When query id is explicitly provided, and is
      //       different from request id, this resource usage won't be tracked.
      if (queryId == null) {
        queryId = metadata.get(DataTable.MetadataKey.REQUEST_ID.getName());
      }
      _threadAccountant.updateUntrackedResourceUsage(queryId, cpuTimeNs, allocatedBytes, TrackingScope.QUERY);
      // WORKLOAD scope - keyed by workload name
      String workloadName = metadata.get(DataTable.MetadataKey.WORKLOAD_NAME.getName());
      _threadAccountant.updateUntrackedResourceUsage(workloadName, cpuTimeNs, allocatedBytes, TrackingScope.WORKLOAD);
    } catch (Exception e) {
      LOGGER.error("Caught exception while deserializing data table of size: {} from server: {}", responseSize,
          _serverRoutingInstance, e);
      _brokerMetrics.addMeteredGlobalValue(BrokerMeter.DATA_TABLE_DESERIALIZATION_EXCEPTIONS, 1);
    }
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
    LOGGER.error("Caught exception while handling response from server: {}", _serverRoutingInstance, cause);
    _brokerMetrics.addMeteredGlobalValue(BrokerMeter.RESPONSE_FETCH_EXCEPTIONS, 1);
  }
}
