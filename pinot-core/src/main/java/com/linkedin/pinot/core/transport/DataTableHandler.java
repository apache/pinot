/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
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
package com.linkedin.pinot.core.transport;

import com.linkedin.pinot.common.metrics.BrokerMeter;
import com.linkedin.pinot.common.metrics.BrokerMetrics;
import com.linkedin.pinot.common.utils.DataTable;
import com.linkedin.pinot.core.common.datatable.DataTableFactory;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class DataTableHandler extends SimpleChannelInboundHandler<ByteBuf> {
  private static final Logger LOGGER = LoggerFactory.getLogger(DataTableHandler.class);

  private final QueryRouter _queryRouter;
  private final Server _server;
  private final BrokerMetrics _brokerMetrics;

  public DataTableHandler(QueryRouter queryRouter, Server server, BrokerMetrics brokerMetrics) {
    _queryRouter = queryRouter;
    _server = server;
    _brokerMetrics = brokerMetrics;
  }

  @Override
  public void channelActive(ChannelHandlerContext ctx) {
    LOGGER.info("Channel for server: {} is now active", _server);
  }

  @Override
  public void channelInactive(ChannelHandlerContext ctx) {
    LOGGER.error("Channel for server: {} is now inactive, marking server down", _server);
    _queryRouter.markServerDown(_server);
  }

  @Override
  protected void channelRead0(ChannelHandlerContext ctx, ByteBuf msg) {
    long responseSize = msg.readableBytes();
    _brokerMetrics.addMeteredGlobalValue(BrokerMeter.NETTY_CONNECTION_BYTES_RECEIVED, responseSize);
    try {
      long deserializationStartTimeMs = System.currentTimeMillis();
      DataTable dataTable = DataTableFactory.getDataTable(msg.nioBuffer());
      _queryRouter.receiveDataTable(_server, dataTable, responseSize,
          System.currentTimeMillis() - deserializationStartTimeMs);
    } catch (Exception e) {
      LOGGER.error("Caught exception while deserializing data table of size: {} from server: {}", responseSize, _server,
          e);
      _brokerMetrics.addMeteredGlobalValue(BrokerMeter.DATA_TABLE_DESERIALIZATION_EXCEPTIONS, 1);
    }
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
    LOGGER.error("Caught exception while handling response from server: {}", _server, cause);
    _brokerMetrics.addMeteredGlobalValue(BrokerMeter.RESPONSE_FETCH_EXCEPTIONS, 1);
  }
}
