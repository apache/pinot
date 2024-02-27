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

import io.netty.channel.ChannelHandler;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.LengthFieldPrepender;
import io.netty.handler.ssl.SslContext;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.pinot.common.config.TlsConfig;
import org.apache.pinot.common.metrics.BrokerMetrics;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.common.utils.tls.TlsUtils;
import org.apache.pinot.core.query.scheduler.QueryScheduler;
import org.apache.pinot.server.access.AccessControl;
import org.apache.pinot.spi.env.PinotConfiguration;


/**
 * The {@code ChannelHandlerFactory} provides all kinds of Netty ChannelHandlers
 */
public class ChannelHandlerFactory {
  public static final String SSL = "ssl";
  // The key is the hashCode of the TlsConfig, the value is the SslContext
  // We don't use TlsConfig as the map key because the TlsConfig is mutable, which means the hashCode can change. If the
  // hashCode changes and the map is resized, the SslContext of the old hashCode will be lost.
  private static final Map<Integer, SslContext> CLIENT_SSL_CONTEXTS_CACHE = new ConcurrentHashMap<>();
  // the key is the hashCode of the TlsConfig, the value is the SslContext
  // We don't use TlsConfig as the map key because the TlsConfig is mutable, which means the hashCode can change. If the
  // hashCode changes and the map is resized, the SslContext of the old hashCode will be lost.
  private static final Map<Integer, SslContext> SERVER_SSL_CONTEXTS_CACHE = new ConcurrentHashMap<>();

  private ChannelHandlerFactory() {
  }

  /**
   * The {@code getLengthFieldBasedFrameDecoder} return a decoder ChannelHandler that splits the received ByteBuffers
   * dynamically by the value of the length field in the message
   */
  public static ChannelHandler getLengthFieldBasedFrameDecoder() {
    return new LengthFieldBasedFrameDecoder(Integer.MAX_VALUE, 0, Integer.BYTES, 0, Integer.BYTES);
  }

  /**
   * The {@code getLengthFieldPrepender} return an encoder ChannelHandler that prepends the length of the message.
   */
  public static ChannelHandler getLengthFieldPrepender() {
    return new LengthFieldPrepender(Integer.BYTES);
  }

  /**
   * The {@code getClientTlsHandler} return a Client side Tls handler that encrypt and decrypt everything.
   */
  public static ChannelHandler getClientTlsHandler(TlsConfig tlsConfig, SocketChannel ch) {
    SslContext sslContext = CLIENT_SSL_CONTEXTS_CACHE
        .computeIfAbsent(tlsConfig.hashCode(), tlsConfigHashCode -> TlsUtils.buildClientContext(tlsConfig));
    return sslContext.newHandler(ch.alloc());
  }

  /**
   * The {@code getServerTlsHandler} return a Server side Tls handler that encrypt and decrypt everything.
   */
  public static ChannelHandler getServerTlsHandler(TlsConfig tlsConfig, SocketChannel ch) {
    SslContext sslContext = SERVER_SSL_CONTEXTS_CACHE.computeIfAbsent(
        tlsConfig.hashCode(), tlsConfigHashCode -> TlsUtils.buildServerContext(tlsConfig));
    return sslContext.newHandler(ch.alloc());
  }

  /**
   * The {@code getDataTableHandler} return a {@code DataTableHandler} Netty inbound handler on Pinot Broker side to
   * handle the serialized data table responses sent from Pinot Server.
   */
  public static ChannelHandler getDataTableHandler(QueryRouter queryRouter, ServerRoutingInstance serverRoutingInstance,
      BrokerMetrics brokerMetrics) {
    return new DataTableHandler(queryRouter, serverRoutingInstance, brokerMetrics);
  }

  /**
   * The {@code getInstanceRequestHandler} return a {@code InstanceRequestHandler} Netty inbound handler on Pinot
   * Server side to handle the serialized instance requests sent from Pinot Broker.
   */
  public static ChannelHandler getInstanceRequestHandler(String instanceName, PinotConfiguration config,
      QueryScheduler queryScheduler, ServerMetrics serverMetrics, AccessControl accessControl) {
    return new InstanceRequestHandler(instanceName, config, queryScheduler, serverMetrics, accessControl);
  }

  public static ChannelHandler getDirectOOMHandler(QueryRouter queryRouter, ServerRoutingInstance serverRoutingInstance,
      ConcurrentHashMap<ServerRoutingInstance, ServerChannels.ServerChannel> serverToChannelMap) {
    return new DirectOOMHandler(queryRouter, serverRoutingInstance, serverToChannelMap);
  }
}
