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
package org.apache.pinot.query.mailbox.channel;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.pinot.query.mailbox.GrpcMailboxService;
import org.apache.pinot.query.service.QueryConfig;
import org.apache.pinot.spi.env.PinotConfiguration;


/**
 * {@code ChannelManager} manages Grpc send/receive channels.
 *
 * <p>Grpc channels are managed centralized per Pinot component. Channels should be reused across different
 * query/job/stages.
 *
 * <p>the channelId should be in the format of: <code>"senderHost:senderPort:receiverHost:receiverPort"</code>
 */
public class ChannelManager {

  private final GrpcMailboxService _mailboxService;
  private final GrpcMailboxServer _grpcMailboxServer;

  private final ConcurrentHashMap<String, ManagedChannel> _channelMap = new ConcurrentHashMap<>();

  public ChannelManager(GrpcMailboxService mailboxService, PinotConfiguration extraConfig) {
    _mailboxService = mailboxService;
    _grpcMailboxServer = new GrpcMailboxServer(_mailboxService, _mailboxService.getMailboxPort(), extraConfig);
  }

  public void init() {
    _grpcMailboxServer.start();
  }

  public void shutdown() {
    _grpcMailboxServer.shutdown();
  }

  public ManagedChannel getChannel(String channelId) {
    return _channelMap.computeIfAbsent(channelId,
        (id) -> constructChannel(id.split(":")));
  }

  private static ManagedChannel constructChannel(String[] channelParts) {
    ManagedChannelBuilder<?> managedChannelBuilder = ManagedChannelBuilder
        .forAddress(channelParts[0], Integer.parseInt(channelParts[1]))
        .maxInboundMessageSize(QueryConfig.DEFAULT_MAX_INBOUND_QUERY_DATA_BLOCK_BYTES_SIZE)
        .usePlaintext();
    return managedChannelBuilder.build();
  }
}
