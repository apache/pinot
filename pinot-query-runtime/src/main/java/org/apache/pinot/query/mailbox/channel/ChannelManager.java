package org.apache.pinot.query.mailbox.channel;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.pinot.query.mailbox.GrpcMailboxService;


/**
 * Manages Grpc send/Receive channels.
 *
 * All changes are keyed by channelId in the form of:
 * <code>senderHost:grpcPort:receiverHost:grpcPort</code>
 */
public class ChannelManager {
  private static final int DEFAULT_MAX_INBOUND_MESSAGE_BYTES_SIZE = 128 * 1024 * 1024;

  private final GrpcMailboxService _mailboxService;
  private final GrpcMailboxServer _grpcMailboxServer;

  private final ConcurrentHashMap<String, ManagedChannel> _channelMap = new ConcurrentHashMap<>();

  public ChannelManager(GrpcMailboxService mailboxService) {
    _mailboxService = mailboxService;
    _grpcMailboxServer = new GrpcMailboxServer(_mailboxService, _mailboxService.getMailboxPort());
  }

  public void init() {
    _grpcMailboxServer.start();
  }

  public void shutdown() {
    _grpcMailboxServer.shutdown();
  }

  public ManagedChannel getChannel(String channelId) {
    String[] channelParts = channelId.split(":");
    return _channelMap.computeIfAbsent(channelId, (id) -> ManagedChannelBuilder
        .forAddress(channelParts[0], Integer.parseInt(channelParts[1]))
        .maxInboundMessageSize(DEFAULT_MAX_INBOUND_MESSAGE_BYTES_SIZE)
        .usePlaintext()
        .build());
  }
}
