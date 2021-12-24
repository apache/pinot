package org.apache.pinot.query.runtime.mailbox.channel;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.pinot.query.runtime.mailbox.GrpcMailboxService;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.CommonConstants;


/**
 * Manages Grpc send/Receive channels.
 *
 * All changes are keyed by channelId in the form of:
 * <code>senderHost:receiverHost:grpcPort</code>
 */
public class ChannelManager {
  private final GrpcMailboxService _mailboxService;
  private final GrpcMailboxServer _grpcMailboxServer;
  private final PinotConfiguration _pinotConfig;

  private final ConcurrentHashMap<String, ManagedChannel> _channelMap = new ConcurrentHashMap<>();

  public ChannelManager(GrpcMailboxService mailboxService, PinotConfiguration config) {
    _mailboxService = mailboxService;
    _pinotConfig = config;
    _grpcMailboxServer = new GrpcMailboxServer(_mailboxService, toGrpcPort(config));
  }

  public void init() {
    _grpcMailboxServer.start();
  }

  public void shutdown() {
    _grpcMailboxServer.shutdown();
  }

  public ManagedChannel getChannel(String channelId) {
    return getChannel(channelId, toGrpcConfig(_pinotConfig));
  }

  public ManagedChannel getChannel(String channelId, Config config) {
    String[] channelParts = channelId.split(":");
    return _channelMap.computeIfAbsent(channelId, (id) -> ManagedChannelBuilder
        .forAddress(channelParts[0], Integer.parseInt(channelParts[1]))
        .maxInboundMessageSize(config.getMaxInboundMessageSizeBytes())
        .usePlaintext()
        .build());
  }

  public static class Config {
    // Default max message size to 128MB
    private static final int DEFAULT_MAX_INBOUND_MESSAGE_BYTES_SIZE = 128 * 1024 * 1024;
    private final int _maxInboundMessageSizeBytes;
    private final boolean _usePlainText;

    public Config() {
      this(DEFAULT_MAX_INBOUND_MESSAGE_BYTES_SIZE, true);
    }

    public Config(int maxInboundMessageSizeBytes, boolean usePlainText) {
      _maxInboundMessageSizeBytes = maxInboundMessageSizeBytes;
      _usePlainText = usePlainText;
    }

    public int getMaxInboundMessageSizeBytes() {
      return _maxInboundMessageSizeBytes;
    }

    public boolean isUsePlainText() {
      return _usePlainText;
    }
  }

  // TODO: wired config in:
  private static Config toGrpcConfig(PinotConfiguration config) {
    return new Config();
  }

  private static int toGrpcPort(PinotConfiguration config) {
    return config.getProperty(CommonConstants.Server.CONFIG_OF_GRPC_PORT, CommonConstants.Server.DEFAULT_GRPC_PORT);
  }
}
