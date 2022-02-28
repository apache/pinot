package org.apache.pinot.query.mailbox;

import io.grpc.ManagedChannel;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.pinot.common.proto.Mailbox.MailboxContent;
import org.apache.pinot.query.mailbox.channel.ChannelManager;
import org.apache.pinot.spi.env.PinotConfiguration;


/**
 * GRPC-based implementation of {@link MailboxService}.
 *
 * It maintains a collection of connected mailbox servers and clients to remote hosts.
 * All indexed by the connectionID (e.g. a pair of host-port combinations).
 *
 * Connections are established from the client side and only tier-down from the client side,
 * or timed out based on a mutually agreed upon timeout period after last message.
 *
 * connections are reused for different jobs (and mailboxes within each job).
 */
public class GrpcMailboxService implements MailboxService<MailboxContent> {
  // channel manager
  private final ChannelManager _channelManager;
  private final String _hostname;
  private final int _mailboxPort;

  // maintaining a list of registered mailboxes.
  private final ConcurrentHashMap<String, ReceivingMailbox<MailboxContent>> _receivingMailboxMap
      = new ConcurrentHashMap<>();
  private final ConcurrentHashMap<String, SendingMailbox<MailboxContent>> _sendingMailboxMap
      = new ConcurrentHashMap<>();


  public GrpcMailboxService(String hostname, int mailboxPort) {
    _hostname = hostname;
    _mailboxPort = mailboxPort;
    _channelManager = new ChannelManager(this);
  }

  @Override
  public void start() {
    _channelManager.init();
  }

  @Override
  public void shutdown() {
    _channelManager.shutdown();
  }

  @Override
  public String getHostname() {
    return _hostname;
  }

  @Override
  public int getMailboxPort() {
    return _mailboxPort;
  }

  /**
   * Register a mailbox, mailbox needs to be registered before use.
   * @param mailboxId the id of the mailbox.
   */
  public SendingMailbox<MailboxContent> getSendingMailbox(String mailboxId) {
    return _sendingMailboxMap.computeIfAbsent(mailboxId, (mId) -> new GrpcSendingMailbox(mId, this));
  }

  /**
   * Register a mailbox, mailbox needs to be registered before use.
   * @param mailboxId the id of the mailbox.
   */
  public ReceivingMailbox<MailboxContent> getReceivingMailbox(String mailboxId) {
    return _receivingMailboxMap.computeIfAbsent(mailboxId, (mId) -> new GrpcReceivingMailbox(mId, this));
  }

  public ManagedChannel getChannel(String mailboxId) {
    return _channelManager.getChannel(Utils.constructChannelId(mailboxId));
  }
}
