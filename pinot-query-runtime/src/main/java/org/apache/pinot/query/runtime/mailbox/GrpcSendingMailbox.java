package org.apache.pinot.query.runtime.mailbox;

import io.grpc.ManagedChannel;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.pinot.common.proto.Mailbox.MailboxContent;
import org.apache.pinot.common.proto.PinotMailboxGrpc;
import org.apache.pinot.query.runtime.mailbox.channel.MailboxStatusStreamObserver;


public class GrpcSendingMailbox implements SendingMailbox<MailboxContent> {
  private final GrpcMailboxService _mailboxService;
  private final String _mailboxId;
  private final AtomicBoolean _initialized = new AtomicBoolean(false);

  private MailboxStatusStreamObserver _statusStreamObserver;

  public GrpcSendingMailbox(String mailboxId, GrpcMailboxService mailboxService) {
    _mailboxService = mailboxService;
    _mailboxId = mailboxId;
    _initialized.set(false);
  }

  public void init() throws UnsupportedOperationException {
    ManagedChannel channel = _mailboxService.getChannel(_mailboxId);
    PinotMailboxGrpc.PinotMailboxStub stub = PinotMailboxGrpc.newStub(channel);
    _statusStreamObserver = new MailboxStatusStreamObserver();
    _statusStreamObserver.init(stub.open(_statusStreamObserver));
    _initialized.set(true);
  }

  @Override
  public void send(MailboxContent data)
      throws UnsupportedOperationException {
    if (!_initialized.get()) {
      init();
    }
    _statusStreamObserver.offer(data);
  }

  @Override
  public String getMailboxId() {
    return _mailboxId;
  }
}
