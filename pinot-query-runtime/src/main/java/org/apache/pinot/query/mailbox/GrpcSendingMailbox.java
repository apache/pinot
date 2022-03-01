package org.apache.pinot.query.mailbox;

import io.grpc.ManagedChannel;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.pinot.common.proto.Mailbox.MailboxContent;
import org.apache.pinot.common.proto.PinotMailboxGrpc;
import org.apache.pinot.query.mailbox.channel.MailboxStatusStreamObserver;


public class GrpcSendingMailbox implements SendingMailbox<MailboxContent> {
  private final GrpcMailboxService _mailboxService;
  private final String _mailboxId;
  private final AtomicBoolean _initialized = new AtomicBoolean(false);
  private final AtomicInteger _totalMsgSent = new AtomicInteger(0);

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
      // initialization is special
      init();
    }
    _statusStreamObserver.send(data);
    _totalMsgSent.incrementAndGet();
  }

  @Override
  public void complete() {
    _statusStreamObserver.complete();
  }

  @Override
  public String getMailboxId() {
    return _mailboxId;
  }
}
