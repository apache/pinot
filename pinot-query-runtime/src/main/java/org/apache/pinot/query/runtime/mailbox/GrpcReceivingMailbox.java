package org.apache.pinot.query.runtime.mailbox;

import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.pinot.common.proto.Mailbox.MailboxContent;
import org.apache.pinot.query.runtime.mailbox.channel.MailboxContentStreamObserver;


public class GrpcReceivingMailbox implements ReceivingMailbox<MailboxContent> {
  private final GrpcMailboxService _mailboxService;
  private final String _mailboxId;
  private final AtomicBoolean _initialized = new AtomicBoolean(false);

  private MailboxContentStreamObserver _contentStreamObserver;

  public GrpcReceivingMailbox(String mailboxId, GrpcMailboxService mailboxService) {
    _mailboxService = mailboxService;
    _mailboxId = mailboxId;
  }

  public void init(MailboxContentStreamObserver streamObserver) {
    if (!_initialized.get()) {
      _contentStreamObserver = streamObserver;
      _initialized.set(true);
    }
  }

  @Override
  public MailboxContent receive() throws Exception {
    if (waitForInitialize()) {
      return _contentStreamObserver.poll();
    }
    return null;
  }

  private boolean waitForInitialize() throws Exception {
    while (true) {
      if (_initialized.get()) {
        return true;
      }
      Thread.sleep(100);
    }
  }

  @Override
  public String getMailboxId() {
    return _mailboxId;
  }
}
