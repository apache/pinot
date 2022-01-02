package org.apache.pinot.query.runtime.mailbox;

import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.pinot.common.proto.Mailbox.MailboxContent;
import org.apache.pinot.query.runtime.mailbox.channel.MailboxContentStreamObserver;


public class GrpcReceivingMailbox implements ReceivingMailbox<MailboxContent> {
  private final GrpcMailboxService _mailboxService;
  private final String _mailboxId;
  private final AtomicBoolean _isOpened = new AtomicBoolean(false);
  private final AtomicBoolean _isClosed = new AtomicBoolean(false);

  private MailboxContentStreamObserver _contentStreamObserver;

  public GrpcReceivingMailbox(String mailboxId, GrpcMailboxService mailboxService) {
    _mailboxService = mailboxService;
    _mailboxId = mailboxId;
  }

  public void init(MailboxContentStreamObserver streamObserver) {
    if (!_isOpened.get()) {
      _contentStreamObserver = streamObserver;
      _isOpened.set(true);
    }
  }

  @Override
  public MailboxContent receive() throws Exception {
    MailboxContent mailboxContent = null;
    if (waitForInitialize()) {
      mailboxContent = _contentStreamObserver.poll();
    }
    return mailboxContent;
  }

  @Override
  public boolean isClosed() {
    return _isClosed.get();
  }

  @Override
  public void close() {
    _isClosed.set(true);
    _contentStreamObserver.onCompleted();
  }

  // TODO: fix busy wait. This should be guarded by timeout.
  private boolean waitForInitialize() throws Exception {
    while (true) {
      if (_isOpened.get()) {
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
