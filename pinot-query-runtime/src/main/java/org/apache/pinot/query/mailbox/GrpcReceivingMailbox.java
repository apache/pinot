package org.apache.pinot.query.mailbox;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.pinot.common.proto.Mailbox.MailboxContent;
import org.apache.pinot.query.mailbox.channel.MailboxContentStreamObserver;


public class GrpcReceivingMailbox implements ReceivingMailbox<MailboxContent> {
  private static final long DEFAULT_MAILBOX_INIT_TIMEOUT = 100L;
  private final GrpcMailboxService _mailboxService;
  private final String _mailboxId;
  private final CountDownLatch _initializationLatch;
  private final AtomicInteger _totalMsgReceived = new AtomicInteger(0);

  private MailboxContentStreamObserver _contentStreamObserver;

  public GrpcReceivingMailbox(String mailboxId, GrpcMailboxService mailboxService) {
    _mailboxService = mailboxService;
    _mailboxId = mailboxId;
    _initializationLatch = new CountDownLatch(1);
  }

  public void init(MailboxContentStreamObserver streamObserver) {
    if (_initializationLatch.getCount() > 0) {
      _contentStreamObserver = streamObserver;
      _initializationLatch.countDown();
    }
  }

  @Override
  public MailboxContent receive() throws Exception {
    MailboxContent mailboxContent = null;
    if (waitForInitialize()) {
      mailboxContent = _contentStreamObserver.poll();
      _totalMsgReceived.incrementAndGet();
    }
    return mailboxContent;
  }

  @Override
  public boolean isInitialized() {
    return _initializationLatch.getCount() <= 0;
  }

  @Override
  public boolean isClosed() {
    return isInitialized() && _contentStreamObserver.isCompleted();
  }

  // TODO: fix busy wait. This should be guarded by timeout.
  private boolean waitForInitialize() throws Exception {
    if (_initializationLatch.getCount() > 0) {
      return _initializationLatch.await(DEFAULT_MAILBOX_INIT_TIMEOUT, TimeUnit.MILLISECONDS);
    } else {
      return true;
    }
  }

  @Override
  public String getMailboxId() {
    return _mailboxId;
  }
}
