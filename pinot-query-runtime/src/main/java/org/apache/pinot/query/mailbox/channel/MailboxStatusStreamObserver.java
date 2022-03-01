package org.apache.pinot.query.mailbox.channel;

import io.grpc.stub.StreamObserver;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.pinot.common.proto.Mailbox;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * This is the observer used by the SendingMailbox to send data over the wire.
 *
 * Once SendingMailbox.init() is called, one instances of this class is created by the GRCPMailboxService;
 * It will start the connection and listen to changes on the mailbox's data buffer; once data is put into the mailbox
 * buffer this sending thread will start sending the data.
 */
public class MailboxStatusStreamObserver implements StreamObserver<Mailbox.MailboxStatus> {
  private static final Logger LOGGER = LoggerFactory.getLogger(MailboxStatusStreamObserver.class);
  private static final int DEFAULT_MAILBOX_QUEUE_CAPACITY = 5;
  private static final long DEFAULT_MAILBOX_POLL_TIMEOUT_MS = 1000L;
  private final AtomicInteger _bufferSize = new AtomicInteger(5);
  private final AtomicBoolean _isCompleted = new AtomicBoolean(false);

  private StreamObserver<Mailbox.MailboxContent> _mailboxContentStreamObserver;

  public MailboxStatusStreamObserver() {
  }

  public void init(StreamObserver<Mailbox.MailboxContent> mailboxContentStreamObserver) {
    this._mailboxContentStreamObserver = mailboxContentStreamObserver;
  }

  public void send(Mailbox.MailboxContent mailboxContent) {
    _mailboxContentStreamObserver.onNext(mailboxContent);
  }

  public void complete() {
    this._mailboxContentStreamObserver.onCompleted();
  }

  @Override
  public void onNext(Mailbox.MailboxStatus mailboxStatus) {
    // when received a mailbox status from the receiving end, sending end update the known buffer size available
    // so we can make better throughput send judgement. here is a simple example.
    // TODO: this feedback info is not used to throttle the send speed. it is currently being discarded.
    if (mailboxStatus.getMetadataMap().containsKey("buffer.size")) {
      _bufferSize.set(Integer.parseInt(mailboxStatus.getMetadataMap().get("buffer.size")));
    } else {
      _bufferSize.set(DEFAULT_MAILBOX_QUEUE_CAPACITY); // DEFAULT_AVAILABILITY;
    }
  }

  @Override
  public void onError(Throwable e) {
    this._isCompleted.set(true);
    this.shutdown();
    throw new RuntimeException(e);
  }

  private void shutdown() {
  }

  @Override
  public void onCompleted() {
    this._isCompleted.set(true);
    this.shutdown();
  }
}
