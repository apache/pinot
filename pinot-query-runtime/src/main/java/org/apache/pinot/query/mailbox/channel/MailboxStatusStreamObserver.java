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
  private static final long DEFAULT_MAILBOX_POLL_TIMEOUT = 1000L;
  private final AtomicInteger _bufferSize = new AtomicInteger(5);
  private final AtomicBoolean _isCompleted = new AtomicBoolean(false);
  private final ArrayBlockingQueue<Mailbox.MailboxContent> _sendingBuffer;
  private final ExecutorService _executorService;

  private StreamObserver<Mailbox.MailboxContent> _mailboxContentStreamObserver;

  public MailboxStatusStreamObserver() {
    _sendingBuffer = new ArrayBlockingQueue<>(DEFAULT_MAILBOX_QUEUE_CAPACITY);
    _executorService = Executors.newFixedThreadPool(1);
  }

  @Override
  public void onNext(Mailbox.MailboxStatus mailboxStatus) {
    // when received a mailbox status from the receiving end, sending end update the known buffer size available
    // so we can make better throughput send judgement. here is a simple example.
    if (mailboxStatus.getMetadataMap().containsKey("buffer.size")) {
      _bufferSize.set(Integer.parseInt(mailboxStatus.getMetadataMap().get("buffer.size")));
    } else {
      _bufferSize.set(1); // DEFAULT_AVAILABILITY;
    }
    // if receiving end echo data receive finished. we finished via calling channel complete
    if (mailboxStatus.getMetadataMap().containsKey("finished")) {
      System.out.printf("onComplete %s, metadata: %s\n", mailboxStatus.getMailboxId(), mailboxStatus.getMetadataMap());
      this._mailboxContentStreamObserver.onCompleted();
    } else {
      // TODO: check stream is not completed before sending data
      // It should never be completed unless content is completed, however since within this class we can't know.
      while (!_isCompleted.get()) {
        try {
          Mailbox.MailboxContent content = _sendingBuffer.poll(DEFAULT_MAILBOX_POLL_TIMEOUT, TimeUnit.MILLISECONDS);
          if (content != null) {
            this._mailboxContentStreamObserver.onNext(content);
            break;
          }
        } catch (InterruptedException e) {
          LOGGER.error("Interrupted during mailbox content", e);
        }
      }
    }
  }

  public void init(StreamObserver<Mailbox.MailboxContent> mailboxContentStreamObserver, Mailbox.MailboxContent data) {
    this._mailboxContentStreamObserver = mailboxContentStreamObserver;
    _mailboxContentStreamObserver.onNext(data);
  }

  public void offer(Mailbox.MailboxContent mailboxContent) {
    _sendingBuffer.offer(mailboxContent);
  }

  @Override
  public void onError(Throwable e) {
    this._isCompleted.set(true);
    this.shutdown();
    throw new RuntimeException(e);
  }

  private void shutdown() {
    _sendingBuffer.clear();
    _executorService.shutdown();
  }

  @Override
  public void onCompleted() {
    this._isCompleted.set(true);
    this.shutdown();
  }
}
