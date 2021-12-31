package org.apache.pinot.query.runtime.mailbox.channel;

import io.grpc.stub.StreamObserver;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.pinot.common.proto.Mailbox;


/**
 * This is the observer used by the SendingMailbox to send data over the wire.
 *
 * Once SendingMailbox.init() is called, one instances of this class is created by the GRCPMailboxService;
 * It will start the connection and listen to changes on the mailbox's data buffer; once data is put into the mailbox
 * buffer this sending thread will start sending the data.
 */
public class MailboxStatusStreamObserver implements StreamObserver<Mailbox.MailboxStatus> {
  private static final int DEFAULT_MAILBOX_QUEUE_CAPACITY = 5;
  private static final long DEFAULT_MAILBOX_POLL_TIMEOUT = 1000L;
  private final AtomicInteger _bufferSize = new AtomicInteger(5);
  private final AtomicBoolean _isRunning = new AtomicBoolean();
  private final ArrayBlockingQueue<Mailbox.MailboxContent> _buffer;
  private final ExecutorService _executorService;

  private StreamObserver<Mailbox.MailboxContent> _mailboxContentStreamObserver;

  public MailboxStatusStreamObserver() {
    _buffer = new ArrayBlockingQueue<>(DEFAULT_MAILBOX_QUEUE_CAPACITY);
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
  }

  public void init(StreamObserver<Mailbox.MailboxContent> mailboxContentStreamObserver) {
    this._mailboxContentStreamObserver = mailboxContentStreamObserver;
    this._isRunning.set(true);
    _executorService.submit(() -> {
      while (_isRunning.get()) {
        this._mailboxContentStreamObserver.onNext(_buffer.poll());
      }
    });
  }

  public void offer(Mailbox.MailboxContent mailboxContent) {
    _buffer.offer(mailboxContent);
  }

  @Override
  public void onError(Throwable e) {
    this._isRunning.set(false);
    this.shutdown();
    throw new RuntimeException(e);
  }

  private void shutdown() {
    _buffer.clear();
    _executorService.shutdown();
  }

  @Override
  public void onCompleted() {
    this._isRunning.set(false);
    // send onCompleted to the server indicating this stream has finished.
    this._mailboxContentStreamObserver.onCompleted();
    this.shutdown();
  }
}
