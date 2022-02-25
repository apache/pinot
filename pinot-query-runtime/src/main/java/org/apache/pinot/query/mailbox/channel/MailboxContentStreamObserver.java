package org.apache.pinot.query.mailbox.channel;

import io.grpc.stub.StreamObserver;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;
import org.apache.pinot.common.proto.Mailbox;
import org.apache.pinot.query.mailbox.GrpcMailboxService;
import org.apache.pinot.query.mailbox.GrpcReceivingMailbox;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * This is the content streaming observer.
 *
 * When the observer onNext() is called (e.g. when data packet has arrived to the GRPCMailboxServer) it puts the
 * mailbox content to the receiving mailbox buffer; response with the remaining buffer size of the receiving mailbox.
 */
public class MailboxContentStreamObserver implements StreamObserver<Mailbox.MailboxContent> {
  private static final Logger LOGGER = LoggerFactory.getLogger(MailboxContentStreamObserver.class);
  private static final int DEFAULT_MAILBOX_QUEUE_CAPACITY = 5;
  private static final long DEFAULT_MAILBOX_POLL_TIMEOUT = 1000L;
  private final GrpcMailboxService _mailboxService;
  private final StreamObserver<Mailbox.MailboxStatus> _responseObserver;
  private ArrayBlockingQueue<Mailbox.MailboxContent> _receivingBuffer;

  public MailboxContentStreamObserver(GrpcMailboxService mailboxService, StreamObserver<Mailbox.MailboxStatus> responseObserver) {
    _mailboxService = mailboxService;
    _responseObserver = responseObserver;
    _receivingBuffer = new ArrayBlockingQueue<>(DEFAULT_MAILBOX_QUEUE_CAPACITY);
  }

  public Mailbox.MailboxContent poll() {
    while (_receivingBuffer != null) {
      try {
        Mailbox.MailboxContent content = _receivingBuffer.poll(DEFAULT_MAILBOX_POLL_TIMEOUT, TimeUnit.MILLISECONDS);
        if (content != null) {
          return content;
        }
      } catch (InterruptedException e) {
        LOGGER.error("Interrupt occurred while waiting for mailbox content", e);
      }
    }
    return null;
  }

  @Override
  public void onNext(Mailbox.MailboxContent mailboxContent) {
    GrpcReceivingMailbox receivingMailbox = (GrpcReceivingMailbox) _mailboxService.getReceivingMailbox(mailboxContent.getMailboxId());
    receivingMailbox.init(this);
    // when the receiving end receives a message. put it in the mailbox queue and returns the buffer available.
    int remainingCapacity = _receivingBuffer.remainingCapacity() - 1;
    _receivingBuffer.offer(mailboxContent);
    Mailbox.MailboxStatus status = Mailbox.MailboxStatus.newBuilder()
        .setMailboxId(mailboxContent.getMailboxId())
        .putMetadata("buffer.size", String.valueOf(remainingCapacity))
        .build();
    // TODO: fix race condition for closing response observer before sending the response metadata.
    _responseObserver.onNext(status);
  }

  @Override
  public void onError(Throwable e) {
    throw new RuntimeException(e);
  }

  @Override
  public void onCompleted() {
    this._receivingBuffer.clear();
    this._receivingBuffer = null;
    this._responseObserver.onCompleted();
  }
}
