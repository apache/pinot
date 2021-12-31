package org.apache.pinot.query.runtime.mailbox.channel;

import io.grpc.stub.StreamObserver;
import java.util.concurrent.ArrayBlockingQueue;
import org.apache.pinot.common.proto.Mailbox;
import org.apache.pinot.query.runtime.mailbox.GrpcMailboxService;
import org.apache.pinot.query.runtime.mailbox.GrpcReceivingMailbox;


/**
 * This is the content streaming observer.
 *
 * When the observer onNext() is called (e.g. when data packet has arrived to the GRPCMailboxServer) it puts the
 * mailbox content to the receiving mailbox buffer; response with the remaining buffer size of the receiving mailbox.
 */
public class MailboxContentStreamObserver implements StreamObserver<Mailbox.MailboxContent> {
  private static final int DEFAULT_MAILBOX_QUEUE_CAPACITY = 5;
  private static final long DEFAULT_MAILBOX_POLL_TIMEOUT = 1000L;
  private final GrpcMailboxService _mailboxService;
  private final StreamObserver<Mailbox.MailboxStatus> _responseObserver;
  private final ArrayBlockingQueue<Mailbox.MailboxContent> _buffer;

  public MailboxContentStreamObserver(GrpcMailboxService mailboxService, StreamObserver<Mailbox.MailboxStatus> responseObserver) {
    _mailboxService = mailboxService;
    _responseObserver = responseObserver;
    _buffer = new ArrayBlockingQueue<>(DEFAULT_MAILBOX_QUEUE_CAPACITY);
  }

  public Mailbox.MailboxContent poll() {
    return _buffer.poll();
  }

  @Override
  public void onNext(Mailbox.MailboxContent mailboxContent) {
    GrpcReceivingMailbox receivingMailbox = (GrpcReceivingMailbox) _mailboxService.getReceivingMailbox(mailboxContent.getMailboxId());
    receivingMailbox.init(this);
    // when the receiving end receives a message. put it in the mailbox queue and returns the buffer available.
    _buffer.offer(mailboxContent);
    Mailbox.MailboxStatus status = Mailbox.MailboxStatus.newBuilder()
        .setMailboxId(mailboxContent.getMailboxId())
        .putMetadata("buffer.size", String.valueOf(_buffer.remainingCapacity()))
        .build();
    _responseObserver.onNext(status);
  }

  @Override
  public void onError(Throwable e) {
    throw new RuntimeException(e);
  }

  @Override
  public void onCompleted() {
    // .. no-op. the observer is closed from the sender side.
  }
}
