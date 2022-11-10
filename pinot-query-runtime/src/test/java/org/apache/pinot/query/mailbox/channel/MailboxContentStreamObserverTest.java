package org.apache.pinot.query.mailbox.channel;

import io.grpc.stub.StreamObserver;
import org.apache.pinot.common.proto.Mailbox;
import org.apache.pinot.query.mailbox.GrpcMailboxService;
import org.mockito.Mock;
import org.testng.annotations.Test;


public class MailboxContentStreamObserverTest {

  private static final class Runner implements Runnable {
    @Mock
    GrpcMailboxService _mailboxService;
    @Mock
    StreamObserver<Mailbox.MailboxStatus> _streamObserver;

    @Override
    public void run() {
      MailboxContentStreamObserver observer = new MailboxContentStreamObserver(_mailboxService, _streamObserver);
      observer.poll();
    }
  }

  @Test
  public void TestPollInteruptException()
      throws InterruptedException {
    Runner t = new Runner();
    Thread t1 = new Thread(t);
    t1.start();
    Thread.sleep(1000);
    t1.interrupt();
    t1.join();
  }
}
