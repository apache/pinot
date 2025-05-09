package org.apache.pinot.query.mailbox.channel;

import com.google.protobuf.ByteString;
import io.grpc.stub.StreamObserver;
import org.apache.pinot.common.proto.Mailbox.MailboxContent;
import org.apache.pinot.common.proto.Mailbox.MailboxStatus;
import org.apache.pinot.query.mailbox.MailboxService;
import org.apache.pinot.query.mailbox.ReceivingMailbox;
import org.mockito.Mockito;
import org.testng.annotations.Test;

import static org.mockito.Mockito.*;

public class MailboxContentObserverTest {

  private static final String TEST_MAILBOX_ID = "test-mailbox-id";
  @Test
  public void testMailboxContentObserverInitializationAndFetch() {
    MailboxService mailboxService = mock(MailboxService.class);
    ReceivingMailbox receivingMailbox = mock(ReceivingMailbox.class);
    when(mailboxService.getReceivingMailbox(TEST_MAILBOX_ID)).thenReturn(receivingMailbox);
    StreamObserver<MailboxStatus> mockStatusObserver = mock(StreamObserver.class);

    MailboxContentObserver observer = new MailboxContentObserver(mailboxService, TEST_MAILBOX_ID, mockStatusObserver);
    verify(mailboxService, times(1)).getReceivingMailbox(TEST_MAILBOX_ID);

    // Now simulate receiving a mailbox content message
    MailboxContent mockContent = Mockito.mock(MailboxContent.class);
    when(mockContent.getPayload()).thenReturn(ByteString.copyFrom(new byte[0]));
    when(mockContent.getMailboxId()).thenReturn(TEST_MAILBOX_ID);
    observer.onNext(mockContent);

    // Mailbox should not be called again in onNext.
    verify(mailboxService, times(1)).getReceivingMailbox(TEST_MAILBOX_ID);
    verifyNoMoreInteractions(mailboxService);
  }
}
