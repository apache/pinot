package org.apache.pinot.query.runtime.mailbox;

import com.google.common.base.Preconditions;
import com.google.protobuf.ByteString;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import org.apache.pinot.common.proto.Mailbox;
import org.testng.Assert;
import org.testng.annotations.Test;


public class GrpcMailboxServiceTest extends GrpcMailboxServiceTestBase {

  @Test
  public void testHappyPath() throws Exception {
    Preconditions.checkState(_mailboxServices.size() >= 2);
    Map.Entry<Integer, GrpcMailboxService> sender = _mailboxServices.firstEntry();
    Map.Entry<Integer, GrpcMailboxService> receiver = _mailboxServices.lastEntry();
    String mailboxId = String.format("happyPath:partitionKey:localhost:%d:localhost:%d",
        sender.getKey(), receiver.getKey());
    SendingMailbox<Mailbox.MailboxContent> sendingMailbox =
        sender.getValue().getSendingMailbox(mailboxId);
    ReceivingMailbox<Mailbox.MailboxContent> receivingMailbox =
        receiver.getValue().getReceivingMailbox(mailboxId);

    // create mock object
    Mailbox.MailboxContent testContent = getTestMailboxContent(mailboxId);
    Thread.sleep(100);
    sendingMailbox.send(testContent);
    Thread.sleep(100);
    Mailbox.MailboxContent receivedContent = receivingMailbox.receive();

    Assert.assertEquals(receivedContent, testContent);
  }

  private Mailbox.MailboxContent getTestMailboxContent(String mailboxId) {
    return Mailbox.MailboxContent.newBuilder()
        .setMailboxId(mailboxId)
        .putAllMetadata(Map.of("key", "value"))
        .setPayload(ByteString.copyFrom("dummy", StandardCharsets.UTF_8))
        .build();
  }
}
