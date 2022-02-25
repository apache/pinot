package org.apache.pinot.query.mailbox;

import com.google.common.base.Preconditions;
import com.google.protobuf.ByteString;
import java.io.IOException;
import java.util.Map;
import org.apache.pinot.common.proto.Mailbox;
import org.apache.pinot.core.common.datatable.DataTableBuilder;
import org.apache.pinot.query.runtime.blocks.DataTableBlock;
import org.apache.pinot.util.TestUtils;
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
    sendingMailbox.send(testContent);

    // wait for receiving mailbox to be created.
    TestUtils.waitForCondition(aVoid -> {
      return receivingMailbox.isInitialized();
    }, 5000L, "Receiving mailbox initialize failed!");

    Mailbox.MailboxContent receivedContent = receivingMailbox.receive();
    Assert.assertEquals(receivedContent, testContent);

    TestUtils.waitForCondition(aVoid -> {
      return receivingMailbox.isClosed();
    }, 5000L, "Receiving mailbox is not closed properly!");
  }

  private Mailbox.MailboxContent getTestMailboxContent(String mailboxId) throws IOException {
    return Mailbox.MailboxContent.newBuilder()
        .setMailboxId(mailboxId)
        .putAllMetadata(Map.of("key", "value", "finished", "true"))
        .setPayload(ByteString.copyFrom(new DataTableBlock(DataTableBuilder.getEmptyDataTable()).toBytes()))
        .build();
  }
}
