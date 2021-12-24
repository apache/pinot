package org.apache.pinot.query.runtime.mailbox;

/**
 * Mailbox service that handles transfer for mailbox contents.
 *
 * @param <T> type of content supported by this mailbox service.
 */
public interface MailboxService<T> {

  void start();

  void shutdown();

  ReceivingMailbox<T> getReceivingMailbox(String mailboxId);

  SendingMailbox<T> getSendingMailbox(String mailboxId);
}
