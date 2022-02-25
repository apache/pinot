package org.apache.pinot.query.mailbox;

/**
 * Mailbox is used to send and receive data.
 *
 * Mailbox should be instantiated on both side of MailboxServer.
 *
 * @param <T> type of data carried over the mailbox.
 */
public interface ReceivingMailbox<T> {

  /**
   * get the unique identifier for the mailbox.
   *
   * @return Mailbox ID.
   */
  String getMailboxId();

  /**
   * receive a data packet from the mailbox.
   * @return data packet.
   * @throws Exception
   */
  T receive() throws Exception;

  /**
   * Check if receiving mailbox is initialized.
   * @return
   */
  boolean isInitialized();

  /**
   * Check if mailbox is closed.
   * @return
   */
  boolean isClosed();
}
