package org.apache.pinot.query.runtime.mailbox;

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
}
