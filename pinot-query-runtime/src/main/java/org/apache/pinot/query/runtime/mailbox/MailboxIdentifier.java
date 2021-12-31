package org.apache.pinot.query.runtime.mailbox;

/**
 *
 */
public interface MailboxIdentifier {

  String getJobId();

  String getPartitionKey();

  String getFromHost();

  String getToHost();

  int getToPort();
}
