package org.apache.pinot.query.runtime.mailbox;

/**
 *
 */
public interface MailboxIdentifier {

  String getJobId();

  String getPartitionKey();

  String getFromAuthority();

  String getToAuthority();
}
