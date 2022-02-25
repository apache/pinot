package org.apache.pinot.query.mailbox;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;


public class StringMailboxIdentifier implements MailboxIdentifier {
  private static final Joiner JOINER = Joiner.on(':');

  private final String _mailboxIdString;
  private final String _jobId;
  private final String _partitionKey;
  private final String _fromHost;
  private final int _fromPort;
  private final String _toHost;
  private final int _toPort;

  public StringMailboxIdentifier(String jobId, String partitionKey, String fromHost, int fromPort, String toHost,
      int toPort) {
    _jobId = jobId;
    _partitionKey = partitionKey;
    _fromHost = fromHost;
    _fromPort = fromPort;
    _toHost = toHost;
    _toPort = toPort;
    _mailboxIdString = JOINER.join(_jobId, _partitionKey, _fromHost, _fromPort, _toHost, _toPort);
  }

  public StringMailboxIdentifier(String mailboxId) {
    _mailboxIdString = mailboxId;
    String[] splits = mailboxId.split(":");
    Preconditions.checkState(splits.length == 6);
    _jobId = splits[0];
    _partitionKey = splits[1];
    _fromHost = splits[2];
    _fromPort = Integer.parseInt(splits[3]);
    _toHost = splits[4];
    _toPort = Integer.parseInt(splits[5]);

    // assert that resulting string are identical.
    Preconditions.checkState(
        JOINER.join(_jobId, _partitionKey, _fromHost, _fromPort, _toHost, _toPort).equals(_mailboxIdString));
  }

  @Override
  public String getJobId() {
    return _jobId;
  }

  @Override
  public String getPartitionKey() {
    return _partitionKey;
  }

  @Override
  public String getFromHost() {
    return _fromHost;
  }

  @Override
  public String getToHost() {
    return _toHost;
  }

  @Override
  public int getToPort() {
    return _toPort;
  }

  @Override
  public String toString() {
    return _mailboxIdString;
  }

  @Override
  public int hashCode() {
    return _mailboxIdString.hashCode();
  }

  @Override
  public boolean equals(Object that) {
    return (that instanceof StringMailboxIdentifier) &&
        this._mailboxIdString.equals(((StringMailboxIdentifier) that)._mailboxIdString);
  }
}
