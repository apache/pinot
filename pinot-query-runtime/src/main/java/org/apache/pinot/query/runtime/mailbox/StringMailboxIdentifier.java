package org.apache.pinot.query.runtime.mailbox;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import org.apache.commons.lang3.StringUtils;


public class StringMailboxIdentifier implements MailboxIdentifier {
  private static final Joiner JOINER = Joiner.on(':');

  private final String _mailboxIdString;
  private final String _jobID;
  private final String _partitionKey;
  private final String _fromAuthority;
  private final String _toAuthority;

  public StringMailboxIdentifier(String mailboxId) {
    _mailboxIdString = mailboxId;
    String[] splits = mailboxId.split(":");
    Preconditions.checkState(splits.length == 6);
    _jobID = splits[0];
    _partitionKey = splits[1];
    _fromAuthority = splits[2] + ":" + splits[3];
    _toAuthority = splits[4] + ":" + splits[5];

    // assert that resulting string are identical.
    Preconditions.checkState(
        JOINER.join(_jobID, _partitionKey, _fromAuthority, _toAuthority).equals(_mailboxIdString));
  }

  @Override
  public String getJobId() {
    return _jobID;
  }

  @Override
  public String getPartitionKey() {
    return _partitionKey;
  }

  @Override
  public String getFromAuthority() {
    return _fromAuthority;
  }

  @Override
  public String getToAuthority() {
    return _toAuthority;
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
