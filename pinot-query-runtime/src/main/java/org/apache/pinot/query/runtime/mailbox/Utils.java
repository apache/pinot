package org.apache.pinot.query.runtime.mailbox;

import com.google.common.base.Joiner;


public final class Utils {
  private static final Joiner JOINER = Joiner.on(':');

  public static String constructChannelId(String mailboxId) {
    MailboxIdentifier mailboxIdentifier = toMailboxIdentifier(mailboxId);
    return JOINER.join(mailboxIdentifier.getToHost(), mailboxIdentifier.getToPort());
  }

  public static MailboxIdentifier toMailboxIdentifier(String mailboxId) {
    return new StringMailboxIdentifier(mailboxId);
  }

  public static String fromMailboxIdentifier(MailboxIdentifier mailboxId) {
    return mailboxId.toString();
  }
}
