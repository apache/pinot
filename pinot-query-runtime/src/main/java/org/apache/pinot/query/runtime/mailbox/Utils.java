package org.apache.pinot.query.runtime.mailbox;

import com.google.common.base.Joiner;


public final class Utils {
  private static final Joiner JOINER = Joiner.on(':');

  public static String constructChannelId(String mailboxId) {
    MailboxIdentifier mailboxIdentifier = toMailboxIdentifier(mailboxId);
    String[] toAuthority = mailboxIdentifier.getToAuthority().split(":");
    // sender port is the opened Grpc server port via GRPC mailbox.
    // receiver port doesn't matter.
    return JOINER.join(toAuthority[0], toAuthority[1]);
  }

  public static MailboxIdentifier toMailboxIdentifier(String mailboxId) {
    return new StringMailboxIdentifier(mailboxId);
  }

  public static String fromMailboxIdentifier(MailboxIdentifier mailboxId) {
    return mailboxId.toString();
  }
}
