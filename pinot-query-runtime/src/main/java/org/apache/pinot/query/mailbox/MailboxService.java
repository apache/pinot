/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.query.mailbox;

/**
 * Mailbox service that handles transfer for mailbox contents.
 *
 * @param <T> type of content supported by this mailbox service.
 */
public interface MailboxService<T> {

  /**
   * Starting the mailbox service.
   */
  void start();

  /**
   * Shutting down the mailbox service.s
   */
  void shutdown();

  /**
   * Get the host name on which this mailbox service is runnning on.
   *
   * @return the host.
   */
  String getHostname();

  /**
   * Get the host port that receives inbound mailbox message.
   *
   * @return the port.
   */
  int getMailboxPort();

  /**
   * Look up a receiving mailbox by {@link MailboxIdentifier}.
   *
   * <p>the acquired {@link ReceivingMailbox} will be constructed if not exist already, but it might not have been
   * initialized.
   *
   * @param mailboxId mailbox identifier.
   * @return a receiving mailbox.
   */
  ReceivingMailbox<T> getReceivingMailbox(MailboxIdentifier mailboxId);

  /**
   * Look up a sending mailbox by {@link MailboxIdentifier}.
   *
   * @param mailboxId mailbox identifier.
   * @return a sending mailbox.
   */
  SendingMailbox<T> getSendingMailbox(MailboxIdentifier mailboxId);
}
