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

import org.apache.pinot.query.runtime.operator.MailboxReceiveOperator;
import org.apache.pinot.query.runtime.operator.OpChain;


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
   * Shutting down the mailbox service.
   */
  void shutdown();

  /**
   * Get the host name on which this mailbox service is running on.
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
   * Return a {@link ReceivingMailbox} for the given {@link MailboxIdentifier}.
   *
   * @param mailboxId mailbox identifier.
   * @return a receiving mailbox.
   */
  ReceivingMailbox<T> getReceivingMailbox(MailboxIdentifier mailboxId);

  /**
   * Return a sending-mailbox for the given {@link MailboxIdentifier}. The returned {@link SendingMailbox} is
   * uninitialized, i.e. it will not open the underlying channel or acquire any additional resources. Instead the
   * {@link SendingMailbox} will initialize lazily when the data is sent for the first time through it.
   *
   * @param mailboxId mailbox identifier.
   * @param deadlineMs deadline in milliseconds, which is usually the same as the query deadline.
   * @return a sending mailbox.
   */
  SendingMailbox<T> getSendingMailbox(MailboxIdentifier mailboxId, long deadlineMs);

  /**
   * A {@link ReceivingMailbox} for a given {@link OpChain} may be created before the OpChain is even registered.
   * Reason being that the sender starts sending data, and the receiver starts receiving the same without waiting for
   * the OpChain to be registered. The ownership for the ReceivingMailbox hence lies with the MailboxService and not
   * the OpChain. There are two ways in which a MailboxService may release its references to a ReceivingMailbox and
   * the underlying resources:
   *
   * <ol>
   *   <li>
   *     If the OpChain corresponding to a ReceivingMailbox was closed or cancelled. In that case,
   *     {@link MailboxReceiveOperator} will call this method as part of its close/cancel call. This is the main
   *     reason why this method exists.
   *   </li>
   *   <li>
   *     There can be cases where the corresponding OpChain was never registered with the scheduler. In that case, it
   *     is up to the {@link MailboxService} to ensure that there are no leaks of resources. E.g. it could setup a
   *     periodic job to detect such mailbox and do any clean-up. Note that for this case, it is not mandatory for
   *     the {@link MailboxService} to use this method. It can use any internal method it needs to do the clean-up.
   *   </li>
   * </ol>
   */
  void releaseReceivingMailbox(ReceivingMailbox<T> mailbox);
}
