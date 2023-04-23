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

import javax.annotation.Nullable;
import org.apache.pinot.query.runtime.operator.MailboxReceiveOperator;
import org.apache.pinot.query.runtime.operator.MailboxSendOperator;


/**
 * Mailbox that's used to receive data. Ownership of the ReceivingMailbox is with the MailboxService, which is unlike
 * the {@link SendingMailbox} whose ownership lies with the {@link MailboxSendOperator}. This is because the
 * ReceivingMailbox can be initialized even before the corresponding OpChain is registered on the receiver, whereas
 * the SendingMailbox is initialized when the MailboxSendOperator is running. Also see {@link #isInitialized()}.
 *
 * @param <T> the unit of data that each {@link #receive()} call returns.
 */
public interface ReceivingMailbox<T> {

  MailboxIdentifier getId();

  /**
   * Returns a unit of data. Implementations are allowed to return null, in which case {@link MailboxReceiveOperator}
   * will assume that this mailbox doesn't have any data to return and it will instead poll the other mailbox (if any).
   */
  @Nullable
  T receive() throws Exception;

  /**
   * A ReceivingMailbox is considered initialized when it has a reference to the underlying channel used for receiving
   * the data. The underlying channel may be a gRPC stream, in-memory queue, etc. Once a receiving mailbox is
   * initialized, it has the ability to close the underlying channel via the {@link #cancel()} method.
   */
  boolean isInitialized();

  /**
   * A ReceivingMailbox is considered closed if it has sent all the data to the receiver and doesn't have any more data
   * to send.
   */
  boolean isClosed();

  /**
   * A ReceivingMailbox may hold a reference to the underlying channel. Usually the channel would be automatically
   * closed once all the data has been received by the receiver, and in such cases {@link #isClosed()} returns true.
   * However in failure scenarios the underlying channel may not be released, and the receiver can use this method to
   * ensure the same.
   *
   * This API should ensure that the underlying channel is "released" if it hasn't been already. If the channel has
   * already been released, the API shouldn't throw and instead return gracefully.
   *
   * <p>
   *   This method may be called multiple times, so implementations should ensure this is idempotent.
   * </p>
   */
  void cancel();
}
