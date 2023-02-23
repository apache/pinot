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

import org.apache.pinot.query.runtime.operator.exchange.BlockExchange;


/**
 * Mailbox that's used to send data.
 *
 * @param <T> unit of data sent in one {@link #send} call.
 */
public interface SendingMailbox<T> {
  /**
   * get the unique identifier for the mailbox.
   *
   * @return Mailbox ID.
   */
  String getMailboxId();

  /**
   * Send a single unit of data to a receiver. Note that SendingMailbox are required to acquire resources lazily in
   * this call and they should <b>not</b> acquire any resources when they are created. This method should throw if there
   * was an error sending the data, since that would allow {@link BlockExchange} to exit early.
   */
  void send(T data)
      throws Exception;

  /**
   * Called when there is no more data to be sent by the {@link BlockExchange}. This is also a signal for the
   * SendingMailbox that the sender is done sending data from its end. Note that this doesn't mean that the receiver
   * has received all the data.
   *
   * <p>
   * <b>Note:</b> While this is similar to a close() method that's usually provided with objects that hold releasable
   * resources, the key difference is that a SendingMailbox cannot completely release the resources on its end
   * gracefully, since it would be waiting for the receiver to ack that it has received all the data. See
   * {@link #cancel} which can allow callers to force release the underlying resources.
   * </p>
   */
  void complete()
      throws Exception;

  /**
   * A SendingMailbox is considered initialized after it has acquired a reference to the underlying channel that will
   * be used to send data to the receiver.
   */
  boolean isInitialized();

  /**
   * A SendingMailbox is considered closed if it has been initialized and it has released all references to the
   * underlying channel.
   */
  boolean isClosed();

  /**
   * Allows terminating the underlying channel.
   */
  void cancel(Throwable t);
}
