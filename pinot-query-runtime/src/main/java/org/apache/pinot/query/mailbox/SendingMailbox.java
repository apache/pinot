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

import java.io.IOException;
import java.util.concurrent.TimeoutException;
import org.apache.pinot.query.runtime.blocks.TransferableBlock;
import org.apache.pinot.query.runtime.operator.exchange.BlockExchange;


/**
 * Mailbox that's used to send data.
 */
public interface SendingMailbox {

  /**
   * Sends a block to the receiver. Note that SendingMailbox are required to acquire resources lazily in this call, and
   * they should <b>not</b> acquire any resources when they are created. This method should throw if there was an error
   * sending the data, since that would allow {@link BlockExchange} to exit early.
   */
  void send(TransferableBlock block)
      throws IOException, TimeoutException;

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
  void complete();

  /**
   * Cancels the mailbox and notifies the receiver of the cancellation so that it can release the underlying resources.
   * No more blocks can be sent after calling this method.
   */
  void cancel(Throwable t);

  /**
   * Returns whether the {@link ReceivingMailbox} is already closed. There is no need to send more blocks after the
   * mailbox is terminated.
   */
  boolean isTerminated();

  /**
   * Returns whether the {@link ReceivingMailbox} is considered itself finished, and is expected a EOS block with
   * statistics to be sent next.
   */
  boolean isEarlyTerminated();
}
