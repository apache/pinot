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
import java.util.List;
import java.util.concurrent.TimeoutException;
import org.apache.pinot.query.runtime.blocks.MseBlock;
import org.apache.pinot.query.runtime.operator.exchange.BlockExchange;
import org.apache.pinot.segment.spi.memory.DataBuffer;


/**
 * Mailbox that's used to send data.
 *
 * Usages of this interface should follow the pattern:
 *
 * <ol>
 *   <li>Zero or more calls to {@link #send(MseBlock.Data)}</li>
 *   <li>Then exactly one of:
 *     <ul>
 *       <li>One call to {@link #send(MseBlock.Eos, List)} if the receiver is not early terminated</li>
 *       <li>One call to {@link #cancel(Throwable)} if the sender wants to cancel the receiver</li>
 *     </ul>
 *   </li>
 * </ol>
 */
public interface SendingMailbox {

  /**
   * Returns whether the mailbox is sending data to a local receiver, where blocks can be directly passed to the
   * receiver.
   */
  boolean isLocal();

  /**
   * Sends a data block to the receiver. Note that SendingMailbox are required to acquire resources lazily in this call,
   * and they should <b>not</b> acquire any resources when they are created. This method should throw if there was an
   * error sending the data, since that would allow {@link BlockExchange} to exit early.
   */
  void send(MseBlock.Data data)
      throws IOException, TimeoutException;

  /**
   * Sends an EOS block to the receiver. Note that SendingMailbox are required to acquire resources lazily in this call,
   * and they should <b>not</b> acquire any resources when they are created. This method should throw if there was an
   * error sending the data, since that would allow {@link BlockExchange} to exit early.
   */
  void send(MseBlock.Eos block, List<DataBuffer> serializedStats)
      throws IOException, TimeoutException;

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
