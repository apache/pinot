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

import java.util.List;
import org.apache.pinot.query.runtime.blocks.MseBlock;
import org.apache.pinot.segment.spi.memory.DataBuffer;


/// Mailbox that's used to send data.
///
/// Usages of this interface should follow the pattern:
///
/// 1. Zero or more calls to [#send(MseBlock.Data)]
/// 2. Then exactly one of:
///   - One call to [#send(MseBlock.Eos, List)] if the receiver is not early terminated
///   - One call to [#cancel(Throwable)] if the sender wants to cancel the receiver
public interface SendingMailbox extends AutoCloseable {

  /// Returns whether blocks can be passed to this mailbox whole, instead of being split into smaller blocks that
  /// respect the maximum content size of a mailbox message.
  ///
  /// This says nothing about whether the receiver ends up with a reference to the block: see
  /// [#deliversByReference()].
  boolean isLocal();

  /// Returns whether a receiver may keep a reference to the blocks sent to this mailbox, instead of reading their
  /// contents within [#send(MseBlock.Data)]. The answer must not change during the life of the mailbox, so callers
  /// can read it once.
  ///
  /// A caller that sends the same block instance to more than one mailbox, when anything downstream mutates the
  /// contents of that block, must:
  ///
  /// 1. Give a copy of the block to all the mailboxes that return `true` here, except one.
  /// 2. Give the original block to that remaining mailbox last, once every other mailbox has returned from
  ///    [#send(MseBlock.Data)].
  ///
  /// Step 2 matters as much as step 1. A receiver of the original block can mutate it as soon as `send` gives it the
  /// block, which would corrupt the block for a mailbox that is still reading it.
  ///
  /// See [org.apache.pinot.query.runtime.operator.exchange.BroadcastExchange].
  boolean deliversByReference();

  /// Sends a data block to the receiver. Note that SendingMailbox are required to acquire resources lazily in this
  /// call, and they should **not** acquire any resources when they are created. This method should throw if there was
  /// an error sending the data, since that would allow
  /// [org.apache.pinot.query.runtime.operator.exchange.BlockExchange] to exit early.
  ///
  /// Implementations that return `false` from [#deliversByReference()] must finish reading the block before this
  /// method returns, because the caller may then pass the same block to a receiver that mutates it.
  void send(MseBlock.Data data);

  /// Sends an EOS block to the receiver. Note that SendingMailbox are required to acquire resources lazily in this
  /// call, and they should **not** acquire any resources when they are created. This method should throw if there was
  /// an error sending the data, since that would allow
  /// [org.apache.pinot.query.runtime.operator.exchange.BlockExchange] to exit early.
  void send(MseBlock.Eos block, List<DataBuffer> serializedStats);

  /// Cancels the mailbox and notifies the receiver of the cancellation so that it can release the underlying resources.
  /// No more blocks can be sent after calling this method.
  void cancel(Throwable t);

  /// Returns whether the [ReceivingMailbox] is already closed. There is no need to send more blocks after the
  /// mailbox is terminated.
  boolean isTerminated();

  /// Returns whether the [ReceivingMailbox] is considered itself finished, and is expected a EOS block with
  /// statistics to be sent next.
  boolean isEarlyTerminated();
}
