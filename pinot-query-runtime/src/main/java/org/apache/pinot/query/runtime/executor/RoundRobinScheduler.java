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
package org.apache.pinot.query.runtime.executor;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Sets;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Queue;
import java.util.Set;
import org.apache.pinot.query.mailbox.MailboxIdentifier;
import org.apache.pinot.query.runtime.operator.OpChain;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * This is a scheduler that schedules operator chains in round robin fashion,
 * but will only schedule them when there is work to be done. The availability
 * of work is signaled using the {@link #onDataAvailable(MailboxIdentifier)}
 * callback.
 */
public class RoundRobinScheduler implements OpChainScheduler {
  private static final Logger LOGGER = LoggerFactory.getLogger(RoundRobinScheduler.class);

  // the _available queue contains operator chains that are available
  // to this scheduler but do not have any data available to schedule
  // while the _ready queue contains the operator chains that are ready
  // to be scheduled (have data, or are first-time scheduled)
  private final Queue<OpChain> _available = new LinkedList<>();
  private final Queue<OpChain> _ready = new LinkedList<>();

  // using a Set here is acceptable because calling hasNext() and
  // onDataAvailable() cannot occur concurrently - that means that
  // anytime we schedule a new operator based on the presence of
  // mail we can be certain that it will consume all of the mail
  // form that mailbox, even if there are multiple items in it. If,
  // during execution of that operator, more mail appears, then the
  // operator will be rescheduled immediately potentially resulting
  // in a false-positive schedule
  @VisibleForTesting
  final Set<MailboxIdentifier> _seenMail = new HashSet<>();

  @Override
  public void register(OpChain operatorChain, boolean isNew) {
    // the first time an operator chain is scheduled, it should
    // immediately be considered ready in case it does not need
    // read from any mailbox (e.g. with a LiteralValueOperator)
    (isNew ? _ready : _available).add(operatorChain);
    trace("registered " + operatorChain);
  }

  @Override
  public void onDataAvailable(MailboxIdentifier mailbox) {
    // it may be possible to receive this callback when there's no corresponding
    // operator chain registered to the mailbox - this can happen when either
    // (1) we get the callback before the first register is called or (2) we get
    // the callback while the operator chain is executing. to account for this,
    // we just store it in a set of seen mail and only check for it when hasNext
    // is called.
    //
    // note that scenario (2) may cause a false-positive schedule where an operator
    // chain gets scheduled for mail that it had already processed, in which case
    // the operator chain will simply do nothing and get put back onto the queue.
    // scenario (2) may additionally cause a memory leak - if onDataAvailable is
    // called with an EOS block _while_ the operator chain is executing, the chain
    // will consume the EOS block and computeReady() will never remove the mailbox
    // from the _seenMail set.
    //
    // TODO: fix the memory leak by adding a close(opChain) callback
    _seenMail.add(mailbox);
    trace("got mail for " + mailbox);
  }

  @Override
  public boolean hasNext() {
    computeReady();
    return !_ready.isEmpty();
  }

  @Override
  public OpChain next() {
    OpChain op = _ready.poll();
    trace("Polled " + op);
    return op;
  }

  @Override
  public int size() {
    return _ready.size() + _available.size();
  }

  private void computeReady() {
    Iterator<OpChain> availableChains = _available.iterator();

    // the algorithm here iterates through all available chains and checks
    // to see whether or not any of the available chains have seen mail for
    // at least one of the mailboxes they receive from - if they do, then
    // we should make that chain available and remove any mail from the
    // mailboxes that it would consume from (after it is scheduled, all
    // mail available to it will have been consumed).
    while (availableChains.hasNext()) {
      OpChain chain = availableChains.next();
      Sets.SetView<MailboxIdentifier> intersect = Sets.intersection(chain.getReceivingMailbox(), _seenMail);

      if (!intersect.isEmpty()) {
        // use an immutable copy because set views use the underlying sets
        // directly, which would cause a concurrent modification exception
        // when removing data from _seenMail
        _seenMail.removeAll(intersect.immutableCopy());
        _ready.add(chain);
        availableChains.remove();
      }
    }
  }

  private void trace(String operation) {
    LOGGER.trace("({}) Ready: {}, Available: {}, Mail: {}",
        operation, _ready, _available, _seenMail);
  }
}
