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

import com.google.common.collect.Sets;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;
import javax.annotation.concurrent.ThreadSafe;
import org.apache.pinot.query.mailbox.MailboxIdentifier;
import org.apache.pinot.query.runtime.operator.OpChain;
import org.apache.pinot.query.runtime.operator.OpChainId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * This is a scheduler that schedules operator chains in round robin fashion,
 * but will only schedule them when there is work to be done. The availability
 * of work is signaled using the {@link #onDataAvailable(MailboxIdentifier)}
 * callback.
 */
@ThreadSafe
public class RoundRobinScheduler implements OpChainScheduler {
  private static final Logger LOGGER = LoggerFactory.getLogger(RoundRobinScheduler.class);
  private static final String AVAILABLE_RELEASE_THREAD_NAME = "round-robin-scheduler-release-thread";

  private final long _releaseTimeout;
  private final Supplier<Long> _ticker;

  private final Map<OpChainId, OpChain> _aliveChains = new ConcurrentHashMap<>();
  private final Set<OpChainId> _runningChains = Sets.newConcurrentHashSet();
  final Set<OpChainId> _seenMail = Sets.newConcurrentHashSet();
  private final Map<OpChainId, Long> _available = new ConcurrentHashMap<>();

  private final BlockingQueue<OpChain> _ready = new LinkedBlockingQueue<>();

  private final Lock _lock = new ReentrantLock();
  private final ScheduledExecutorService _availableOpChainReleaseService;

  public RoundRobinScheduler(long releaseTimeout) {
    this(releaseTimeout, System::currentTimeMillis);
  }

  public RoundRobinScheduler(long releaseTimeoutMs, Supplier<Long> ticker) {
    _releaseTimeout = releaseTimeoutMs;
    _ticker = ticker;
    _availableOpChainReleaseService = Executors.newSingleThreadScheduledExecutor(r -> {
      Thread t = new Thread(r);
      t.setName(AVAILABLE_RELEASE_THREAD_NAME);
      t.setDaemon(true);
      return t;
    });
    if (releaseTimeoutMs > 0) {
      _availableOpChainReleaseService.scheduleAtFixedRate(() -> {
        for (Map.Entry<OpChainId, Long> entry : _available.entrySet()) {
          if (Thread.interrupted()) {
            LOGGER.warn("Thread={} interrupted. Scheduler may be shutting down.", AVAILABLE_RELEASE_THREAD_NAME);
            break;
          }
          OpChainId opChainId = entry.getKey();
          if (_ticker.get() + _releaseTimeout > entry.getValue()) {
            _lock.lock();
            try {
              if (_available.containsKey(opChainId)) {
                _available.remove(opChainId);
                _ready.offer(_aliveChains.get(opChainId));
              }
            } finally {
              _lock.unlock();
            }
          }
        }
      }, _releaseTimeout, _releaseTimeout, TimeUnit.MILLISECONDS);
    }
  }

  @Override
  public void register(OpChain operatorChain) {
    _lock.lock();
    try {
      _aliveChains.put(operatorChain.getId(), operatorChain);
      _ready.add(operatorChain);
    } finally {
      _lock.unlock();
    }
    trace("registered " + operatorChain);
  }

  @Override
  public void deregister(OpChain operatorChain) {
    _lock.lock();
    try {
      _aliveChains.remove(operatorChain.getId());
      // deregister can only be called if the OpChain is running, so remove from _runningChains.
      _runningChains.remove(operatorChain.getId());
      // it could be that the onDataAvailable callback was called when the OpChain was executing, in which case there
      // could be a dangling entry in _seenMail.
      _seenMail.remove(operatorChain.getId());
    } finally {
      _lock.unlock();
    }
  }

  @Override
  public void yield(OpChain operatorChain) {
    long releaseTs = _releaseTimeout < 0 ? Long.MAX_VALUE : _ticker.get() + _releaseTimeout;
    _lock.lock();
    try {
      _runningChains.remove(operatorChain.getId());
      // It could be that this OpChain received data before it could be yielded completely. In that case, mark it ready
      // to get it scheduled asap.
      if (_seenMail.contains(operatorChain.getId())) {
        _seenMail.remove(operatorChain.getId());
        _ready.add(operatorChain);
        return;
      }
      _available.put(operatorChain.getId(), releaseTs);
    } finally {
      _lock.unlock();
    }
  }

  @Override
  public void onDataAvailable(MailboxIdentifier mailbox) {
    // TODO: Should we add an API in MailboxIdentifier to get the requestId?
    OpChainId opChainId = new OpChainId(Long.parseLong(mailbox.getJobId().split("_")[0]),
        mailbox.getReceiverStageId());
    // If this chain isn't alive as per the scheduler, don't do anything. If the OpChain is registered after this, it
    // will anyways be scheduled to run since new OpChains are run immediately.
    if (!_aliveChains.containsKey(opChainId)) {
      trace("got mail, but the OpChain is not registered so ignoring the event " + mailbox);
      return;
    }
    _lock.lock();
    try {
      if (!_aliveChains.containsKey(opChainId)) {
        return;
      }
      if (_runningChains.contains(opChainId)) {
        // If the OpChain is running right now, mark it in _seenMail. When there's an attempt to yield the OpChain
        // after it's done running, we'll check against this and mark it ready to run again. If after the current run
        // the OpChain is finished, then we'll clean-up _seenMail in deregister.
        _seenMail.add(opChainId);
        return;
      }
      if (_available.containsKey(opChainId)) {
        _available.remove(opChainId);
        _ready.offer(_aliveChains.get(opChainId));
      }
    } finally {
      _lock.unlock();
    }
    trace("got mail for " + mailbox);
  }

  @Override
  public OpChain next(long time, TimeUnit timeUnit) throws InterruptedException {
    // Poll outside the lock since we don't want to block inside the lock.
    // This is thread-safe anyways since we use a BlockingQueue.
    OpChain op = _ready.poll(time, timeUnit);
    _lock.lock();
    try {
      if (op != null) {
        _runningChains.add(op.getId());
      }
      trace("Polled " + op);
      return op;
    } finally {
      _lock.unlock();
    }
  }

  @Override
  public int size() {
    return _ready.size() + _available.size();
  }

  @Override
  public void shutdownNow() {
    // TODO: Figure out shutdown flow in context of graceful shutdown.
    _availableOpChainReleaseService.shutdownNow();
  }

  private void trace(String operation) {
    LOGGER.trace("({}) Ready: {}, Available: {}, Mail: {}",
        operation, _ready, _available, _seenMail);
  }
}
