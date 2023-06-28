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
import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;
import java.util.ArrayList;
import java.util.List;
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
import org.apache.pinot.query.runtime.operator.OpChain;
import org.apache.pinot.query.runtime.operator.OpChainId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * This is a scheduler that schedules operator chains in round-robin fashion, but will only schedule them when there is
 * work to be done. The availability of work is signaled using the {@link #onDataAvailable} callback.
 * <p>
 *   Design: There are 3 states for a OpChain:
 *
 *   1. Ready: This state means that an OpChain is ready to be run. If an OpChain is in this state, then it would be in
 *             the _ready queue. The _ready queue is polled on every call to {@link #next}.
 *   2. Available/Yielded: This is when the OpChain was run and it returned a no-op block, indicating that it has no
 *                         new data to process. The {@link OpChainId} for these OpChains are stored in the _available
 *                         map, where the value of the map is the release-timeout.
 *   3. Running: When the OpChain is returned by the {@link #next} method, it is considered to be in the Running state.
 *
 *   The following state transitions are possible:
 *   1. Ready ==> Running: This happens when the OpChain is returned by the {@link #next} method.
 *   2. Running ==> Available/Yielded: This happens when a running OpChain returns a No-op block, following which a
 *                                     yield is called and there's no entry for the OpChain in _seenMail.
 *   3. Available/Yielded ==> Ready: This can happen in two cases: (1) When yield is called but _seenMail has an
 *                                   entry for the corresponding OpChainId, which means there was some data received
 *                                   by MailboxReceiveOperator after the last poll. (2) When a sender has died or hasn't
 *                                   sent any data in the last _releaseTimeoutMs milliseconds.
 *
 *                      |--------------( #yield() )---------|
 *                      |                                   |
 *                     \/                                   |
 *   [ START ] --> [ READY ] -----------( #next() )--> [ RUNNING ]
 *                     /\                                   |
 *                     |                                    |
 *         ( #seenMail() or #periodic())                    |
 *                     |                                    |
 *                [ AVAILABLE ] <------( #yield() )---------|
 *                                                          |
 *                                                          |
 *   [ EXIT ] <-----------------( #deregistered() )---------|
 *
 *   The OpChain is considered "alive" from the time it is registered until it is de-registered. Any reference to the
 *   OpChain or its related metadata is kept only while the OpChain is alive. The {@link #onDataAvailable} callback
 *   can be called before an OpChain was ever registered. In that case, this scheduler will simply ignore the callback,
 *   since once the OpChain gets registered it will anyways be put into the _ready queue immediately. In case the
 *   OpChain never gets registered (e.g. if the broker couldn't dispatch it), as long as the sender cleans up all
 *   resources that it has acquired, there will be no leak, since the scheduler doesn't hold any references for
 *   non-alive OpChains.
 * </p>
 */
@ThreadSafe
public class RoundRobinScheduler implements OpChainScheduler {
  private static final Logger LOGGER = LoggerFactory.getLogger(RoundRobinScheduler.class);
  private static final String AVAILABLE_RELEASE_THREAD_NAME = "round-robin-scheduler-release-thread";

  private final long _releaseTimeoutMs;
  private final Supplier<Long> _ticker;

  private final Map<OpChainId, OpChain> _aliveChains = new ConcurrentHashMap<>();
  private final Set<OpChainId> _seenMail = Sets.newConcurrentHashSet();
  private final Map<OpChainId, Long> _available = new ConcurrentHashMap<>();

  private final BlockingQueue<OpChain> _ready = new LinkedBlockingQueue<>();

  private final Lock _lock = new ReentrantLock();
  private final ScheduledExecutorService _availableOpChainReleaseService;

  public RoundRobinScheduler(long releaseTimeoutMs) {
    this(releaseTimeoutMs, System::currentTimeMillis);
  }

  RoundRobinScheduler(long releaseTimeoutMs, Supplier<Long> ticker) {
    Preconditions.checkArgument(releaseTimeoutMs > 0, "Release timeout for round-robin scheduler should be > 0ms");
    _releaseTimeoutMs = releaseTimeoutMs;
    _ticker = ticker;
    _availableOpChainReleaseService = Executors.newSingleThreadScheduledExecutor(r -> {
      Thread t = new Thread(r);
      t.setName(AVAILABLE_RELEASE_THREAD_NAME);
      t.setDaemon(true);
      return t;
    });
    _availableOpChainReleaseService.scheduleAtFixedRate(() -> {
      List<OpChainId> timedOutWaiting = new ArrayList<>();
      for (Map.Entry<OpChainId, Long> entry : _available.entrySet()) {
        if (Thread.interrupted()) {
          LOGGER.warn("Thread={} interrupted. Scheduler may be shutting down.", AVAILABLE_RELEASE_THREAD_NAME);
          break;
        }
        if (_ticker.get() > entry.getValue()) {
          timedOutWaiting.add(entry.getKey());
        }
      }
      for (OpChainId opChainId : timedOutWaiting) {
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
    }, _releaseTimeoutMs, _releaseTimeoutMs, TimeUnit.MILLISECONDS);
  }

  @Override
  public void register(OpChain operatorChain) {
    Preconditions.checkState(!_aliveChains.containsKey(operatorChain.getId()),
        String.format("Tried to re-register op-chain: %s", operatorChain.getId()));
    _lock.lock();
    try {
      _aliveChains.put(operatorChain.getId(), operatorChain);
      _ready.offer(operatorChain);
    } finally {
      _lock.unlock();
    }
    trace("registered " + operatorChain);
  }

  @Override
  public void deregister(OpChain operatorChain) {
    Preconditions.checkState(_aliveChains.containsKey(operatorChain.getId()),
        "Tried to de-register an un-registered op-chain");
    _lock.lock();
    try {
      OpChainId chainId = operatorChain.getId();
      _aliveChains.remove(chainId);
      // it could be that the onDataAvailable callback was called when the OpChain was executing, in which case there
      // could be a dangling entry in _seenMail.
      _seenMail.remove(chainId);
    } finally {
      _lock.unlock();
    }
    // invoke opChain deregister callback
    operatorChain.getOpChainFinishCallback().accept(operatorChain.getId());
  }

  @Override
  public void yield(OpChain operatorChain) {
    long releaseTs = _ticker.get() + _releaseTimeoutMs;
    _lock.lock();
    try {
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
  public void onDataAvailable(OpChainId opChainId) {
    // If this chain isn't alive as per the scheduler, don't do anything. If the OpChain is registered after this, it
    // will anyways be scheduled to run since new OpChains are run immediately.
    if (!_aliveChains.containsKey(opChainId)) {
      trace("woken up but the OpChain is not registered so ignoring the event: " + opChainId);
      return;
    }
    _lock.lock();
    try {
      if (!_aliveChains.containsKey(opChainId)) {
        return;
      }
      if (_available.containsKey(opChainId)) {
        _available.remove(opChainId);
        _ready.offer(_aliveChains.get(opChainId));
      } else {
        // There are two cases here:
        // 1. OpChain is in the _ready queue: the next time it gets polled, we'll remove the _seenMail entry.
        // 2. OpChain is running: the next time yield is called for it, we'll check against _seenMail and put it back
        //    in the _ready queue again.
        _seenMail.add(opChainId);
      }
    } finally {
      _lock.unlock();
    }
    trace("got data for " + opChainId);
  }

  @Override
  public OpChain next(long time, TimeUnit timeUnit)
      throws InterruptedException {
    return _ready.poll(time, timeUnit);
  }

  @Override
  public int size() {
    return _aliveChains.size();
  }

  @Override
  public void shutdownNow() {
    // TODO: Figure out shutdown flow in context of graceful shutdown.
    _availableOpChainReleaseService.shutdownNow();
  }

  @VisibleForTesting
  int readySize() {
    return _ready.size();
  }

  @VisibleForTesting
  int availableSize() {
    return _available.size();
  }

  @VisibleForTesting
  int seenMailSize() {
    return _seenMail.size();
  }

  @VisibleForTesting
  int aliveChainsSize() {
    return _aliveChains.size();
  }

  private void trace(String operation) {
    LOGGER.trace("({}) Ready: {}, Available: {}, Mail: {}", operation, _ready, _available, _seenMail);
  }
}
