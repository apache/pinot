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
package org.apache.pinot.core.query.scheduler;

import com.google.common.base.Preconditions;
import java.util.Iterator;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import javax.annotation.Nonnull;


/**
 * Abstract {@link SchedulerGroup} class that provides common facilities like
 * managing pending queries as a Linked queue of requests and provides basic accounting
 * by tracking running queries, reserved threads and in-use threads
 */
public abstract class AbstractSchedulerGroup implements SchedulerGroup {
  // Queue of pending queries for this group
  protected final ConcurrentLinkedQueue<SchedulerQueryContext> _pendingQueries = new ConcurrentLinkedQueue<>();
  protected final String _name;
  // Tracks number of running queries for this group
  protected final AtomicInteger _numRunning = new AtomicInteger(0);
  // Total threads in use for this group
  protected final AtomicInteger _threadsInUse = new AtomicInteger(0);
  // Total reserved threads for currently running queries for this group
  protected final AtomicInteger _reservedThreads = new AtomicInteger(0);

  public AbstractSchedulerGroup(@Nonnull String name) {
    Preconditions.checkNotNull(name);
    _name = name;
  }

  @Override
  public String name() {
    return _name;
  }

  @Override
  public void addLast(SchedulerQueryContext query) {
    _pendingQueries.add(query);
  }

  @Override
  public SchedulerQueryContext peekFirst() {
    return _pendingQueries.peek();
  }

  @Override
  public SchedulerQueryContext removeFirst() {
    return _pendingQueries.poll();
  }

  @Override
  public void trimExpired(long deadlineMillis) {
    Iterator<SchedulerQueryContext> iter = _pendingQueries.iterator();
    while (iter.hasNext()) {
      SchedulerQueryContext next = iter.next();
      if (next.getArrivalTimeMs() < deadlineMillis) {
        iter.remove();
      }
    }
  }

  @Override
  public boolean isEmpty() {
    return _pendingQueries.isEmpty();
  }

  @Override
  public int numPending() {
    return _pendingQueries.size();
  }

  @Override
  public int numRunning() {
    return _numRunning.get();
  }

  @Override
  public void incrementThreads() {
    _threadsInUse.incrementAndGet();
  }

  @Override
  public void decrementThreads() {
    _threadsInUse.decrementAndGet();
  }

  @Override
  public int getThreadsInUse() {
    return _threadsInUse.get();
  }

  @Override
  public void addReservedThreads(int threads) {
    _reservedThreads.addAndGet(threads);
  }

  @Override
  public void releasedReservedThreads(int threads) {
    _reservedThreads.addAndGet(-1 * threads);
  }

  @Override
  public int totalReservedThreads() {
    return _reservedThreads.get();
  }

  @Override
  public void startQuery() {
    incrementThreads();
    _numRunning.incrementAndGet();
  }

  @Override
  public void endQuery() {
    decrementThreads();
    _numRunning.decrementAndGet();
  }
}
