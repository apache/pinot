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
  protected final ConcurrentLinkedQueue<SchedulerQueryContext> pendingQueries = new ConcurrentLinkedQueue<>();
  protected final String name;
  // Tracks number of running queries for this group
  protected AtomicInteger numRunning = new AtomicInteger(0);
  // Total threads in use for this group
  protected AtomicInteger threadsInUse = new AtomicInteger(0);
  // Total reserved threads for currently running queries for this group
  protected AtomicInteger reservedThreads = new AtomicInteger(0);

  public AbstractSchedulerGroup(@Nonnull String name) {
    Preconditions.checkNotNull(name);
    this.name = name;
  }

  @Override
  public String name() {
    return this.name;
  }

  @Override
  public void addLast(SchedulerQueryContext query) {
    pendingQueries.add(query);
  }

  @Override
  public SchedulerQueryContext peekFirst() {
    return pendingQueries.peek();
  }

  @Override
  public SchedulerQueryContext removeFirst() {
    return pendingQueries.poll();
  }

  @Override
  public void trimExpired(long deadlineMillis) {
    Iterator<SchedulerQueryContext> iter = pendingQueries.iterator();
    while (iter.hasNext()) {
      SchedulerQueryContext next = iter.next();
      if (next.getArrivalTimeMs() < deadlineMillis) {
        iter.remove();
      }
    }
  }

  @Override
  public boolean isEmpty() {
    return pendingQueries.isEmpty();
  }

  @Override
  public int numPending() {
    return pendingQueries.size();
  }

  @Override
  public int numRunning() {
    return numRunning.get();
  }

  @Override
  public void incrementThreads() {
    threadsInUse.incrementAndGet();
  }

  @Override
  public void decrementThreads() {
    threadsInUse.decrementAndGet();
  }

  @Override
  public int getThreadsInUse() {
    return threadsInUse.get();
  }

  @Override
  public void addReservedThreads(int threads) {
    reservedThreads.addAndGet(threads);
  }

  @Override
  public void releasedReservedThreads(int threads) {
    reservedThreads.addAndGet(-1 * threads);
  }

  @Override
  public int totalReservedThreads() {
    return reservedThreads.get();
  }

  @Override
  public void startQuery() {
    incrementThreads();
    numRunning.incrementAndGet();
  }

  @Override
  public void endQuery() {
    decrementThreads();
    numRunning.decrementAndGet();
  }
}
