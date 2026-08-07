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
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;
import javax.annotation.Nullable;
import org.apache.pinot.core.query.request.ServerQueryRequest;
import org.apache.pinot.core.query.scheduler.fcfs.FCFSSchedulerGroup;
import org.apache.pinot.core.query.scheduler.resources.ResourceManager;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/// Queue to maintain secondary workload queries. Used by the BinaryWorkloadScheduler.
public class SecondaryWorkloadQueue {
  private static final Logger LOGGER = LoggerFactory.getLogger(SecondaryWorkloadQueue.class);
  private static final String SECONDARY_WORKLOAD_GROUP_NAME = "Secondary";

  public static final String SECONDARY_QUEUE_QUERY_TIMEOUT = "binarywlm.secondaryQueueQueryTimeout";
  private static final int DEFAULT_SECONDARY_QUEUE_QUERY_TIMEOUT_SEC = 40;

  public static final String MAX_PENDING_SECONDARY_QUERIES = "binarywlm.maxPendingSecondaryQueries";
  private static final int DEFAULT_MAX_PENDING_SECONDARY_QUERIES = 20;

  public static final String QUEUE_WAKEUP_MS = "binarywlm.queueWakeupMs";
  private static final int DEFAULT_WAKEUP_MS = 1;

  private static int _wakeUpTimeMs;
  private final int _maxPendingPerGroup;

  private final SchedulerGroup _schedulerGroup;

  private final Lock _queueLock = new ReentrantLock();
  private final Condition _queryReaderCondition = _queueLock.newCondition();
  private final ResourceManager _resourceManager;
  private final int _queryDeadlineMs;
  private final Consumer<SchedulerQueryContext> _expiredQueryHandler;

  public SecondaryWorkloadQueue(PinotConfiguration config, ResourceManager resourceManager,
      Consumer<SchedulerQueryContext> expiredQueryHandler) {
    Preconditions.checkNotNull(config);
    Preconditions.checkNotNull(resourceManager);
    Preconditions.checkNotNull(expiredQueryHandler);

    _queryDeadlineMs =
        config.getProperty(SECONDARY_QUEUE_QUERY_TIMEOUT, DEFAULT_SECONDARY_QUEUE_QUERY_TIMEOUT_SEC) * 1000;
    _wakeUpTimeMs = config.getProperty(QUEUE_WAKEUP_MS, DEFAULT_WAKEUP_MS);
    _maxPendingPerGroup = config.getProperty(MAX_PENDING_SECONDARY_QUERIES, DEFAULT_MAX_PENDING_SECONDARY_QUERIES);
    LOGGER.info("queryDeadlineMs={}, wakeupTimeMs={},maxPendingPerGroup={}", _queryDeadlineMs, _wakeUpTimeMs,
        _maxPendingPerGroup);
    _schedulerGroup = new FCFSSchedulerGroup(SECONDARY_WORKLOAD_GROUP_NAME);
    _resourceManager = resourceManager;
    _expiredQueryHandler = expiredQueryHandler;
  }

  /// Adds a query to the secondary workload queue.
  /// @param query
  /// @throws OutOfCapacityException
  public void put(SchedulerQueryContext query)
      throws OutOfCapacityException {
    Preconditions.checkNotNull(query);
    _queueLock.lock();
    try {
      checkSchedulerGroupCapacity(query);
      query.setSchedulerGroupContext(_schedulerGroup);
      _schedulerGroup.addLast(query);
      _queryReaderCondition.signal();
    } finally {
      _queueLock.unlock();
    }
  }

  /// Blocking call to read the next query
  /// @return
  @Nullable
  public SchedulerQueryContext take() {
    while (true) {
      SchedulerQueryContext schedulerQueryContext = null;
      List<SchedulerQueryContext> expired;
      _queueLock.lock();
      try {
        expired = _schedulerGroup.trimExpired(System.currentTimeMillis() - _queryDeadlineMs);
        if (!_schedulerGroup.isEmpty() && _resourceManager.canSchedule(_schedulerGroup)) {
          schedulerQueryContext = _schedulerGroup.removeFirst();
        } else if (expired.isEmpty()) {
          // Nothing to dispatch and nothing to clean up, so wait for a signal.
          try {
            if (_schedulerGroup.isEmpty()) {
              _queryReaderCondition.await(); // woken by put()
            } else {
              _queryReaderCondition.await(_wakeUpTimeMs, TimeUnit.MILLISECONDS); // TTL sweep safety net
            }
          } catch (InterruptedException e) {
            return null;
          }
        }
      } finally {
        _queueLock.unlock();
      }

      // Complete expired queries outside the lock, since the handler writes to the request channel.
      // A failure must not escape: it would drop the query dequeued above and leak the caller's runner permit.
      for (SchedulerQueryContext queryContext : expired) {
        try {
          _expiredQueryHandler.accept(queryContext);
        } catch (Throwable t) {
          LOGGER.error("Failed to complete expired query: {}", queryContext.getQueryRequest().getRequestId(), t);
          // Last resort, so the request always gets a response. No-op if the handler already completed the future.
          queryContext.getResultFuture().setException(t);
        }
      }
      if (schedulerQueryContext != null) {
        if (LOGGER.isDebugEnabled()) {
          ServerQueryRequest queryRequest = schedulerQueryContext.getQueryRequest();
          LOGGER.debug("Scheduling query: {} for group: {} with arrivalTimeMs: {} and numSegments: {}",
              queryRequest.getRequestId(), _schedulerGroup.name(),
              queryRequest.getTimerContext().getQueryArrivalTimeMs(), queryRequest.getSegmentsToQuery().size());
        }
        return schedulerQueryContext;
      }
    }
  }

  public List<SchedulerQueryContext> drain() {
    List<SchedulerQueryContext> pending = new ArrayList<>();
    _queueLock.lock();
    try {
      while (!_schedulerGroup.isEmpty()) {
        pending.add(_schedulerGroup.removeFirst());
      }
    } finally {
      _queueLock.unlock();
    }
    return pending;
  }

  private void checkSchedulerGroupCapacity(SchedulerQueryContext query)
      throws OutOfCapacityException {
    if (_schedulerGroup.numPending() >= _maxPendingPerGroup
        && _schedulerGroup.totalReservedThreads() >= _resourceManager.getTableThreadsHardLimit()) {
      throw new OutOfCapacityException(
          "SchedulerGroup " + _schedulerGroup.name() + " is out of capacity. numPending: "
              + _schedulerGroup.numPending() + ", maxPending: " + _maxPendingPerGroup + ", reservedThreads: "
              + _schedulerGroup.totalReservedThreads() + " threadsHardLimit: "
              + _resourceManager.getTableThreadsHardLimit());
    }
  }

  /// Signals the reader that reserved threads were released, so canSchedule() may now pass.
  public void signalWorkersReleased() {
    _queueLock.lock();
    try {
      _queryReaderCondition.signal();
    } finally {
      _queueLock.unlock();
    }
  }
}
