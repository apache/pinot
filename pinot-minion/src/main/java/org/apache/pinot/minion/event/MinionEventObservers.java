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
package org.apache.pinot.minion.event;

import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;
import org.apache.pinot.minion.MinionConf;
import org.apache.pinot.spi.utils.CommonConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class MinionEventObservers {
  private static final Logger LOGGER = LoggerFactory.getLogger(MinionEventObservers.class);

  private static final MinionEventObservers DEFAULT_INSTANCE = new MinionEventObservers();
  private static volatile MinionEventObservers _customInstance = null;

  private final Map<String, MinionEventObserver> _taskEventObservers = new ConcurrentHashMap<>();
  // Tasks are added roughly in time order, so use LinkedList instead of PriorityQueue for simplicity.
  private final Queue<EndedTask> _endedTaskQueue = new LinkedList<>();
  private final ReentrantLock _queueLock = new ReentrantLock();
  private final Condition _availableToClean = _queueLock.newCondition();

  private final long _eventObserverCleanupDelayMs;
  private final ExecutorService _cleanupExecutor;

  private MinionEventObservers() {
    this(0, null);
  }

  private MinionEventObservers(long eventObserverCleanupDelayInSec, ExecutorService executorService) {
    _eventObserverCleanupDelayMs = eventObserverCleanupDelayInSec * 1000;
    _cleanupExecutor = executorService;
    startCleanup();
  }

  private void startCleanup() {
    if (_cleanupExecutor == null || _eventObserverCleanupDelayMs == 0) {
      LOGGER.info("Configured to clean up task event observers immediately");
      return;
    }
    LOGGER.info("Configured to clean up task event observers with cleanupDelayMs: {}", _eventObserverCleanupDelayMs);
    _cleanupExecutor.submit(() -> {
      LOGGER.info("Start to cleanup task event observers with cleanupDelayMs: {}", _eventObserverCleanupDelayMs);
      while (!Thread.interrupted()) {
        _queueLock.lock();
        try {
          EndedTask task = _endedTaskQueue.peek();
          if (task == null) {
            _availableToClean.await();
          } else {
            long nowMs = System.currentTimeMillis();
            if (task._endTs + _eventObserverCleanupDelayMs <= nowMs) {
              LOGGER.info("Cleaning up event observer for task: {} that ended at: {}, now at: {} after delay: {}",
                  task._taskId, task._endTs, nowMs, _eventObserverCleanupDelayMs);
              _taskEventObservers.remove(task._taskId);
              _endedTaskQueue.poll();
            } else {
              _availableToClean.await(task._endTs + _eventObserverCleanupDelayMs - nowMs, TimeUnit.MILLISECONDS);
            }
          }
        } finally {
          _queueLock.unlock();
        }
      }
      LOGGER.info("Stop to cleanup task event observers");
      return null;
    });
  }

  public static void init(MinionConf config, ExecutorService executorService) {
    // Default delay is set to 0 so that task's observer object is cleaned immediately upon task ends.
    _customInstance = new MinionEventObservers(
        config.getProperty(CommonConstants.Minion.CONFIG_OF_EVENT_OBSERVER_CLEANUP_DELAY_IN_SEC, 0), executorService);
  }

  public static MinionEventObservers getInstance() {
    if (_customInstance != null) {
      return _customInstance;
    }
    // Test code might reach here, but this should never happen in prod case, as instance is created upon worker
    // starts before any tasks can run. But log something for debugging just in case.
    LOGGER.warn("Using default MinionEventObservers instance");
    return DEFAULT_INSTANCE;
  }

  public MinionEventObserver getMinionEventObserver(String taskId) {
    return _taskEventObservers.get(taskId);
  }

  /**
   * Gets all {@link MinionEventObserver}s
   * @return a map of subtask ID to {@link MinionEventObserver}
   */
  public Map<String, MinionEventObserver> getMinionEventObservers() {
    return _taskEventObservers;
  }

  /**
   * Gets all {@link MinionEventObserver}s with the given {@link MinionTaskState}
   * @param taskState the {@link MinionTaskState} to match
   * @return a map of subtask ID to {@link MinionEventObserver}
   */
  public Map<String, MinionEventObserver> getMinionEventObserverWithGivenState(MinionTaskState taskState) {
    return _taskEventObservers.entrySet().stream()
        .filter(e -> e.getValue().getTaskState() == taskState)
        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
  }

  public void addMinionEventObserver(String taskId, MinionEventObserver eventObserver) {
    LOGGER.debug("Keep track of event observer for task: {}", taskId);
    _taskEventObservers.put(taskId, eventObserver);
  }

  public void removeMinionEventObserver(String taskId) {
    if (_eventObserverCleanupDelayMs <= 0) {
      LOGGER.debug("Clean up event observer for task: {} immediately", taskId);
      _taskEventObservers.remove(taskId);
      return;
    }
    LOGGER.debug("Clean up event observer for task: {} after delay: {}", taskId, _eventObserverCleanupDelayMs);
    _queueLock.lock();
    try {
      _endedTaskQueue.offer(new EndedTask(taskId, System.currentTimeMillis()));
      _availableToClean.signalAll();
    } finally {
      _queueLock.unlock();
    }
  }

  private static class EndedTask {
    private final String _taskId;
    private final long _endTs;

    public EndedTask(String taskId, long ts) {
      _taskId = taskId;
      _endTs = ts;
    }
  }
}
