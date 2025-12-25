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

import java.util.Map;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Global runtime throttling controller for query runner concurrency.
 *
 * This class exposes a dynamic permit gate that schedulers can consult to
 * limit the number of in-flight queries. The gate can be adjusted at runtime
 * via cluster configs or by accounting triggers (alarm/critical/panic).
 *
 * Keys (under pinot.query.scheduler.* cluster config):
 * - throttling.pause_on_alarm: boolean (default false)
 * - throttling.alarm_max_concurrent: int (default 1)
 * - throttling.normal_max_concurrent: int (default = defaultConcurrency)
 */
public final class ThrottlingRuntime {
  private static final Logger LOGGER = LoggerFactory.getLogger(ThrottlingRuntime.class);

  private static final AtomicInteger DEFAULT_CONCURRENCY = new AtomicInteger(0);
  private static final AtomicInteger NORMAL_MAX_CONCURRENT = new AtomicInteger(0);
  private static final AtomicInteger ALARM_MAX_CONCURRENT = new AtomicInteger(1);
  private static final AtomicBoolean PAUSE_ON_ALARM = new AtomicBoolean(false);

  private static final ConcurrencyController CONTROLLER = new ConcurrencyController();

  private ThrottlingRuntime() {
  }

  public static void setDefaultConcurrency(int defaultConcurrency) {
    if (defaultConcurrency <= 0) {
      return;
    }
    if (DEFAULT_CONCURRENCY.get() == 0) {
      DEFAULT_CONCURRENCY.set(defaultConcurrency);
      if (NORMAL_MAX_CONCURRENT.get() == 0) {
        NORMAL_MAX_CONCURRENT.set(defaultConcurrency);
      }
      CONTROLLER.updateLimit(NORMAL_MAX_CONCURRENT.get());
      LOGGER.info("Initialized default scheduler concurrency: {}", defaultConcurrency);
    }
  }

  public static void applyClusterConfig(Map<String, String> schedulerScopedConfigs) {
    // Expect keys without the common prefix (already stripped by caller)
    // throttling.pause_on_alarm
    if (schedulerScopedConfigs.containsKey("throttling.pause_on_alarm")) {
      boolean pause = Boolean.parseBoolean(schedulerScopedConfigs.get("throttling.pause_on_alarm"));
      PAUSE_ON_ALARM.set(pause);
      LOGGER.info("Updated throttling.pause_on_alarm -> {}", pause);
    }
    // throttling.alarm_max_concurrent
    if (schedulerScopedConfigs.containsKey("throttling.alarm_max_concurrent")) {
      try {
        int v = Integer.parseInt(schedulerScopedConfigs.get("throttling.alarm_max_concurrent"));
        if (v > 0) {
          ALARM_MAX_CONCURRENT.set(v);
          LOGGER.info("Updated throttling.alarm_max_concurrent -> {}", v);
        }
      } catch (Exception e) {
        LOGGER.warn("Invalid throttling.alarm_max_concurrent: {}",
            schedulerScopedConfigs.get("throttling.alarm_max_concurrent"), e);
      }
    }
    // throttling.normal_max_concurrent
    if (schedulerScopedConfigs.containsKey("throttling.normal_max_concurrent")) {
      try {
        int v = Integer.parseInt(schedulerScopedConfigs.get("throttling.normal_max_concurrent"));
        if (v > 0) {
          NORMAL_MAX_CONCURRENT.set(v);
          LOGGER.info("Updated throttling.normal_max_concurrent -> {}", v);
        }
      } catch (Exception e) {
        LOGGER.warn("Invalid throttling.normal_max_concurrent: {}",
            schedulerScopedConfigs.get("throttling.normal_max_concurrent"), e);
      }
    }
  }

  public static void onLevelChange(String levelName) {
    // Normalize
    String level = levelName == null ? "" : levelName.toUpperCase();
    switch (level) {
      case "HEAPMEMORYALARMINGVERBOSE":
        if (PAUSE_ON_ALARM.get()) {
          CONTROLLER.updateLimit(Math.max(1, ALARM_MAX_CONCURRENT.get()));
          LOGGER.debug("Alarm level detected, pausing concurrency to {}", ALARM_MAX_CONCURRENT.get());
        }
        break;
      case "NORMAL":
        // restore
        CONTROLLER.updateLimit(Math.max(1, NORMAL_MAX_CONCURRENT.get() > 0 ? NORMAL_MAX_CONCURRENT.get()
            : DEFAULT_CONCURRENCY.get()));
        LOGGER.debug("Restored concurrency to normal: {}", NORMAL_MAX_CONCURRENT.get());
        break;
      case "HEAPMEMORYCRITICAL":
      case "HEAPMEMORYPANIC":
      case "CPUTIMEBASEDKILLING":
      default:
        // Leave concurrency as-is; killing strategies already act elsewhere
        break;
    }
  }

  public static void acquireSchedulerPermit() {
    CONTROLLER.acquire();
  }

  public static void releaseSchedulerPermit() {
    CONTROLLER.release();
  }

  public static boolean isPauseOnAlarm() {
    return PAUSE_ON_ALARM.get();
  }

  public static int getAlarmMaxConcurrent() {
    return ALARM_MAX_CONCURRENT.get();
  }

  public static int getNormalMaxConcurrent() {
    return NORMAL_MAX_CONCURRENT.get();
  }

  public static int getDefaultConcurrency() {
    return DEFAULT_CONCURRENCY.get();
  }

  public static int getCurrentLimit() {
    return CONTROLLER.currentLimit();
  }

  public static void setCurrentLimit(int newLimit) {
    CONTROLLER.updateLimit(newLimit);
  }

  private static final class ConcurrencyController {
    private final Semaphore _semaphore = new Semaphore(0);
    private final AtomicInteger _limit = new AtomicInteger(0);

    void updateLimit(int newLimit) {
      if (newLimit <= 0) {
        return;
      }
      int old = _limit.getAndSet(newLimit);
      int delta = newLimit - old;
      if (old == 0) {
        // first init
        _semaphore.release(newLimit);
        return;
      }
      if (delta > 0) {
        _semaphore.release(delta);
      } else if (delta < 0) {
        // Reduce available permits by acquiring the extra permits. This will block until enough are released.
        _semaphore.acquireUninterruptibly(-delta);
      }
    }

    void acquire() {
      _semaphore.acquireUninterruptibly(1);
    }

    void release() {
      _semaphore.release(1);
    }

    int currentLimit() {
      return _limit.get();
    }
  }
}
