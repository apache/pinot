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

import java.util.Deque;
import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.pinot.minion.MinionConf;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.tasks.MinionTaskBaseObserverStats;
import org.apache.pinot.spi.tasks.MinionTaskObserverStorageManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class DefaultMinionTaskObserverStorageManager implements MinionTaskObserverStorageManager {
  private static final Logger LOGGER = LoggerFactory.getLogger(DefaultMinionTaskObserverStorageManager.class);

  public static final String MAX_NUM_STATUS_TO_TRACK = "pinot.minion.task.maxNumStatusToTrack";
  public static final int DEFAULT_MAX_NUM_STATUS_TO_TRACK = 128;

  private final Map<String, MinionTaskBaseObserverStats> _minionTaskProgressStatsMap = new HashMap<>();
  private int _maxNumStatusToTrack;

  private static final DefaultMinionTaskObserverStorageManager DEFAULT_INSTANCE;
  static {
    DEFAULT_INSTANCE = new DefaultMinionTaskObserverStorageManager();
    DEFAULT_INSTANCE.init(new MinionConf());
  }

  public static DefaultMinionTaskObserverStorageManager getDefaultInstance() {
    return DEFAULT_INSTANCE;
  }

  @Override
  public void init(PinotConfiguration configuration) {
    try {
      _maxNumStatusToTrack =
          Integer.parseInt(configuration.getProperty(DefaultMinionTaskObserverStorageManager.MAX_NUM_STATUS_TO_TRACK));
    } catch (NumberFormatException e) {
      LOGGER.warn("Unable to parse the value of '{}' using default value: {}",
          DefaultMinionTaskObserverStorageManager.MAX_NUM_STATUS_TO_TRACK,
          DefaultMinionTaskObserverStorageManager.DEFAULT_MAX_NUM_STATUS_TO_TRACK);
      _maxNumStatusToTrack = DefaultMinionTaskObserverStorageManager.DEFAULT_MAX_NUM_STATUS_TO_TRACK;
    }
  }

  @Nullable
  @Override
  public MinionTaskBaseObserverStats getTaskProgress(String taskId) {
    if (_minionTaskProgressStatsMap.containsKey(taskId)) {
      return new MinionTaskBaseObserverStats(_minionTaskProgressStatsMap.get(taskId));
    }
    return null;
  }

  @Override
  public synchronized void setTaskProgress(String taskId, MinionTaskBaseObserverStats progress) {
    _minionTaskProgressStatsMap.put(taskId, progress);
    Deque<MinionTaskBaseObserverStats.StatusEntry> progressLogs = progress.getProgressLogs();
    int logSize = progressLogs.size();
    int overflow = Math.max(logSize - _maxNumStatusToTrack, 0);
    if (overflow > 0) {
      progressLogs.pollFirst();
    }
  }

  @Override
  public MinionTaskBaseObserverStats deleteTaskProgress(String taskId) {
    return _minionTaskProgressStatsMap.remove(taskId);
  }
}
