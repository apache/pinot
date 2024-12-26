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

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.tasks.MinionTaskProgressManager;
import org.apache.pinot.spi.tasks.MinionTaskProgressStats;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class DefaultMinionTaskProgressManager implements MinionTaskProgressManager {
  private static final Logger LOGGER = LoggerFactory.getLogger(DefaultMinionTaskProgressManager.class);

  public static final String MAX_NUM_STATUS_TO_TRACK = "pinot.minion.task.maxNumStatusToTrack";
  public static final int DEFAULT_MAX_NUM_STATUS_TO_TRACK = 128;

  private final Map<String, MinionTaskProgressStats> _minionTaskProgressStatsMap = new HashMap<>();
  private int _maxNumStatusToTrack;

  @Override
  public void init(PinotConfiguration configuration) {
    try {
      _maxNumStatusToTrack =
          Integer.parseInt(configuration.getProperty(DefaultMinionTaskProgressManager.MAX_NUM_STATUS_TO_TRACK));
    } catch (NumberFormatException e) {
      LOGGER.warn("Unable to parse the value of '{}' using default value: {}",
          DefaultMinionTaskProgressManager.MAX_NUM_STATUS_TO_TRACK,
          DefaultMinionTaskProgressManager.DEFAULT_MAX_NUM_STATUS_TO_TRACK);
      _maxNumStatusToTrack = DefaultMinionTaskProgressManager.DEFAULT_MAX_NUM_STATUS_TO_TRACK;
    }
  }

  @Override
  public MinionTaskProgressStats getTaskProgress(String taskId) {
    MinionTaskProgressStats progressStats = _minionTaskProgressStatsMap.get(taskId);
    if (progressStats == null) {
      return new MinionTaskProgressStats()
          .setProgressLogs(new LinkedList<>());
    }
    return progressStats;
  }

  @Override
  public synchronized void setTaskProgress(String taskId, MinionTaskProgressStats progress) {
    _minionTaskProgressStatsMap.put(taskId, progress);
    List<MinionTaskProgressStats.StatusEntry> progressLogs = progress.getProgressLogs();
    int logSize = progressLogs.size();
    int overflow = Math.max(logSize - _maxNumStatusToTrack, 0);
    for (int i = 0; i < overflow; i++) {
      progressLogs.removeFirst();
    }
  }
}
