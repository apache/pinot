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
package org.apache.pinot.controller.helix.core.minion;

import org.apache.helix.zookeeper.zkclient.IZkDataListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class TaskTypeMetricsUpdater implements IZkDataListener {
  private static final Logger LOGGER = LoggerFactory.getLogger(TaskTypeMetricsUpdater.class);
  private final String _taskType;
  private final PinotTaskManager _pinotTaskManager;

  public TaskTypeMetricsUpdater(String taskType, PinotTaskManager pinotTaskManager) {
    _taskType = taskType;
    _pinotTaskManager = pinotTaskManager;
  }

  @Override
  public void handleDataChange(String dataPath, Object data) {
    updateMetrics();
  }

  @Override
  public void handleDataDeleted(String dataPath) {
    updateMetrics();
  }

  private void updateMetrics() {
    try {
      _pinotTaskManager.reportMetrics(_taskType);
    } catch (Exception e) {
      LOGGER.error("Failed to update metrics for task type {}", _taskType, e);
      throw e;
    }
  }
}
