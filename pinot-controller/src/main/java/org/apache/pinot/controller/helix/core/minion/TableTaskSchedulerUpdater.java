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

import org.I0Itec.zkclient.IZkDataListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class TableTaskSchedulerUpdater implements IZkDataListener {
  private static final Logger LOGGER = LoggerFactory.getLogger(TableTaskSchedulerUpdater.class);
  private final String _tableWithType;
  private final PinotTaskManager _pinotTaskManager;

  public TableTaskSchedulerUpdater(String tableWithType, PinotTaskManager pinotTaskManager) {
    _tableWithType = tableWithType;
    _pinotTaskManager = pinotTaskManager;
  }

  @Override
  public void handleDataChange(String dataPath, Object data)
      throws Exception {
    try {
      _pinotTaskManager.updateCronTaskScheduler(_tableWithType);
    } catch (Exception e) {
      LOGGER.error("Failed to update cron task scheduler for table {}", _tableWithType, e);
      throw e;
    }
  }

  @Override
  public void handleDataDeleted(String dataPath)
      throws Exception {
    try {
      _pinotTaskManager.cleanUpCronTaskSchedulerForTable(_tableWithType);
    } catch (Exception e) {
      LOGGER.error("Failed to delete cron task scheduler for table {}", _tableWithType, e);
      throw e;
    }
  }
}
