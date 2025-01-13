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
package org.apache.pinot.minion;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import org.apache.pinot.core.minion.PinotTaskConfig;
import org.apache.pinot.minion.event.DefaultMinionTaskProgressManager;
import org.apache.pinot.minion.event.MinionProgressObserver;
import org.apache.pinot.spi.tasks.MinionTaskProgressManager;


public class MinionTestUtils {

  private MinionTestUtils() {
  }

  public static MinionProgressObserver getMinionProgressObserver() {
    MinionProgressObserver progressObserver = new MinionProgressObserver();
    progressObserver.init(DefaultMinionTaskProgressManager.getDefaultInstance());
    return progressObserver;
  }

  public static MinionProgressObserver getMinionProgressObserver(int progressLimit) {
    MinionProgressObserver progressObserver = new MinionProgressObserver();
    MinionConf conf = new MinionConf();
    conf.setProperty(DefaultMinionTaskProgressManager.MAX_NUM_STATUS_TO_TRACK, progressLimit);
    MinionTaskProgressManager progressManager = new DefaultMinionTaskProgressManager();
    progressManager.init(conf);
    progressObserver.init(progressManager);
    return progressObserver;
  }

  public static PinotTaskConfig getPinotTaskConfig(String taskId) {
    Map<String, String> taskConfigs = new HashMap<>();
    taskConfigs.put("TASK_ID", taskId != null ? taskId : UUID.randomUUID().toString());
    return new PinotTaskConfig("DUMMY_TASK", taskConfigs);
  }
}
