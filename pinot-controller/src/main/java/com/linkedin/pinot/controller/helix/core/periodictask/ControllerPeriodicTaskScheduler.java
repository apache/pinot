/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.linkedin.pinot.controller.helix.core.periodictask;

import com.linkedin.pinot.controller.ControllerLeadershipManager;
import com.linkedin.pinot.controller.LeadershipChangeSubscriber;
import com.linkedin.pinot.core.periodictask.PeriodicTask;
import com.linkedin.pinot.core.periodictask.PeriodicTaskScheduler;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * A {@link PeriodicTaskScheduler} for scheduling {@link ControllerPeriodicTask} according to controller leadership changes.
 * Any controllerPeriodicTasks provided during initialization, will run only on leadership, and stop when leadership lost
 */
public class ControllerPeriodicTaskScheduler extends PeriodicTaskScheduler implements LeadershipChangeSubscriber {

  private static final Logger LOGGER = LoggerFactory.getLogger(ControllerPeriodicTaskScheduler.class);

  /**
   * Initialize the {@link ControllerPeriodicTaskScheduler} with the list of {@link ControllerPeriodicTask} created at startup
   * This is called only once during controller startup
   * @param controllerPeriodicTasks
   */
  public void init(List<PeriodicTask> controllerPeriodicTasks) {
    super.init(controllerPeriodicTasks);
    ControllerLeadershipManager.getInstance().subscribe(ControllerPeriodicTaskScheduler.class.getName(), this);
  }

  @Override
  public void onBecomingLeader() {
    LOGGER.info("Received callback for controller leadership gain. Starting PeriodicTaskScheduler.");
    start();
  }

  @Override
  public void onBecomingNonLeader() {
    LOGGER.info("Received callback for controller leadership loss. Stopping PeriodicTaskScheduler.");
    stop();
  }
}
