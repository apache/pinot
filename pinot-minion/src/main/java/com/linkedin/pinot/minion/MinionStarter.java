/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
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
package com.linkedin.pinot.minion;

import com.linkedin.pinot.common.utils.CommonConstants;
import com.linkedin.pinot.common.utils.NetUtil;
import com.linkedin.pinot.minion.executor.PinotTaskExecutor;
import com.linkedin.pinot.minion.executor.TaskExecutorRegistry;
import com.linkedin.pinot.minion.taskfactory.TaskFactoryRegistry;
import javax.annotation.Nonnull;
import org.apache.commons.configuration.Configuration;
import org.apache.helix.HelixAdmin;
import org.apache.helix.HelixManager;
import org.apache.helix.InstanceType;
import org.apache.helix.manager.zk.ZKHelixManager;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.task.TaskStateModelFactory;


/**
 * The class <code>MinionStarter</code> provides methods to start and stop the Pinot Minion.
 * <p>Pinot Minion will automatically join the given Helix cluster as a participant.
 */
public class MinionStarter {
  private final String _helixClusterName;
  private final String _minionId;
  private final HelixManager _helixManager;
  private final TaskExecutorRegistry _taskExecutorRegistry;

  private HelixAdmin _helixAdmin;

  public MinionStarter(String zkAddress, String helixClusterName, Configuration config)
      throws Exception {
    _helixClusterName = helixClusterName;
    _minionId = config.getString(CommonConstants.Helix.Instance.INSTANCE_ID_KEY,
        CommonConstants.Helix.PREFIX_OF_MINION_INSTANCE + NetUtil.getHostAddress());
    _helixManager = new ZKHelixManager(_helixClusterName, _minionId, InstanceType.PARTICIPANT, zkAddress);
    _taskExecutorRegistry = new TaskExecutorRegistry();
  }

  /**
   * Register a class of task executor.
   * <p>This is for pluggable task executors.
   *
   * @param taskType Task type
   * @param taskExecutorClass Class of task executor to be registered
   */
  public void registerTaskExecutorClass(@Nonnull String taskType,
      @Nonnull Class<? extends PinotTaskExecutor> taskExecutorClass) {
    _taskExecutorRegistry.registerTaskExecutorClass(taskType, taskExecutorClass);
  }

  /**
   * Start the Pinot Minion instance.
   * <p>Should be called after all classes of task executor get registered.
   */
  public void start()
      throws Exception {
    _helixManager.getStateMachineEngine()
        .registerStateModelFactory("Task", new TaskStateModelFactory(_helixManager,
            new TaskFactoryRegistry(_taskExecutorRegistry).getTaskFactoryRegistry()));
    _helixManager.connect();
    _helixAdmin = _helixManager.getClusterManagmentTool();
    addInstanceTagIfNeeded();
  }

  /**
   * Stop the Pinot Minion instance.
   */
  public void stop() {
    _helixManager.disconnect();
  }

  /**
   * Tag Pinot Minion instance if needed.
   */
  private void addInstanceTagIfNeeded() {
    InstanceConfig instanceConfig = _helixAdmin.getInstanceConfig(_helixClusterName, _minionId);
    if (instanceConfig.getTags().isEmpty()) {
      _helixAdmin.addInstanceTag(_helixClusterName, _minionId, CommonConstants.Helix.UNTAGGED_MINION_INSTANCE);
    }
  }
}
