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
package org.apache.pinot.plugin.minion.tasks.mergerollup.segmentgroupmananger;

import java.util.Map;
import org.apache.pinot.core.common.MinionConstants;
import org.apache.pinot.spi.plugin.PluginManager;


/**
 * Provider class for {@link MergeRollupTaskSegmentGroupManager}
 */
public abstract class MergeRollupTaskSegmentGroupManagerProvider {

  private static String _defaultMergeRollupTaskSegmentGroupManagerClassName =
      DefaultMergeRollupTaskSegmentGroupManager.class.getName();

  public static void setDefaultMergeRollupTaskSegmentGroupManagerClassName(String className) {
    _defaultMergeRollupTaskSegmentGroupManagerClassName = className;
  }

  /**
   * Constructs the {@link MergeRollupTaskSegmentGroupManager} using MergeRollup task configs
   */
  public static MergeRollupTaskSegmentGroupManager create(Map<String, String> taskConfigs) {
    String segmentGroupManagerClassName =
        taskConfigs.getOrDefault(MinionConstants.MergeRollupTask.SEGMENT_GROUP_MANAGER_CLASS_NAME_KEY,
            _defaultMergeRollupTaskSegmentGroupManagerClassName);
    try {
      return PluginManager.get().createInstance(segmentGroupManagerClassName);
    } catch (Exception e) {
      throw new RuntimeException("Fail to create segment group manager", e);
    }
  }
}
