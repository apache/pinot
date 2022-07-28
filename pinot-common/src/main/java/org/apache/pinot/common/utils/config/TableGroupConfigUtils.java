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
package org.apache.pinot.common.utils.config;

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.base.Preconditions;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.lang.StringUtils;
import org.apache.helix.ZNRecord;
import org.apache.pinot.spi.config.table.TableGroupConfig;
import org.apache.pinot.spi.config.table.assignment.InstanceAssignmentConfig;
import org.apache.pinot.spi.config.table.assignment.InstancePartitionsType;
import org.apache.pinot.spi.utils.JsonUtils;


public class TableGroupConfigUtils {
  private TableGroupConfigUtils() {
  }

  public static ZNRecord toZNRecord(TableGroupConfig tableGroupConfig)
      throws IOException {
    Preconditions.checkArgument(tableGroupConfig != null, "Table group config cannot be null");
    Preconditions.checkArgument(StringUtils.isNotBlank(tableGroupConfig.getGroupName()), "Table group name cannot be "
        + "blank");
    Preconditions.checkArgument(tableGroupConfig.getInstanceAssignmentConfigMap() != null,
        "Instance assignment config map cannot be null for a table-group");
    Preconditions.checkArgument(tableGroupConfig.getInstanceAssignmentConfigMap().size() > 0,
        "Instance assignment config map cannot be empty");
    String groupName = tableGroupConfig.getGroupName();
    ZNRecord znRecord = new ZNRecord(groupName);
    Map<String, String> mapFields = new HashMap<>();
    mapFields.put(TableGroupConfig.ASSIGNMENT_CONFIG_KEY,
        JsonUtils.objectToString(tableGroupConfig.getInstanceAssignmentConfigMap()));
    znRecord.setMapField("config", mapFields);
    znRecord.setBooleanField(TableGroupConfig.IN_SYNC_CONFIG_KEY, tableGroupConfig.getKeepInstancePartitionsInSync());
    return znRecord;
  }

  public static TableGroupConfig fromZNRecord(ZNRecord znRecord)
      throws IOException {
    String groupName = znRecord.getId();
    boolean keepInSync = znRecord.getBooleanField(TableGroupConfig.IN_SYNC_CONFIG_KEY, true);
    Map<String, String> configMap = znRecord.getMapFields().get("config");
    Map<InstancePartitionsType, InstanceAssignmentConfig> instanceAssignmentConfigMap =
        JsonUtils.stringToObject(configMap.get(TableGroupConfig.ASSIGNMENT_CONFIG_KEY),
            new TypeReference<Map<InstancePartitionsType, InstanceAssignmentConfig>>() { });
    return new TableGroupConfig(groupName, keepInSync, instanceAssignmentConfigMap);
  }
}
