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
import java.util.Map;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.pinot.spi.config.workload.NodeConfig;
import org.apache.pinot.spi.config.workload.QueryWorkloadConfig;
import org.apache.pinot.spi.utils.JsonUtils;


public class WorkloadConfigUtils {
  private WorkloadConfigUtils() {
  }

  /**
   * Converts a ZNRecord into a QueryWorkloadConfig object by extracting mapFields.
   *
   * @param znRecord The ZNRecord containing workload config data.
   * @return A QueryWorkloadConfig object.
   */
  public static QueryWorkloadConfig fromZNRecord(ZNRecord znRecord) {
    Preconditions.checkNotNull(znRecord, "ZNRecord cannot be null");
    String queryWorkloadName = znRecord.getSimpleField(QueryWorkloadConfig.QUERY_WORKLOAD_NAME);
    Preconditions.checkNotNull(queryWorkloadName, "queryWorkloadName cannot be null");
    String nodeConfigsJson = znRecord.getSimpleField(QueryWorkloadConfig.NODE_CONFIGS);
    Preconditions.checkNotNull(nodeConfigsJson, "nodeConfigs cannot be null");
    try {
      Map<NodeConfig.Type, NodeConfig> nodeConfigs = JsonUtils.stringToObject(nodeConfigsJson, new TypeReference<>() {
      });
      return new QueryWorkloadConfig(queryWorkloadName, nodeConfigs);
    } catch (Exception e) {
      String errorMessage = String.format("Failed to convert ZNRecord : %s to QueryWorkloadConfig", znRecord);
      throw new RuntimeException(errorMessage, e);
    }
  }

  /**
   * Updates a ZNRecord with the fields from a WorkloadConfig object.
   *
   * @param queryWorkloadConfig The QueryWorkloadConfig object to convert.
   * @param znRecord The ZNRecord to update.
   */
  public static void updateZNRecordWithWorkloadConfig(ZNRecord znRecord, QueryWorkloadConfig queryWorkloadConfig) {
    Preconditions.checkNotNull(znRecord, "ZNRecord cannot be null");
    Preconditions.checkNotNull(queryWorkloadConfig, "QueryWorkloadConfig cannot be null");
    Preconditions.checkNotNull(queryWorkloadConfig.getQueryWorkloadName(), "QueryWorkload cannot be null");
    Preconditions.checkNotNull(queryWorkloadConfig.getNodeConfigs(), "NodeConfigs cannot be null");

    znRecord.setSimpleField(QueryWorkloadConfig.QUERY_WORKLOAD_NAME, queryWorkloadConfig.getQueryWorkloadName());
    try {
      znRecord.setSimpleField(QueryWorkloadConfig.NODE_CONFIGS,
          JsonUtils.objectToString(queryWorkloadConfig.getNodeConfigs()));
    } catch (Exception e) {
      String errorMessage = String.format("Failed to convert QueryWorkloadConfig : %s to ZNRecord",
          queryWorkloadConfig);
      throw new RuntimeException(errorMessage, e);
    }
  }
}
