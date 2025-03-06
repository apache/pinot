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

import com.google.common.base.Preconditions;
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
  public static QueryWorkloadConfig fromZNRecord(ZNRecord znRecord) throws Exception {
    Preconditions.checkNotNull(znRecord, "ZNRecord cannot be null");
    Preconditions.checkNotNull(znRecord.getId(), "WorkloadId cannot be null");
    Preconditions.checkNotNull(znRecord.getSimpleField(QueryWorkloadConfig.LEAF_NODE), "LeafNode cannot be null");
    Preconditions.checkNotNull(znRecord.getSimpleField(QueryWorkloadConfig.NON_LEAF_NODE),
        "NonLeafNode cannot be null");

    String workloadId = znRecord.getId();

    String leafNodeJsonString = znRecord.getSimpleField(QueryWorkloadConfig.LEAF_NODE);
    NodeConfig leafNode = JsonUtils.stringToObject(leafNodeJsonString, NodeConfig.class);

    String nonLeadNodeJsonString = znRecord.getSimpleField(QueryWorkloadConfig.NON_LEAF_NODE);
    NodeConfig nonLeadNode = JsonUtils.stringToObject(nonLeadNodeJsonString, NodeConfig.class);

    return new QueryWorkloadConfig(workloadId, leafNode, nonLeadNode);
  }

  /**
   * Updates a ZNRecord with the fields from a WorkloadConfig object.
   *
   * @param queryWorkloadConfig The QueryWorkloadConfig object to convert.
   * @param znRecord The ZNRecord to update.
   */
  public static void updateZNRecordWithWorkloadConfig(ZNRecord znRecord, QueryWorkloadConfig queryWorkloadConfig) {
    Preconditions.checkNotNull(queryWorkloadConfig, "QueryWorkloadConfig cannot be null");
    Preconditions.checkNotNull(znRecord, "ZNRecord cannot be null");
    Preconditions.checkNotNull(queryWorkloadConfig.getWorkloadName(), "WorkloadId cannot be null");
    Preconditions.checkNotNull(queryWorkloadConfig.getLeafNode(), "LeafNode cannot be null");
    Preconditions.checkNotNull(queryWorkloadConfig.getNonLeafNode(), "NonLeafNode cannot be null");

    znRecord.setSimpleField(QueryWorkloadConfig.LEAF_NODE, queryWorkloadConfig.getLeafNode().toJsonString());
    znRecord.setSimpleField(QueryWorkloadConfig.NON_LEAF_NODE, queryWorkloadConfig.getNonLeafNode().toJsonString());
  }
}
