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
package org.apache.pinot.controller.helix.core.sharding;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import javax.ws.rs.NotSupportedException;
import org.apache.helix.HelixAdmin;
import org.apache.helix.HelixManager;
import org.apache.helix.ZNRecord;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.pinot.common.config.TagNameUtils;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.common.segment.SegmentMetadata;
import org.apache.pinot.common.utils.helix.HelixHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Assigns a segment to the instance that has same sharding key.
 *
 *
 */
public class BucketizedSegmentStrategy implements SegmentAssignmentStrategy {
  private static final Logger LOGGER = LoggerFactory.getLogger(BucketizedSegmentStrategy.class);

  @Override
  public List<String> getAssignedInstances(HelixManager helixManager, HelixAdmin helixAdmin,
      ZkHelixPropertyStore<ZNRecord> propertyStore, String helixClusterName, SegmentMetadata segmentMetadata,
      int numReplicas, String tenantName) {
    String serverTenantName = TagNameUtils.getOfflineTagForTenant(tenantName);

    List<String> allInstances = HelixHelper.getEnabledInstancesWithTag(helixManager, serverTenantName);
    List<String> selectedInstanceList = new ArrayList<>();
    if (segmentMetadata.getShardingKey() != null) {
      for (String instance : allInstances) {
        if (HelixHelper.getInstanceConfigsMapFor(instance, helixClusterName, helixAdmin).get("shardingKey")
            .equalsIgnoreCase(segmentMetadata.getShardingKey())) {
          selectedInstanceList.add(instance);
        }
      }
      LOGGER.info("Segment assignment result for : " + segmentMetadata.getName() + ", in resource : " + segmentMetadata
          .getTableName() + ", selected instances: " + Arrays.toString(selectedInstanceList.toArray()));
      return selectedInstanceList;
    } else {
      throw new RuntimeException("Segment missing sharding key!");
    }
  }

  @Override
  public List<String> getAssignedInstances(HelixManager helixManager, HelixAdmin helixAdmin,
      ZkHelixPropertyStore<ZNRecord> propertyStore, String helixClusterName, String tableNameWithType,
      SegmentZKMetadata segmentZKMetadata, int numReplicas, String tenantName) {
    throw new UnsupportedOperationException("Not supported segment assignment");
  }
}
