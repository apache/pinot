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
package org.apache.pinot.broker.routing.instanceselector;

import com.google.common.base.Preconditions;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.pinot.broker.routing.adaptiveserverselector.AdaptiveServerSelector;
import org.apache.pinot.common.assignment.InstancePartitions;
import org.apache.pinot.common.assignment.InstancePartitionsUtils;
import org.apache.pinot.common.metrics.BrokerMetrics;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.config.table.assignment.InstancePartitionsType;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;


public class MultiStageReplicaGroupSelector extends BaseInstanceSelector {
  private String _tableNameWithType;
  private ZkHelixPropertyStore<ZNRecord> _propertyStore;
  private InstancePartitions _instancePartitions;

  public MultiStageReplicaGroupSelector(String tableNameWithType, BrokerMetrics brokerMetrics, @Nullable
      AdaptiveServerSelector adaptiveServerSelector, ZkHelixPropertyStore<ZNRecord> propertyStore) {
    super(tableNameWithType, brokerMetrics, adaptiveServerSelector);
    _tableNameWithType = tableNameWithType;
    _propertyStore = propertyStore;
  }

  @Override
  public void init(Set<String> enabledInstances, IdealState idealState, ExternalView externalView,
      Set<String> onlineSegments) {
    super.init(enabledInstances, idealState, externalView, onlineSegments);
    _instancePartitions = getInstancePartitions();
  }

  @Override
  public void onInstancesChange(Set<String> enabledInstances, List<String> changedInstances) {
    super.onInstancesChange(enabledInstances, changedInstances);
    _instancePartitions = getInstancePartitions();
  }

  @Override
  public void onAssignmentChange(IdealState idealState, ExternalView externalView, Set<String> onlineSegments) {
    super.onAssignmentChange(idealState, externalView, onlineSegments);
    _instancePartitions = getInstancePartitions();
  }

  @Override
  Map<String, String> select(List<String> segments, long requestId,
      Map<String, List<String>> segmentToEnabledInstancesMap, Map<String, String> queryOptions) {
    // TODO: Fix race condition in _instancePartitions.
    int replicaGroupSelected = (int) requestId % _instancePartitions.getNumReplicaGroups();
    for (int iteration = 0; iteration < _instancePartitions.getNumReplicaGroups(); iteration++) {
      int replicaGroup = (replicaGroupSelected + iteration) % _instancePartitions.getNumReplicaGroups();
      try {
        return tryAssigning(segmentToEnabledInstancesMap, _instancePartitions, replicaGroup);
      } catch (Exception ignored) {
      }
    }
    throw new RuntimeException("Unable to find any instance");
  }

  private Map<String, String> tryAssigning(Map<String, List<String>> segmentToEnabledInstancesMap,
      InstancePartitions instancePartitions, int replicaId) {
    Set<String> instanceLookUpSet = new HashSet<>();
    for (int partition = 0; partition < instancePartitions.getNumPartitions(); partition++) {
      List<String> instances = instancePartitions.getInstances(partition, replicaId);
      instanceLookUpSet.addAll(instances);
    }
    Map<String, String> result = new HashMap<>();
    for (Map.Entry<String, List<String>> entry : segmentToEnabledInstancesMap.entrySet()) {
      String segmentName = entry.getKey();
      boolean found = false;
      for (String enabledInstanceForSegment : entry.getValue()) {
        if (instanceLookUpSet.contains(enabledInstanceForSegment)) {
          found = true;
          result.put(segmentName, enabledInstanceForSegment);
          break;
        }
      }
      if (!found) {
        throw new RuntimeException("");
      }
    }
    return result;
  }

  private InstancePartitions getInstancePartitions() {
    TableType tableType = TableNameBuilder.getTableTypeFromTableName(_tableNameWithType);
    Preconditions.checkNotNull(tableType);
    InstancePartitions instancePartitions = null;
    if (tableType.equals(TableType.OFFLINE)) {
      instancePartitions = InstancePartitionsUtils.fetchInstancePartitions(_propertyStore,
          InstancePartitionsUtils.getInstancePartitionsName(_tableNameWithType, tableType.name()));
    } else {
      instancePartitions = InstancePartitionsUtils.fetchInstancePartitions(_propertyStore,
          InstancePartitionsUtils.getInstancePartitionsName(_tableNameWithType,
              InstancePartitionsType.CONSUMING.name()));
    }
    Preconditions.checkNotNull(instancePartitions);
    return instancePartitions;
  }
}
