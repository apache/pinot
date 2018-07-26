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
package com.linkedin.pinot.common.partition;

import com.google.common.collect.Lists;
import com.linkedin.pinot.common.config.TableNameBuilder;
import com.linkedin.pinot.common.utils.LLCSegmentName;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.helix.model.IdealState;

import static com.linkedin.pinot.common.utils.CommonConstants.Helix.StateModel.RealtimeSegmentOnlineOfflineStateModel.*;


/**
 * Builder for ideal state for testing
 */
public class IdealStateBuilderUtil {

  private IdealState _idealState;
  private String _tableName;
  private String _rawTableName;

  public IdealStateBuilderUtil(String tableNameWithType) {
    _tableName = tableNameWithType;
    _rawTableName = TableNameBuilder.extractRawTableName(tableNameWithType);
    _idealState = new IdealState(tableNameWithType);
  }

  public IdealState build() {
    return _idealState;
  }

  public IdealStateBuilderUtil disableIdealState() {
    _idealState.getRecord().setSimpleField(IdealState.IdealStateProperty.HELIX_ENABLED.name(), "false");
    return this;
  }

  public IdealStateBuilderUtil setNumReplicas(int numReplicas) {
    _idealState.setReplicas(String.valueOf(numReplicas));
    return this;
  }

  public String getSegment(int partition, int seqNum) {
    for (String segmentName : _idealState.getRecord().getMapFields().keySet()) {
      if (LLCSegmentName.isLowLevelConsumerSegmentName(segmentName)) {
        LLCSegmentName llcSegmentName = new LLCSegmentName(segmentName);
        if (llcSegmentName.getPartitionId() == partition && llcSegmentName.getSequenceNumber() == seqNum) {
          return segmentName;
        }
      }
    }
    return null;
  }

  public IdealStateBuilderUtil addSegment(String segmentName, Map<String, String> instanceStateMap) {
    _idealState.setInstanceStateMap(segmentName, instanceStateMap);
    return this;
  }

  public IdealStateBuilderUtil addConsumingSegments(int numPartitions, int seqNum, int numReplicas, List<String> instances) {
    int serverId = 0;
    for (int p = 0; p < numPartitions; p++) {
      LLCSegmentName llcSegmentName = new LLCSegmentName(_rawTableName, p, seqNum, System.currentTimeMillis());
      Map<String, String> instanceStateMap = new HashMap<>(numReplicas);
      for (int r = 0; r < numReplicas; r++) {
        instanceStateMap.put(instances.get(serverId++), CONSUMING);
        if (serverId == instances.size()) {
          serverId = 0;
        }
      }
      _idealState.setInstanceStateMap(llcSegmentName.getSegmentName(), instanceStateMap);
    }
    return this;
  }

  public IdealStateBuilderUtil setSegmentState(int partition, int seqNum, String state) {
    for (String segmentName : _idealState.getRecord().getMapFields().keySet()) {
      if (LLCSegmentName.isLowLevelConsumerSegmentName(segmentName)) {
        LLCSegmentName llcSegmentName = new LLCSegmentName(segmentName);
        if (llcSegmentName.getPartitionId() == partition && llcSegmentName.getSequenceNumber() == seqNum) {
          Map<String, String> instanceStateMap = _idealState.getInstanceStateMap(segmentName);
          for (Map.Entry<String, String> entry : instanceStateMap.entrySet()) {
            instanceStateMap.put(entry.getKey(), state);
          }
          break;
        }
      }
    }
    return this;
  }

  public IdealStateBuilderUtil setSegmentState(String segmentName, String state) {
    Map<String, String> instanceStateMap = _idealState.getInstanceStateMap(segmentName);
    for (Map.Entry<String, String> entry : instanceStateMap.entrySet()) {
      instanceStateMap.put(entry.getKey(), state);
    }
    return this;
  }

  public IdealStateBuilderUtil addConsumingSegment(int partition, int seqNum, int numReplicas, List<String> instances) {
    LLCSegmentName llcSegmentName = new LLCSegmentName(_rawTableName, partition, seqNum, System.currentTimeMillis());
    Map<String, String> instanceStateMap = new HashMap<>(numReplicas);
    for (int i = 0; i < numReplicas; i++) {
      instanceStateMap.put(instances.get(i), CONSUMING);
    }
    _idealState.setInstanceStateMap(llcSegmentName.getSegmentName(), instanceStateMap);
    return this;
  }

  public IdealStateBuilderUtil moveToServers(int partition, int seqNum, List<String> instances) {
    for (String segmentName : _idealState.getRecord().getMapFields().keySet()) {
      if (LLCSegmentName.isLowLevelConsumerSegmentName(segmentName)) {
        LLCSegmentName llcSegmentName = new LLCSegmentName(segmentName);
        if (llcSegmentName.getPartitionId() == partition && llcSegmentName.getSequenceNumber() == seqNum) {
          Map<String, String> instanceStateMap = _idealState.getInstanceStateMap(segmentName);
          Map<String, String> newInstanceStateMap = new HashMap<>(instanceStateMap.size());
          int serverId = 0;
          for (Map.Entry<String, String> entry : instanceStateMap.entrySet()) {
            newInstanceStateMap.put(instances.get(serverId ++), entry.getValue());
          }
          _idealState.setInstanceStateMap(llcSegmentName.getSegmentName(), newInstanceStateMap);
          break;
        }
      }
    }
    return this;
  }

  public List<String> getInstances(int partition, int seqNum) {
    List<String> instances = new ArrayList<>();
    for (String segmentName : _idealState.getRecord().getMapFields().keySet()) {
      if (LLCSegmentName.isLowLevelConsumerSegmentName(segmentName)) {
        LLCSegmentName llcSegmentName = new LLCSegmentName(segmentName);
        if (llcSegmentName.getPartitionId() == partition && llcSegmentName.getSequenceNumber() == seqNum) {
          Map<String, String> instanceStateMap = _idealState.getInstanceStateMap(segmentName);
          instances = Lists.newArrayList(instanceStateMap.keySet());
          break;
        }
      }
    }
    return instances;
  }

  public IdealStateBuilderUtil removeSegment(String segmentName) {
    _idealState.getRecord().getMapFields().remove(segmentName);
    return this;
  }

  public IdealStateBuilderUtil clear() {
    _idealState.getRecord().getMapFields().clear();
    return this;
  }

}
