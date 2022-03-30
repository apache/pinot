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
package org.apache.pinot.common.assignment;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import org.apache.helix.ZNRecord;
import org.apache.pinot.spi.utils.JsonUtils;


/**
 * Instance partitions for the table.
 *
 * <p>The instance partitions is stored as a map from partition of the format: {@code <partitionId>_<replicaGroupId>} to
 * list of instances, and is persisted under the ZK path: {@code <cluster>/PROPERTYSTORE/INSTANCE_PARTITIONS}.
 * <ul>
 *   <li>
 *     Partition: a set of instances that contains the segments of the same partition (value partition for offline
 *     table, or stream partition for real-time table)
 *     <p>NOTE: For real-time table CONSUMING instance partitions, partition cannot be explicitly configured (number of
 *     partitions must be 1), but has to be derived from the index of the instance. Each instance will contain all the
 *     segments from a stream partition before them getting relocated to the COMPLETED instance partitions. The stream
 *     partitions will be evenly spread over all instances (within each replica-group if replica-group is configured).
 *     <p>TODO: Support explicit partition configuration for CONSUMING instance partitions
 *   </li>
 *   <li>
 *     Replica-group: a set of instances that contains one replica of all the segments
 *   </li>
 * </ul>
 * <p>The instance partitions name is of the format {@code <rawTableName>_<instancePartitionsType>}, e.g.
 * {@code table_OFFLINE}, {@code table_CONSUMING}, {@code table_COMPLETED}.
 * <p>When partition is not enabled, all instances will be stored as partition 0.
 * <p>When replica-group is not enabled, all instances will be stored as replica-group 0.
 * <p>The segment assignment will be based on the instance partitions of the table.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class InstancePartitions {
  private static final char PARTITION_REPLICA_GROUP_SEPARATOR = '_';
  private static final String POOLS_KEY = "pools";
  private static final String INSTANCE_SEPARATOR = "/";

  private final String _instancePartitionsName;
  // A map to store the partition and its associated list of instances.
  // The partition key would be like "0_0", where the 1st number denotes the partition id,
  // and the 2nd one denotes the replica group id.
  private final Map<String, List<String>> _partitionToInstancesMap;
  // A map to store the selected pool numbers and their associated lists of instances.
  private final Map<Integer, List<String>> _poolToInstancesMap;
  private int _numPartitions;
  private int _numReplicaGroups;

  public InstancePartitions(String instancePartitionsName) {
    _instancePartitionsName = instancePartitionsName;
    _partitionToInstancesMap = new TreeMap<>();
    _poolToInstancesMap = new TreeMap<>();
  }

  @JsonCreator
  private InstancePartitions(
      @JsonProperty(value = "instancePartitionsName", required = true) String instancePartitionsName,
      @JsonProperty(value = "partitionToInstancesMap", required = true)
          Map<String, List<String>> partitionToInstancesMap,
      @JsonProperty(value = "poolToInstancesMap") Map<String, String> poolToInstancesMap) {
    _instancePartitionsName = instancePartitionsName;
    _partitionToInstancesMap = partitionToInstancesMap;
    _poolToInstancesMap = new TreeMap<>();
    if (poolToInstancesMap != null) {
      for (Map.Entry<String, String> entry : poolToInstancesMap.entrySet()) {
        _poolToInstancesMap.put(Integer.parseInt(entry.getKey()), extractInstances(entry.getValue()));
      }
    }
    for (String key : partitionToInstancesMap.keySet()) {
      int separatorIndex = key.indexOf(PARTITION_REPLICA_GROUP_SEPARATOR);
      int partitionId = Integer.parseInt(key.substring(0, separatorIndex));
      int replicaGroupId = Integer.parseInt(key.substring(separatorIndex + 1));
      _numPartitions = Integer.max(_numPartitions, partitionId + 1);
      _numReplicaGroups = Integer.max(_numReplicaGroups, replicaGroupId + 1);
    }
  }

  @JsonProperty
  public String getInstancePartitionsName() {
    return _instancePartitionsName;
  }

  @JsonProperty
  public Map<String, List<String>> getPartitionToInstancesMap() {
    return _partitionToInstancesMap;
  }

  @JsonIgnore
  public int getNumPartitions() {
    return _numPartitions;
  }

  @JsonIgnore
  public int getNumReplicaGroups() {
    return _numReplicaGroups;
  }

  @JsonIgnore
  public Map<Integer, List<String>> getPoolToInstancesMap() {
    return _poolToInstancesMap;
  }

  public List<String> getInstances(int partitionId, int replicaGroupId) {
    return _partitionToInstancesMap
        .get(Integer.toString(partitionId) + PARTITION_REPLICA_GROUP_SEPARATOR + replicaGroupId);
  }

  public void setInstances(int partitionId, int replicaGroupId, List<String> instances) {
    String key = Integer.toString(partitionId) + PARTITION_REPLICA_GROUP_SEPARATOR + replicaGroupId;
    _partitionToInstancesMap.put(key, instances);
    _numPartitions = Integer.max(_numPartitions, partitionId + 1);
    _numReplicaGroups = Integer.max(_numReplicaGroups, replicaGroupId + 1);
  }

  public void setPoolToInstancesMap(Map<Integer, List<String>> poolToInstancesMap) {
    _poolToInstancesMap.putAll(poolToInstancesMap);
  }

  public static InstancePartitions fromZNRecord(ZNRecord znRecord) {
    return new InstancePartitions(znRecord.getId(), znRecord.getListFields(), znRecord.getMapField(POOLS_KEY));
  }

  private static List<String> extractInstances(String instancesRawString) {
    if (instancesRawString == null || instancesRawString.length() == 0) {
      return Collections.emptyList();
    }
    String[] instancesArray = instancesRawString.split(INSTANCE_SEPARATOR);
    List<String> instances = new ArrayList<>(instancesArray.length);
    Collections.addAll(instances, instancesArray);
    return instances;
  }

  private String convertInstancesToString(List<String> instances) {
    if (instances == null || instances.isEmpty()) {
      return "";
    }
    StringBuilder stringBuilder = new StringBuilder();
    for (String instance : instances) {
      if (stringBuilder.length() == 0) {
        stringBuilder.append(instance);
      } else {
        stringBuilder.append(INSTANCE_SEPARATOR).append(instance);
      }
    }
    return stringBuilder.toString();
  }

  private Map<String, String> convertListToStringMap() {
    Map<String, String> convertedMap = new TreeMap<>();
    for (Map.Entry<Integer, List<String>> entry : _poolToInstancesMap.entrySet()) {
      convertedMap.put(Integer.toString(entry.getKey()), convertInstancesToString(entry.getValue()));
    }
    return convertedMap;
  }

  public ZNRecord toZNRecord() {
    ZNRecord znRecord = new ZNRecord(_instancePartitionsName);
    znRecord.setListFields(_partitionToInstancesMap);
    znRecord.setMapField(POOLS_KEY, convertListToStringMap());
    return znRecord;
  }

  public String toJsonString() {
    try {
      return JsonUtils.objectToString(this);
    } catch (JsonProcessingException e) {
      throw new IllegalStateException(e);
    }
  }

  @Override
  public String toString() {
    return toJsonString();
  }
}
