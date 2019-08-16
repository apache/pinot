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
package org.apache.pinot.controller.helix.core.assignment;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import org.apache.helix.ZNRecord;
import org.apache.pinot.common.utils.JsonUtils;


/**
 * Instance (server) partitions for the table.
 *
 * <p>The instance partitions is stored as a map from partition of the format: {@code <partitionId>_<replicaId>} to
 * list of server instances, and is persisted under the ZK path: {@code <cluster>/PROPERTYSTORE/INSTANCE_PARTITIONS}.
 * <p>The segment assignment will be based on the instance partitions of the table.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class InstancePartitions {
  private static final char PARTITION_REPLICA_SEPARATOR = '_';

  // Name will be of the format "<rawTableName>_<type>", e.g. "table_OFFLINE", "table_CONSUMING", "table_COMPLETED"
  private final String _name;
  private final Map<String, List<String>> _partitionToInstancesMap;
  private int _numPartitions;
  private int _numReplicas;

  public InstancePartitions(String name) {
    _name = name;
    _partitionToInstancesMap = new TreeMap<>();
  }

  @JsonCreator
  private InstancePartitions(@JsonProperty(value = "name", required = true) String name,
      @JsonProperty(value = "partitionToInstancesMap", required = true) Map<String, List<String>> partitionToInstancesMap) {
    _name = name;
    _partitionToInstancesMap = partitionToInstancesMap;
    for (String key : partitionToInstancesMap.keySet()) {
      int splitterIndex = key.indexOf(PARTITION_REPLICA_SEPARATOR);
      int partition = Integer.parseInt(key.substring(0, splitterIndex));
      int replica = Integer.parseInt(key.substring(splitterIndex + 1));
      _numPartitions = Integer.max(_numPartitions, partition + 1);
      _numReplicas = Integer.max(_numReplicas, replica + 1);
    }
  }

  @JsonProperty
  public String getName() {
    return _name;
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
  public int getNumReplicas() {
    return _numReplicas;
  }

  public List<String> getInstances(int partitionId, int replicaId) {
    return _partitionToInstancesMap.get(Integer.toString(partitionId) + PARTITION_REPLICA_SEPARATOR + replicaId);
  }

  public void setInstances(int partitionId, int replicaId, List<String> instances) {
    String key = Integer.toString(partitionId) + PARTITION_REPLICA_SEPARATOR + replicaId;
    _partitionToInstancesMap.put(key, instances);
    _numPartitions = Integer.max(_numPartitions, partitionId + 1);
    _numReplicas = Integer.max(_numReplicas, replicaId + 1);
  }

  public static InstancePartitions fromZNRecord(ZNRecord znRecord) {
    return new InstancePartitions(znRecord.getId(), znRecord.getListFields());
  }

  public ZNRecord toZNRecord() {
    ZNRecord znRecord = new ZNRecord(_name);
    znRecord.setListFields(_partitionToInstancesMap);
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
