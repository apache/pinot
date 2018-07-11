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

import com.linkedin.pinot.common.utils.EqualityUtils;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;


/**
 * Class to represent a partition assignment
 */
public class PartitionAssignment {

  protected String _tableName;
  protected Map<String, List<String>> _partitionToInstances;

  public PartitionAssignment(String tableName) {
    _tableName = tableName;
    _partitionToInstances = new HashMap<>(1);
  }

  public PartitionAssignment(String tableName, Map<String, List<String>> partitionToInstances) {
    _tableName = tableName;
    _partitionToInstances = partitionToInstances;
  }

  public String getTableName() {
    return _tableName;
  }

  public Map<String, List<String>> getPartitionToInstances() {
    return _partitionToInstances;
  }

  public void addPartition(String partitionName, List<String> instances) {
    _partitionToInstances.put(partitionName, instances);
  }

  public List<String> getInstancesListForPartition(String partition) {
    return _partitionToInstances.get(partition);
  }

  public int getNumPartitions() {
    return _partitionToInstances.size();
  }

  /**
   * Get all instances.
   *
   * @return Set of all instances for this table
   */
  public List<String> getAllInstances() {
    Set<String> serverList = new HashSet<>();
    for(List<String> servers: getPartitionToInstances().values()) {
      serverList.addAll(servers);
    }
    return new ArrayList<>(serverList);
  }

  @Override
  public String toString() {
    return "PartitionAssignment{" + "_tableName='" + _tableName + '\'' + ", _partitionToInstances="
        + _partitionToInstances + '}';
  }

  @Override
  public boolean equals(Object o) {
    if (EqualityUtils.isSameReference(this, o)) {
      return true;
    }

    if (EqualityUtils.isNullOrNotSameClass(this, o)) {
      return false;
    }

    PartitionAssignment that = (PartitionAssignment) o;

    return EqualityUtils.isEqual(_tableName, that._tableName) && EqualityUtils.isEqual(_partitionToInstances,
        that._partitionToInstances);
  }

  @Override
  public int hashCode() {
    int result = EqualityUtils.hashCodeOf(_tableName);
    result = EqualityUtils.hashCodeOf(result, _partitionToInstances);
    return result;
  }
}
