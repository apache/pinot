/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
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

package com.linkedin.pinot.controller.helix;

import java.util.HashMap;
import java.util.List;
import java.util.Map;


/**
 * Class to represent a partition assignment
 */
public class PartitionAssignment {

  private String _tableName;
  private Map<String, List<String>> _partitionToInstances;

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

  @Override
  public String toString() {
    return "PartitionAssignment{" + "_tableName='" + _tableName + '\'' + ", _partitionToInstances="
        + _partitionToInstances + '}';
  }
}
