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
package com.linkedin.pinot.controller.helix.core.realtime.partition;

import com.linkedin.pinot.common.config.TableConfig;
import com.linkedin.pinot.controller.helix.PartitionAssignment;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;


/**
 * Generates a uniform partition assignment for the table
 * The partition assignment is done by uniformly spraying the partitions across available instances
 * A random point is picked as the start for the spreading out over instances
 *
 * This strategy does not care about what are the other tables on the tenant,
 * and only assigns partitions for the one table that is being added/updated
 *
 * NOTE: We do not support/expect partition aware tables with multi tenant setup, hence this strategy should suffice
 *
 * An example znode for 8 kafka partitions and and 6 realtime servers (Server_s1 to Server_s6)
 * for a table config with UniformStreamPartitionAssignmentStrategy looks as below in zookeeper.
 * This example assumes that the random point picked was at the first server i.e. Server_s1.company.com
 *
 {
 "id":"KafkaTopicName"
 ,"simpleFields":{
 }
 ,"listFields":{
 "0":["Server_s1.company.com_8001","Server_s2.company.com_8001","Server_s3.company.com_8001"]
 ,"1":["Server_s4.company.com_8001","Server_s5.company.com_8001","Server_s6.company.com_8001"]
 ,"2":["Server_s1.company.com_8001","Server_s2.company.com_8001","Server_s3.company.com_8001"]
 ,"3":["Server_s4.company.com_8001","Server_s5.company.com_8001","Server_s6.company.com_8001"]
 ,"4":["Server_s1.company.com_8001","Server_s2.company.com_8001","Server_s3.company.com_8001"]
 ,"5":["Server_s4.company.com_8001","Server_s5.company.com_8001","Server_s6.company.com_8001"]
 ,"6":["Server_s1.company.com_8001","Server_s2.company.com_8001","Server_s3.company.com_8001"]
 ,"7":["Server_s4.company.com_8001","Server_s5.company.com_8001","Server_s6.company.com_8001"]
 }
 ,"mapFields":{
 }
 }
 *
 */
public class UniformStreamPartitionAssignmentStrategy implements StreamPartitionAssignmentStrategy {

  private List<String> _instanceNames;

  private final Random rand = new Random();

  @Override
  public void init(List<TableConfig> allTablesInTenant, List<String> instanceNames,
      Map<String, PartitionAssignment> tableNameToPartitionsAssignment) {
    _instanceNames = instanceNames;
    // TODO: this strategy does not read other tables in the tenant and their partition assignments for now
    // This is because we want to avoid the race conditions we might encounter,
    // when multiple controllers try to read/write the partitions at the same time
  }

  @Override
  public Map<String, PartitionAssignment> generatePartitionAssignment(TableConfig tableConfig, int numPartitions) {

    Map<String, PartitionAssignment> newPartitionAssignment = new HashMap<>(1);

    String tableName = tableConfig.getTableName();
    int numReplicas = tableConfig.getValidationConfig().getReplicasPerPartitionNumber();

    Map<String, List<String>> realtimePartitionToInstances = new HashMap<>(numPartitions);
    int serverId = rand.nextInt(_instanceNames.size());
    for (int p = 0; p < numPartitions; p++) {
      List<String> instances = new ArrayList<>(numReplicas);
      for (int r = 0; r < numReplicas; r++) {
        instances.add(_instanceNames.get(serverId++));
        if (serverId == _instanceNames.size()) {
          serverId = 0;
        }
      }
      realtimePartitionToInstances.put(String.valueOf(p), instances);
    }
    newPartitionAssignment.put(tableName, new PartitionAssignment(tableName, realtimePartitionToInstances));
    return newPartitionAssignment;
  }
}
