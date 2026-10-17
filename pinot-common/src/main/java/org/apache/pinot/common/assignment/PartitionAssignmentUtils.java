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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import javax.annotation.Nullable;
import org.apache.helix.model.IdealState;


/// Partition assignment of a table: the instances hosting all segments of each segment partition, stored in the list
/// fields of the table IdealState. List fields whose key is not a partition id are not part of it and are left alone.
///
/// Each list field maps a segment partition id (the stream partition for a real-time table, the value partition for an
/// offline table) to the sorted list of instances hosting all segments of that partition. The partition assignment is
/// maintained for partition-pinned tables only, i.e. tables whose segments must be collocated per partition: tables
/// with strict replica-group routing, which upsert and dedup tables use. For these tables:
/// - A new segment is assigned to the instances of its partition when the partition has an entry, even when the
///   instance partitions have changed since the entry was created. The entry is created from the instance partitions
///   otherwise.
/// - Table rebalance moves the entries together with the segments, so that a segment added during the rebalance is
///   assigned to the same instances as the other segments of its partition at that point of the rebalance.
///
/// The entries are read and written together with the map fields of the IdealState, so they are never observed out of
/// sync with the segment assignment. Helix ignores the list fields of an IdealState in CUSTOMIZED rebalance mode.
public class PartitionAssignmentUtils {
  private PartitionAssignmentUtils() {
  }

  /// Returns the instances of the given partition, or `null` if the partition has no entry.
  @Nullable
  public static List<String> getInstances(IdealState idealState, int partitionId) {
    return idealState.getRecord().getListField(Integer.toString(partitionId));
  }

  /// Sets the instances of the given partition.
  public static void setInstances(IdealState idealState, int partitionId, Collection<String> instances) {
    idealState.getRecord().setListField(Integer.toString(partitionId), toInstanceList(instances));
  }

  /// Returns the partition assignment as a sorted map from partition id to the instances of the partition. The lists
  /// are the ones stored in the IdealState record and must not be modified.
  public static Map<Integer, List<String>> getPartitionAssignment(IdealState idealState) {
    Map<Integer, List<String>> partitionAssignment = new TreeMap<>();
    for (Map.Entry<String, List<String>> entry : idealState.getRecord().getListFields().entrySet()) {
      try {
        partitionAssignment.put(Integer.parseInt(entry.getKey()), entry.getValue());
      } catch (NumberFormatException e) {
        // Not a partition entry
      }
    }
    return partitionAssignment;
  }

  /// Replaces the partition assignment with the given one. Entries of partitions absent from the given map are
  /// removed.
  public static void setPartitionAssignment(IdealState idealState, Map<Integer, List<String>> partitionAssignment) {
    Map<String, List<String>> listFields = idealState.getRecord().getListFields();
    listFields.keySet().removeIf(PartitionAssignmentUtils::isPartitionId);
    for (Map.Entry<Integer, List<String>> entry : partitionAssignment.entrySet()) {
      listFields.put(Integer.toString(entry.getKey()), toInstanceList(entry.getValue()));
    }
  }

  private static boolean isPartitionId(String key) {
    try {
      Integer.parseInt(key);
      return true;
    } catch (NumberFormatException e) {
      return false;
    }
  }

  /// Returns the given instances as an entry of the partition assignment: a sorted list, so that entries created from
  /// the instance partitions, from the existing segments of the partition and from the rebalance steps compare equal
  /// when they hold the same instances.
  public static List<String> toInstanceList(Collection<String> instances) {
    List<String> instanceList = new ArrayList<>(instances);
    Collections.sort(instanceList);
    return instanceList;
  }
}
