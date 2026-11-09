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

import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import javax.annotation.Nullable;
import org.apache.helix.model.IdealState;


/// Partition role assignments for replicate mode, stored in auxiliary fields of the table IdealState.
/// The leader is stored in `simpleFields[replicateMode.leader.<partitionId>]`, and the full replica candidate list,
/// including the leader, in `listFields[replicateMode.replicas.<partitionId>]`. Followers are derived from this list.
/// These fields describe placement intent, not liveness, query readiness or a failover authorization.
/// Entries can outlive a mode change while earlier segments are still completing. The segment metadata determines
/// whether the recorded replicate-mode roles apply to a consuming segment.
///
/// Callers must persist changes together with the segment assignment in the same version-checked IdealState update.
/// These helpers do not synchronize access to the supplied IdealState. The non-numeric keys are independent of the
/// partition-pinned assignment maintained by [PartitionAssignmentUtils].
public final class ReplicateModeAssignmentUtils {
  private static final String LEADER_KEY_PREFIX = "replicateMode.leader.";
  private static final String REPLICAS_KEY_PREFIX = "replicateMode.replicas.";

  private ReplicateModeAssignmentUtils() {
  }

  @Nullable
  public static String getLeader(IdealState idealState, int partitionId) {
    return idealState.getRecord().getSimpleField(LEADER_KEY_PREFIX + partitionId);
  }

  /// Returns an immutable snapshot of the full candidate list, or an empty list if no assignment has been recorded.
  public static List<String> getReplicas(IdealState idealState, int partitionId) {
    List<String> replicas = idealState.getRecord().getListField(REPLICAS_KEY_PREFIX + partitionId);
    return replicas != null ? List.copyOf(replicas) : List.of();
  }

  public static List<String> getFollowers(IdealState idealState, int partitionId) {
    List<String> followers = new ArrayList<>(getReplicas(idealState, partitionId));
    followers.remove(getLeader(idealState, partitionId));
    return List.copyOf(followers);
  }

  /// Records the leader and the complete candidate set. Candidates are sorted to keep the stored form deterministic.
  /// The consuming segment itself must only be assigned to the leader by the caller.
  public static void setAssignment(IdealState idealState, int partitionId, String leader,
      Collection<String> replicas) {
    Preconditions.checkArgument(partitionId >= 0, "Invalid partition id: %s", partitionId);
    Preconditions.checkArgument(leader != null && replicas.contains(leader),
        "Leader must belong to the replica candidates");
    Preconditions.checkArgument(new HashSet<>(replicas).size() == replicas.size(),
        "Replica candidates must be distinct");
    List<String> replicaList = PartitionAssignmentUtils.toInstanceList(replicas);
    idealState.getRecord().setSimpleField(LEADER_KEY_PREFIX + partitionId, leader);
    idealState.getRecord().setListField(REPLICAS_KEY_PREFIX + partitionId, replicaList);
  }

  /// Returns the recorded leaders by Pinot partition id, including the stream index for multi-stream tables.
  public static Map<Integer, String> getLeaders(IdealState idealState) {
    Map<Integer, String> leaders = new TreeMap<>();
    for (Map.Entry<String, String> entry : idealState.getRecord().getSimpleFields().entrySet()) {
      if (entry.getKey().startsWith(LEADER_KEY_PREFIX) && entry.getValue() != null) {
        try {
          int partitionId = Integer.parseInt(entry.getKey().substring(LEADER_KEY_PREFIX.length()));
          if (partitionId >= 0) {
            leaders.put(partitionId, entry.getValue());
          }
        } catch (NumberFormatException e) {
          // Ignore auxiliary fields that do not identify a partition.
        }
      }
    }
    return leaders;
  }
}
