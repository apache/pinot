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
package org.apache.pinot.query.service.dispatch;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import org.apache.pinot.common.proto.Worker;
import org.apache.pinot.query.routing.WorkerMetadata;


/// Validates complete materialized output coverage and binds logical partition `p` to consumer worker `p`.
///
/// Routing is deterministic and does not change worker count, worker ids, mailbox metadata, custom properties, or
/// placement. The planner supplies workers in dense ID order; staged dispatch binds each consumer once.
/// This class is stateless and thread-safe.
final class MaterializedPartitionRouter {
  private static final Comparator<Worker.MaterializedPartitionHandle> HANDLE_ORDER =
      Comparator.comparingInt(Worker.MaterializedPartitionHandle::getProducerWorkerId);

  private MaterializedPartitionRouter() {
  }

  static List<WorkerMetadata> route(long requestId, int producerStageId, List<WorkerMetadata> producerWorkers,
      List<WorkerMetadata> consumerWorkers, List<Worker.MaterializedPartitionHandle> availableHandles) {
    int producerCount = producerWorkers.size();
    int partitionCount = consumerWorkers.size();
    boolean[][] covered = new boolean[producerCount][partitionCount];
    List<List<Worker.MaterializedPartitionHandle>> assigned = new ArrayList<>(partitionCount);
    for (int i = 0; i < partitionCount; i++) {
      assigned.add(new ArrayList<>());
    }

    int actualCount = 0;
    for (Worker.MaterializedPartitionHandle handle : availableHandles) {
      if (handle.getProducerStageId() != producerStageId) {
        continue;
      }
      actualCount++;
      if (handle.getRequestId() != requestId) {
        throw new IllegalStateException("Unexpected materialized input request id: " + handle.getRequestId());
      }
      int producerWorkerId = handle.getProducerWorkerId();
      int partitionId = handle.getLogicalPartitionId();
      if (producerWorkerId < 0 || producerWorkerId >= producerCount
          || partitionId < 0 || partitionId >= partitionCount) {
        throw new IllegalStateException("Unexpected materialized input: " + identity(handle));
      }
      if (covered[producerWorkerId][partitionId]) {
        throw new IllegalStateException("Duplicate materialized input: " + identity(handle));
      }
      covered[producerWorkerId][partitionId] = true;
      assigned.get(partitionId).add(handle);
    }

    int expectedCount = producerCount * partitionCount;
    if (actualCount != expectedCount) {
      throw new IllegalStateException("Incomplete materialized input coverage for producer stage " + producerStageId
          + ": expected=" + expectedCount + ", actual=" + actualCount);
    }

    List<WorkerMetadata> routed = new ArrayList<>(partitionCount);
    for (int workerId = 0; workerId < partitionCount; workerId++) {
      WorkerMetadata original = consumerWorkers.get(workerId);
      List<Worker.MaterializedPartitionHandle> inputs = assigned.get(workerId);
      inputs.sort(HANDLE_ORDER);
      routed.add(original.withMaterializedInputs(inputs));
    }
    return List.copyOf(routed);
  }

  private static String identity(Worker.MaterializedPartitionHandle handle) {
    return handle.getProducerStageId() + "/" + handle.getProducerWorkerId() + "/"
        + handle.getLogicalPartitionId();
  }
}
