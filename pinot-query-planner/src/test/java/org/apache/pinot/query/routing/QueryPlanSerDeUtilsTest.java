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
package org.apache.pinot.query.routing;

import java.util.List;
import java.util.Map;
import org.apache.pinot.common.proto.Worker;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.query.planner.plannode.PlanNode;
import org.apache.pinot.query.planner.plannode.ValueNode;
import org.apache.pinot.query.planner.serde.PlanNodeSerializer;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;


/// Tests worker metadata protocol serialization.
public class QueryPlanSerDeUtilsTest {
  @Test
  public void testMaterializedInputsRoundTrip()
      throws Exception {
    Worker.MaterializedPartitionHandle handle = Worker.MaterializedPartitionHandle.newBuilder()
        .setRequestId(7L)
        .setProducerStageId(1)
        .setProducerWorkerId(2)
        .setLogicalPartitionId(3)
        .setHost("producer")
        .setTransferPort(1234)
        .build();
    WorkerMetadata workerMetadata = new WorkerMetadata(0, Map.of(), Map.of(), List.of(handle));
    Worker.WorkerMetadata protoWorker =
        QueryPlanSerDeUtils.toProtoWorkerMetadataList(List.of(workerMetadata)).get(0);
    DataSchema dataSchema =
        new DataSchema(new String[]{"col"}, new ColumnDataType[]{ColumnDataType.INT});
    PlanNode root = new ValueNode(4, dataSchema, PlanNode.NodeHint.EMPTY, List.of(), List.of());
    Worker.StagePlan protoStage = Worker.StagePlan.newBuilder()
        .setRootNode(PlanNodeSerializer.process(root).toByteString())
        .setStageMetadata(Worker.StageMetadata.newBuilder()
            .setStageId(4)
            .addWorkerMetadata(protoWorker)
            .setCustomProperty(QueryPlanSerDeUtils.toProtoProperties(Map.of())))
        .build();

    StagePlan stagePlan = QueryPlanSerDeUtils.fromProtoStagePlan(protoStage);

    assertEquals(protoWorker.getMaterializedInputList(), List.of(handle));
    assertEquals(stagePlan.getStageMetadata().getWorkerMetadataList().get(0).getMaterializedInputs(),
        List.of(handle));
  }
}
