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
package org.apache.pinot.controller.api.resources;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;
import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import org.apache.helix.AccessOption;
import org.apache.helix.model.IdealState;
import org.apache.pinot.common.assignment.InstancePartitions;
import org.apache.pinot.common.assignment.InstancePartitionsUtils;
import org.apache.pinot.common.metadata.ZKMetadataProvider;
import org.apache.pinot.common.metadata.segment.SegmentPartitionMetadata;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.common.utils.LLCSegmentName;
import org.apache.pinot.common.utils.helix.HelixHelper;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.controller.helix.core.assignment.segment.SegmentAssignment;
import org.apache.pinot.controller.helix.core.assignment.segment.SegmentAssignmentFactory;
import org.apache.pinot.controller.helix.core.assignment.segment.SegmentAssignmentUtils;
import org.apache.pinot.controller.helix.core.realtime.PinotLLCRealtimeSegmentManager;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.config.table.assignment.InstancePartitionsType;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;


@Api(tags = Constants.SEGMENT_TAG)
@Path("/")
public class PinotSegmentStateResource {

  @Inject
  PinotHelixResourceManager _pinotHelixResourceManager;

  // TODO add support for choosing default offset for other partitions
  private static class RealtimeSegmentResumeConfig {
    private Map<Integer, Long> _partitionIdToOffsetMap;

    public Map<Integer, Long> getPartitionIdToOffsetMap() {
      return _partitionIdToOffsetMap;
    }

    public void setPartitionIdToOffsetMap(Map<Integer, Long> partitionIdToOffsetMap) {
      _partitionIdToOffsetMap = partitionIdToOffsetMap;
    }
  }

  @POST
  @Path("/segmentState/{tableName}/resumeRealtimeTable")
  @Produces(MediaType.APPLICATION_JSON)
  @Consumes(MediaType.APPLICATION_JSON)
  @ApiOperation(value = "Resume a realtime table", notes = "Resume a segment")
  public String resumeRealtimeTable(
      @ApiParam(value = "Name of the table", required = true) @PathParam("tableName") String tableName,
      RealtimeSegmentResumeConfig realtimeSegmentResumeConfig) {
    TableConfig tableConfig = _pinotHelixResourceManager.getTableConfig(tableName, TableType.REALTIME);
    String realtimeTableName = TableNameBuilder.REALTIME.tableNameWithType(tableName);
    InstancePartitions instancePartitions =
        InstancePartitionsUtils.fetchOrComputeInstancePartitions(_pinotHelixResourceManager.getHelixZkManager(),
            tableConfig, InstancePartitionsType.CONSUMING);
    Map<InstancePartitionsType, InstancePartitions> instancePartitionsMap =
        Collections.singletonMap(InstancePartitionsType.CONSUMING, instancePartitions);

    IdealState idealState =
        HelixHelper.getTableIdealState(_pinotHelixResourceManager.getHelixZkManager(), realtimeTableName);
    int numReplicas = PinotLLCRealtimeSegmentManager.getNumReplicas(tableConfig, instancePartitions);
    Map<String, Map<String, String>> instanceStatesMap = idealState.getRecord().getMapFields();

    for (Map.Entry<Integer, Long> partitionOffset : realtimeSegmentResumeConfig.getPartitionIdToOffsetMap()
        .entrySet()) {
      int partitionId = partitionOffset.getKey();
      Long offset = partitionOffset.getValue();

      // Step 1: Create PROPERTYSTORE nodes for each partitionId (IN_PROGRESS)
      long newSegmentCreationTimeMs = System.currentTimeMillis();

      // TODO(saurabh) Get the sequence number from ideal state (largest seq # + 1)
      int seqNumber = Math.abs(new Random().nextInt());

      LLCSegmentName newLLCSegment = new LLCSegmentName(tableName, partitionId, seqNumber, newSegmentCreationTimeMs);

      SegmentZKMetadata newSegmentZKMetadata = new SegmentZKMetadata(newLLCSegment.getSegmentName());
      newSegmentZKMetadata.setCreationTime(newLLCSegment.getCreationTimeMs());
      newSegmentZKMetadata.setStartOffset(offset.toString());
      newSegmentZKMetadata.setNumReplicas(numReplicas);
      newSegmentZKMetadata.setStatus(CommonConstants.Segment.Realtime.Status.IN_PROGRESS);
      SegmentPartitionMetadata segmentPartitionMetadata =
          PinotLLCRealtimeSegmentManager.getPartitionMetadataFromTableConfig(tableConfig, partitionId);
      if (segmentPartitionMetadata != null) {
        newSegmentZKMetadata.setPartitionMetadata(segmentPartitionMetadata);
      }

      // TODO(saurabh) should we be updating flushThresholds for this segment?

      _pinotHelixResourceManager.getPropertyStore().set(
          ZKMetadataProvider.constructPropertyStorePathForSegment(realtimeTableName, newLLCSegment.getSegmentName()),
          newSegmentZKMetadata.toZNRecord(), -1, AccessOption.PERSISTENT);

      // Step 2: Update IDEALSTATE
      SegmentAssignment segmentAssignment =
          SegmentAssignmentFactory.getSegmentAssignment(_pinotHelixResourceManager.getHelixZkManager(), tableConfig);
      List<String> instancesAssigned =
          segmentAssignment.assignSegment(newLLCSegment.getSegmentName(), instanceStatesMap, instancePartitionsMap);
      instanceStatesMap.put(newLLCSegment.getSegmentName(),
          SegmentAssignmentUtils.getInstanceStateMap(instancesAssigned,
              CommonConstants.Helix.StateModel.SegmentStateModel.CONSUMING));
    }

    // TODO should we commit the state for each partition instead?
    _pinotHelixResourceManager.getHelixAdmin()
        .setResourceIdealState(_pinotHelixResourceManager.getHelixClusterName(), realtimeTableName, idealState);

    // TODO change this
    return null;
  }
}
