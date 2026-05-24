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
package org.apache.pinot.controller.api.upload;

import java.util.Map;
import java.util.Set;
import javax.ws.rs.core.Response;
import org.apache.pinot.controller.api.exception.ControllerApplicationException;
import org.apache.pinot.segment.spi.ColumnMetadata;
import org.apache.pinot.segment.spi.SegmentMetadata;
import org.apache.pinot.spi.config.table.ColumnPartitionConfig;
import org.apache.pinot.spi.config.table.ReplicaGroupStrategyConfig;
import org.apache.pinot.spi.config.table.SegmentPartitionConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.config.table.UpsertConfig;
import org.apache.pinot.spi.config.table.assignment.InstanceAssignmentConfig;
import org.apache.pinot.spi.config.table.assignment.InstancePartitionsType;
import org.apache.pinot.spi.config.table.assignment.InstanceReplicaGroupPartitionConfig;
import org.apache.pinot.spi.config.table.assignment.InstanceTagPoolConfig;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.expectThrows;


public class SegmentValidationUtilsTest {
  private static final String RAW_TABLE_NAME = "testTable";
  private static final String SEGMENT_NAME = "testSegment";
  private static final String PARTITION_COLUMN = "pk";
  private static final int NUM_PARTITIONS = 4;

  @Test
  public void testValidateUpsertSegmentPartitionMetadataAcceptsSinglePartitionId() {
    SegmentValidationUtils.validateUpsertSegmentPartitionMetadata(mockSegmentMetadata(Set.of(2)),
        getOfflineUpsertTableConfig());
  }

  @Test
  public void testValidateUpsertSegmentPartitionMetadataAcceptsInstanceAssignmentPartitionColumn() {
    InstanceAssignmentConfig instanceAssignmentConfig =
        new InstanceAssignmentConfig(new InstanceTagPoolConfig("DefaultTenant", false, 0, null), null,
            new InstanceReplicaGroupPartitionConfig(true, 0, 0, 0, 1, 0, false, PARTITION_COLUMN), null, false);
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE)
        .setTableName(RAW_TABLE_NAME)
        .setUpsertConfig(new UpsertConfig(UpsertConfig.Mode.FULL))
        .setInstanceAssignmentConfigMap(Map.of(InstancePartitionsType.OFFLINE.name(), instanceAssignmentConfig))
        .build();

    SegmentValidationUtils.validateUpsertSegmentPartitionMetadata(mockSegmentMetadata(Set.of(2)), tableConfig);
  }

  @Test
  public void testValidateUpsertSegmentPartitionMetadataAcceptsSingleSegmentPartitionConfigColumn() {
    SegmentValidationUtils.validateUpsertSegmentPartitionMetadata(mockSegmentMetadata(Set.of(2)),
        getOfflineUpsertTableConfigWithSegmentPartitionConfig());
  }

  @Test
  public void testValidateUpsertSegmentPartitionMetadataRejectsMultipleIdsWithSingleSegmentPartitionConfigColumn() {
    ControllerApplicationException exception = expectThrows(ControllerApplicationException.class,
        () -> SegmentValidationUtils.validateUpsertSegmentPartitionMetadata(mockSegmentMetadata(Set.of(1, 2)),
            getOfflineUpsertTableConfigWithSegmentPartitionConfig()));
    assertEquals(exception.getResponse().getStatus(), Response.Status.BAD_REQUEST.getStatusCode());
  }

  @Test
  public void testValidateUpsertSegmentPartitionMetadataRejectsMultiplePartitionIds() {
    ControllerApplicationException exception = expectThrows(ControllerApplicationException.class,
        () -> SegmentValidationUtils.validateUpsertSegmentPartitionMetadata(mockSegmentMetadata(Set.of(1, 2)),
            getOfflineUpsertTableConfig()));
    assertEquals(exception.getResponse().getStatus(), Response.Status.BAD_REQUEST.getStatusCode());
  }

  @Test
  public void testValidateUpsertSegmentPartitionMetadataRejectsMissingPartitionId() {
    ControllerApplicationException exception = expectThrows(ControllerApplicationException.class,
        () -> SegmentValidationUtils.validateUpsertSegmentPartitionMetadata(mockSegmentMetadata(null),
            getOfflineUpsertTableConfig()));
    assertEquals(exception.getResponse().getStatus(), Response.Status.BAD_REQUEST.getStatusCode());
  }

  @Test
  public void testValidateUpsertSegmentPartitionMetadataRejectsMissingPartitionColumnMetadata() {
    ControllerApplicationException exception = expectThrows(ControllerApplicationException.class,
        () -> SegmentValidationUtils.validateUpsertSegmentPartitionMetadata(mockSegmentMetadataWithoutPartitionColumn(),
            getOfflineUpsertTableConfig()));
    assertEquals(exception.getResponse().getStatus(), Response.Status.BAD_REQUEST.getStatusCode());
  }

  @Test
  public void testValidateUpsertSegmentPartitionMetadataSkipsNonUpsertTable() {
    SegmentValidationUtils.validateUpsertSegmentPartitionMetadata(mockSegmentMetadata(Set.of(1)),
        new TableConfigBuilder(TableType.OFFLINE).setTableName(RAW_TABLE_NAME).build());
  }

  @Test
  public void testValidateUpsertSegmentPartitionMetadataSkipsTableWithoutPartitionColumn() {
    SegmentValidationUtils.validateUpsertSegmentPartitionMetadata(mockSegmentMetadata(Set.of(1)),
        new TableConfigBuilder(TableType.OFFLINE)
            .setTableName(RAW_TABLE_NAME)
            .setUpsertConfig(new UpsertConfig(UpsertConfig.Mode.FULL))
            .build());
  }

  @Test
  public void testValidateUpsertSegmentPartitionMetadataDoesNotValidateTablePartitionConfigCardinality() {
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE)
        .setTableName(RAW_TABLE_NAME)
        .setUpsertConfig(new UpsertConfig(UpsertConfig.Mode.FULL))
        .setReplicaGroupStrategyConfig(new ReplicaGroupStrategyConfig(PARTITION_COLUMN, 1))
        .setSegmentPartitionConfig(new SegmentPartitionConfig(Map.of(PARTITION_COLUMN,
            new ColumnPartitionConfig("Murmur", NUM_PARTITIONS), "otherPartitionColumn",
            new ColumnPartitionConfig("Modulo", NUM_PARTITIONS))))
        .build();

    SegmentValidationUtils.validateUpsertSegmentPartitionMetadata(mockSegmentMetadata(Set.of(1)), tableConfig);
  }

  private static TableConfig getOfflineUpsertTableConfig() {
    return new TableConfigBuilder(TableType.OFFLINE)
        .setTableName(RAW_TABLE_NAME)
        .setUpsertConfig(new UpsertConfig(UpsertConfig.Mode.FULL))
        .setReplicaGroupStrategyConfig(new ReplicaGroupStrategyConfig(PARTITION_COLUMN, 1))
        .build();
  }

  private static TableConfig getOfflineUpsertTableConfigWithSegmentPartitionConfig() {
    return new TableConfigBuilder(TableType.OFFLINE)
        .setTableName(RAW_TABLE_NAME)
        .setUpsertConfig(new UpsertConfig(UpsertConfig.Mode.FULL))
        .setSegmentPartitionConfig(new SegmentPartitionConfig(Map.of(PARTITION_COLUMN,
            new ColumnPartitionConfig("Murmur", NUM_PARTITIONS))))
        .build();
  }

  private static SegmentMetadata mockSegmentMetadata(Set<Integer> partitions) {
    ColumnMetadata columnMetadata = mockColumnMetadata(partitions);
    SegmentMetadata segmentMetadata = mock(SegmentMetadata.class);
    when(segmentMetadata.getName()).thenReturn(SEGMENT_NAME);
    when(segmentMetadata.getColumnMetadataFor(PARTITION_COLUMN)).thenReturn(columnMetadata);
    return segmentMetadata;
  }

  private static SegmentMetadata mockSegmentMetadataWithoutPartitionColumn() {
    SegmentMetadata segmentMetadata = mock(SegmentMetadata.class);
    when(segmentMetadata.getName()).thenReturn(SEGMENT_NAME);
    when(segmentMetadata.getColumnMetadataFor(PARTITION_COLUMN)).thenReturn(null);
    return segmentMetadata;
  }

  private static ColumnMetadata mockColumnMetadata(Set<Integer> partitions) {
    ColumnMetadata columnMetadata = mock(ColumnMetadata.class);
    when(columnMetadata.getPartitions()).thenReturn(partitions);
    return columnMetadata;
  }
}
