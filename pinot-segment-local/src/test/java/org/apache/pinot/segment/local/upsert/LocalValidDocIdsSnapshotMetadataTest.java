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
package org.apache.pinot.segment.local.upsert;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.spi.config.table.ColumnPartitionConfig;
import org.apache.pinot.spi.config.table.HashFunction;
import org.apache.pinot.spi.config.table.SegmentPartitionConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.config.table.UpsertConfig;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.*;


public class LocalValidDocIdsSnapshotMetadataTest {
  private static final File TEMP_DIR = new File(FileUtils.getTempDirectory(), "LocalValidDocIdsSnapshotMetadataTest");
  private static final List<String> PRIMARY_KEY_COLUMNS = Arrays.asList("pk1", "pk2");
  private static final List<String> COMPARISON_COLUMNS = Collections.singletonList("timestamp");
  private static final String DELETE_RECORD_COLUMN = "deleted";
  private static final double METADATA_TTL = 86400.0;
  private static final double DELETED_KEYS_TTL = 172800.0;
  private static final int PARTITION_ID = 0;
  private static final int NUM_PARTITIONS = 8;

  @BeforeMethod
  public void setUp()
      throws IOException {
    FileUtils.forceMkdir(TEMP_DIR);
  }

  @AfterMethod
  public void tearDown()
      throws IOException {
    FileUtils.forceDelete(TEMP_DIR);
  }

  private UpsertContext createMockUpsertContext(List<String> primaryKeyColumns, List<String> comparisonColumns,
      String deleteRecordColumn, HashFunction hashFunction, double metadataTTL, double deletedKeysTTL,
      UpsertConfig.Mode upsertMode) {
    return createMockUpsertContext(primaryKeyColumns, comparisonColumns, deleteRecordColumn, hashFunction,
        metadataTTL, deletedKeysTTL, upsertMode, NUM_PARTITIONS);
  }

  private UpsertContext createMockUpsertContext(List<String> primaryKeyColumns, List<String> comparisonColumns,
      String deleteRecordColumn, HashFunction hashFunction, double metadataTTL, double deletedKeysTTL,
      UpsertConfig.Mode upsertMode, int numPartitions) {
    UpsertContext context = mock(UpsertContext.class);
    when(context.getPrimaryKeyColumns()).thenReturn(primaryKeyColumns);
    when(context.getComparisonColumns()).thenReturn(comparisonColumns);
    when(context.getDeleteRecordColumn()).thenReturn(deleteRecordColumn);
    when(context.getHashFunction()).thenReturn(hashFunction);
    when(context.getMetadataTTL()).thenReturn(metadataTTL);
    when(context.getDeletedKeysTTL()).thenReturn(deletedKeysTTL);

    UpsertConfig upsertConfig = new UpsertConfig(upsertMode);
    if (upsertMode == UpsertConfig.Mode.PARTIAL) {
      upsertConfig.setDefaultPartialUpsertStrategy(UpsertConfig.Strategy.OVERWRITE);
      Map<String, UpsertConfig.Strategy> strategies = new HashMap<>();
      strategies.put("col1", UpsertConfig.Strategy.INCREMENT);
      upsertConfig.setPartialUpsertStrategies(strategies);
    }

    TableConfigBuilder tableConfigBuilder = new TableConfigBuilder(TableType.REALTIME)
        .setTableName("testTable")
        .setUpsertConfig(upsertConfig);

    // Add segment partition config if numPartitions > 0
    if (numPartitions > 0) {
      Map<String, ColumnPartitionConfig> columnPartitionMap = new HashMap<>();
      columnPartitionMap.put("partitionColumn", new ColumnPartitionConfig("Murmur3", numPartitions));
      SegmentPartitionConfig segmentPartitionConfig = new SegmentPartitionConfig(columnPartitionMap);
      tableConfigBuilder.setSegmentPartitionConfig(segmentPartitionConfig);
    }

    TableConfig tableConfig = tableConfigBuilder.build();
    when(context.getTableConfig()).thenReturn(tableConfig);

    return context;
  }

  @Test
  public void testFromUpsertContext() {
    UpsertContext context = createMockUpsertContext(PRIMARY_KEY_COLUMNS, COMPARISON_COLUMNS, DELETE_RECORD_COLUMN,
        HashFunction.NONE, METADATA_TTL, DELETED_KEYS_TTL, UpsertConfig.Mode.FULL);

    LocalValidDocIdsSnapshotMetadata metadata =
        LocalValidDocIdsSnapshotMetadata.fromUpsertContext(PARTITION_ID, context);

    assertEquals(metadata.getVersion(), LocalValidDocIdsSnapshotMetadata.CURRENT_VERSION);
    assertEquals(metadata.getPartitionId(), PARTITION_ID);
    assertEquals(metadata.getPrimaryKeyColumns(), PRIMARY_KEY_COLUMNS);
    assertEquals(metadata.getComparisonColumns(), COMPARISON_COLUMNS);
    assertEquals(metadata.getDeleteRecordColumn(), DELETE_RECORD_COLUMN);
    assertEquals(metadata.getHashFunction(), HashFunction.NONE);
    assertEquals(metadata.getMetadataTTL(), METADATA_TTL);
    assertEquals(metadata.getDeletedKeysTTL(), DELETED_KEYS_TTL);
    assertEquals(metadata.getUpsertMode(), UpsertConfig.Mode.FULL);
    assertNull(metadata.getPartialUpsertStrategies());
    assertNull(metadata.getDefaultPartialUpsertStrategy());
  }

  @Test
  public void testFromUpsertContextPartialMode() {
    UpsertContext context = createMockUpsertContext(PRIMARY_KEY_COLUMNS, COMPARISON_COLUMNS, DELETE_RECORD_COLUMN,
        HashFunction.MURMUR3, METADATA_TTL, DELETED_KEYS_TTL, UpsertConfig.Mode.PARTIAL);

    LocalValidDocIdsSnapshotMetadata metadata =
        LocalValidDocIdsSnapshotMetadata.fromUpsertContext(PARTITION_ID, context);

    assertEquals(metadata.getUpsertMode(), UpsertConfig.Mode.PARTIAL);
    assertNotNull(metadata.getPartialUpsertStrategies());
    assertEquals(metadata.getPartialUpsertStrategies().get("col1"), UpsertConfig.Strategy.INCREMENT);
    assertEquals(metadata.getDefaultPartialUpsertStrategy(), UpsertConfig.Strategy.OVERWRITE);
  }

  @Test
  public void testPersistAndRead()
      throws IOException {
    UpsertContext context = createMockUpsertContext(PRIMARY_KEY_COLUMNS, COMPARISON_COLUMNS, DELETE_RECORD_COLUMN,
        HashFunction.NONE, METADATA_TTL, DELETED_KEYS_TTL, UpsertConfig.Mode.FULL);

    LocalValidDocIdsSnapshotMetadata metadata =
        LocalValidDocIdsSnapshotMetadata.fromUpsertContext(PARTITION_ID, context);
    metadata.persist(TEMP_DIR);

    LocalValidDocIdsSnapshotMetadata readMetadata =
        LocalValidDocIdsSnapshotMetadata.fromDirectory(TEMP_DIR, PARTITION_ID);
    assertNotNull(readMetadata);
    assertEquals(readMetadata.getPartitionId(), PARTITION_ID);
    assertEquals(readMetadata.getPrimaryKeyColumns(), PRIMARY_KEY_COLUMNS);
    assertEquals(readMetadata.getComparisonColumns(), COMPARISON_COLUMNS);
    assertEquals(readMetadata.getDeleteRecordColumn(), DELETE_RECORD_COLUMN);
    assertEquals(readMetadata.getHashFunction(), HashFunction.NONE);
    assertEquals(readMetadata.getMetadataTTL(), METADATA_TTL);
    assertEquals(readMetadata.getDeletedKeysTTL(), DELETED_KEYS_TTL);
    assertEquals(readMetadata.getUpsertMode(), UpsertConfig.Mode.FULL);
  }

  @Test
  public void testFromDirectoryNoFile() {
    LocalValidDocIdsSnapshotMetadata metadata =
        LocalValidDocIdsSnapshotMetadata.fromDirectory(TEMP_DIR, PARTITION_ID);
    assertNull(metadata);
  }

  @Test
  public void testIsCompatibleSameConfig()
      throws IOException {
    UpsertContext context = createMockUpsertContext(PRIMARY_KEY_COLUMNS, COMPARISON_COLUMNS, DELETE_RECORD_COLUMN,
        HashFunction.NONE, METADATA_TTL, DELETED_KEYS_TTL, UpsertConfig.Mode.FULL);

    LocalValidDocIdsSnapshotMetadata metadata =
        LocalValidDocIdsSnapshotMetadata.fromUpsertContext(PARTITION_ID, context);
    metadata.persist(TEMP_DIR);

    LocalValidDocIdsSnapshotMetadata readMetadata =
        LocalValidDocIdsSnapshotMetadata.fromDirectory(TEMP_DIR, PARTITION_ID);
    assertNotNull(readMetadata);
    assertTrue(readMetadata.isCompatible(context, "testTable"));
  }

  @Test
  public void testIsCompatibleDifferentPrimaryKeys()
      throws IOException {
    UpsertContext originalContext = createMockUpsertContext(PRIMARY_KEY_COLUMNS, COMPARISON_COLUMNS,
        DELETE_RECORD_COLUMN, HashFunction.NONE, METADATA_TTL, DELETED_KEYS_TTL, UpsertConfig.Mode.FULL);

    LocalValidDocIdsSnapshotMetadata metadata =
        LocalValidDocIdsSnapshotMetadata.fromUpsertContext(PARTITION_ID, originalContext);
    metadata.persist(TEMP_DIR);

    // Create context with different primary keys
    UpsertContext newContext = createMockUpsertContext(Collections.singletonList("pk1"), COMPARISON_COLUMNS,
        DELETE_RECORD_COLUMN, HashFunction.NONE, METADATA_TTL, DELETED_KEYS_TTL, UpsertConfig.Mode.FULL);

    LocalValidDocIdsSnapshotMetadata readMetadata =
        LocalValidDocIdsSnapshotMetadata.fromDirectory(TEMP_DIR, PARTITION_ID);
    assertNotNull(readMetadata);
    assertFalse(readMetadata.isCompatible(newContext, "testTable"));
  }

  @Test
  public void testIsCompatibleDifferentComparisonColumns()
      throws IOException {
    UpsertContext originalContext = createMockUpsertContext(PRIMARY_KEY_COLUMNS, COMPARISON_COLUMNS,
        DELETE_RECORD_COLUMN, HashFunction.NONE, METADATA_TTL, DELETED_KEYS_TTL, UpsertConfig.Mode.FULL);

    LocalValidDocIdsSnapshotMetadata metadata =
        LocalValidDocIdsSnapshotMetadata.fromUpsertContext(PARTITION_ID, originalContext);
    metadata.persist(TEMP_DIR);

    // Create context with different comparison columns
    UpsertContext newContext = createMockUpsertContext(PRIMARY_KEY_COLUMNS,
        Collections.singletonList("eventTime"), DELETE_RECORD_COLUMN, HashFunction.NONE, METADATA_TTL,
        DELETED_KEYS_TTL, UpsertConfig.Mode.FULL);

    LocalValidDocIdsSnapshotMetadata readMetadata =
        LocalValidDocIdsSnapshotMetadata.fromDirectory(TEMP_DIR, PARTITION_ID);
    assertNotNull(readMetadata);
    assertFalse(readMetadata.isCompatible(newContext, "testTable"));
  }

  @Test
  public void testIsCompatibleDifferentDeleteRecordColumn()
      throws IOException {
    UpsertContext originalContext = createMockUpsertContext(PRIMARY_KEY_COLUMNS, COMPARISON_COLUMNS,
        DELETE_RECORD_COLUMN, HashFunction.NONE, METADATA_TTL, DELETED_KEYS_TTL, UpsertConfig.Mode.FULL);

    LocalValidDocIdsSnapshotMetadata metadata =
        LocalValidDocIdsSnapshotMetadata.fromUpsertContext(PARTITION_ID, originalContext);
    metadata.persist(TEMP_DIR);

    // Create context with different delete record column
    UpsertContext newContext = createMockUpsertContext(PRIMARY_KEY_COLUMNS, COMPARISON_COLUMNS,
        "isDeleted", HashFunction.NONE, METADATA_TTL, DELETED_KEYS_TTL, UpsertConfig.Mode.FULL);

    LocalValidDocIdsSnapshotMetadata readMetadata =
        LocalValidDocIdsSnapshotMetadata.fromDirectory(TEMP_DIR, PARTITION_ID);
    assertNotNull(readMetadata);
    assertFalse(readMetadata.isCompatible(newContext, "testTable"));
  }

  @Test
  public void testIsCompatibleDifferentMetadataTTL()
      throws IOException {
    UpsertContext originalContext = createMockUpsertContext(PRIMARY_KEY_COLUMNS, COMPARISON_COLUMNS,
        DELETE_RECORD_COLUMN, HashFunction.NONE, METADATA_TTL, DELETED_KEYS_TTL, UpsertConfig.Mode.FULL);

    LocalValidDocIdsSnapshotMetadata metadata =
        LocalValidDocIdsSnapshotMetadata.fromUpsertContext(PARTITION_ID, originalContext);
    metadata.persist(TEMP_DIR);

    // Create context with different metadata TTL
    UpsertContext newContext = createMockUpsertContext(PRIMARY_KEY_COLUMNS, COMPARISON_COLUMNS,
        DELETE_RECORD_COLUMN, HashFunction.NONE, METADATA_TTL * 2, DELETED_KEYS_TTL, UpsertConfig.Mode.FULL);

    LocalValidDocIdsSnapshotMetadata readMetadata =
        LocalValidDocIdsSnapshotMetadata.fromDirectory(TEMP_DIR, PARTITION_ID);
    assertNotNull(readMetadata);
    assertFalse(readMetadata.isCompatible(newContext, "testTable"));
  }

  @Test
  public void testIsCompatibleDifferentDeletedKeysTTL()
      throws IOException {
    UpsertContext originalContext = createMockUpsertContext(PRIMARY_KEY_COLUMNS, COMPARISON_COLUMNS,
        DELETE_RECORD_COLUMN, HashFunction.NONE, METADATA_TTL, DELETED_KEYS_TTL, UpsertConfig.Mode.FULL);

    LocalValidDocIdsSnapshotMetadata metadata =
        LocalValidDocIdsSnapshotMetadata.fromUpsertContext(PARTITION_ID, originalContext);
    metadata.persist(TEMP_DIR);

    // Create context with different deleted keys TTL
    UpsertContext newContext = createMockUpsertContext(PRIMARY_KEY_COLUMNS, COMPARISON_COLUMNS,
        DELETE_RECORD_COLUMN, HashFunction.NONE, METADATA_TTL, DELETED_KEYS_TTL * 2, UpsertConfig.Mode.FULL);

    LocalValidDocIdsSnapshotMetadata readMetadata =
        LocalValidDocIdsSnapshotMetadata.fromDirectory(TEMP_DIR, PARTITION_ID);
    assertNotNull(readMetadata);
    assertFalse(readMetadata.isCompatible(newContext, "testTable"));
  }

  @Test
  public void testIsCompatibleDifferentHashFunction()
      throws IOException {
    UpsertContext originalContext = createMockUpsertContext(PRIMARY_KEY_COLUMNS, COMPARISON_COLUMNS,
        DELETE_RECORD_COLUMN, HashFunction.NONE, METADATA_TTL, DELETED_KEYS_TTL, UpsertConfig.Mode.FULL);

    LocalValidDocIdsSnapshotMetadata metadata =
        LocalValidDocIdsSnapshotMetadata.fromUpsertContext(PARTITION_ID, originalContext);
    metadata.persist(TEMP_DIR);

    // Create context with different hash function
    UpsertContext newContext = createMockUpsertContext(PRIMARY_KEY_COLUMNS, COMPARISON_COLUMNS,
        DELETE_RECORD_COLUMN, HashFunction.MURMUR3, METADATA_TTL, DELETED_KEYS_TTL, UpsertConfig.Mode.FULL);

    LocalValidDocIdsSnapshotMetadata readMetadata =
        LocalValidDocIdsSnapshotMetadata.fromDirectory(TEMP_DIR, PARTITION_ID);
    assertNotNull(readMetadata);
    assertFalse(readMetadata.isCompatible(newContext, "testTable"));
  }

  @Test
  public void testIsCompatibleDifferentUpsertMode()
      throws IOException {
    UpsertContext originalContext = createMockUpsertContext(PRIMARY_KEY_COLUMNS, COMPARISON_COLUMNS,
        DELETE_RECORD_COLUMN, HashFunction.NONE, METADATA_TTL, DELETED_KEYS_TTL, UpsertConfig.Mode.FULL);

    LocalValidDocIdsSnapshotMetadata metadata =
        LocalValidDocIdsSnapshotMetadata.fromUpsertContext(PARTITION_ID, originalContext);
    metadata.persist(TEMP_DIR);

    // Create context with different upsert mode
    UpsertContext newContext = createMockUpsertContext(PRIMARY_KEY_COLUMNS, COMPARISON_COLUMNS,
        DELETE_RECORD_COLUMN, HashFunction.NONE, METADATA_TTL, DELETED_KEYS_TTL, UpsertConfig.Mode.PARTIAL);

    LocalValidDocIdsSnapshotMetadata readMetadata =
        LocalValidDocIdsSnapshotMetadata.fromDirectory(TEMP_DIR, PARTITION_ID);
    assertNotNull(readMetadata);
    assertFalse(readMetadata.isCompatible(newContext, "testTable"));
  }

  @Test
  public void testIsCompatibleDifferentNumPartitions()
      throws IOException {
    UpsertContext originalContext = createMockUpsertContext(PRIMARY_KEY_COLUMNS, COMPARISON_COLUMNS,
        DELETE_RECORD_COLUMN, HashFunction.NONE, METADATA_TTL, DELETED_KEYS_TTL, UpsertConfig.Mode.FULL,
        NUM_PARTITIONS);

    LocalValidDocIdsSnapshotMetadata metadata =
        LocalValidDocIdsSnapshotMetadata.fromUpsertContext(PARTITION_ID, originalContext);
    metadata.persist(TEMP_DIR);

    // Create context with different number of partitions
    UpsertContext newContext = createMockUpsertContext(PRIMARY_KEY_COLUMNS, COMPARISON_COLUMNS,
        DELETE_RECORD_COLUMN, HashFunction.NONE, METADATA_TTL, DELETED_KEYS_TTL, UpsertConfig.Mode.FULL,
        NUM_PARTITIONS * 2);

    LocalValidDocIdsSnapshotMetadata readMetadata =
        LocalValidDocIdsSnapshotMetadata.fromDirectory(TEMP_DIR, PARTITION_ID);
    assertNotNull(readMetadata);
    assertFalse(readMetadata.isCompatible(newContext, "testTable"));
  }

  @Test
  public void testEquals() {
    UpsertContext context = createMockUpsertContext(PRIMARY_KEY_COLUMNS, COMPARISON_COLUMNS, DELETE_RECORD_COLUMN,
        HashFunction.NONE, METADATA_TTL, DELETED_KEYS_TTL, UpsertConfig.Mode.FULL);

    LocalValidDocIdsSnapshotMetadata metadata1 =
        LocalValidDocIdsSnapshotMetadata.fromUpsertContext(PARTITION_ID, context);
    LocalValidDocIdsSnapshotMetadata metadata2 =
        LocalValidDocIdsSnapshotMetadata.fromUpsertContext(PARTITION_ID, context);

    // Set same creation time for comparison
    metadata1.setCreationTime(12345678L);
    metadata2.setCreationTime(12345678L);

    assertEquals(metadata1, metadata2);
    assertEquals(metadata1.hashCode(), metadata2.hashCode());
  }
}
