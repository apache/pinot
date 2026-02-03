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
import org.apache.pinot.spi.config.table.HashFunction;
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

    TableConfig tableConfig = new TableConfigBuilder(TableType.REALTIME)
        .setTableName("testTable")
        .setUpsertConfig(upsertConfig)
        .build();
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
  public void testIsCompatibleWithSameConfig()
      throws IOException {
    UpsertContext context = createMockUpsertContext(PRIMARY_KEY_COLUMNS, COMPARISON_COLUMNS, DELETE_RECORD_COLUMN,
        HashFunction.NONE, METADATA_TTL, DELETED_KEYS_TTL, UpsertConfig.Mode.FULL);

    LocalValidDocIdsSnapshotMetadata metadata =
        LocalValidDocIdsSnapshotMetadata.fromUpsertContext(PARTITION_ID, context);
    metadata.persist(TEMP_DIR);

    LocalValidDocIdsSnapshotMetadata readMetadata =
        LocalValidDocIdsSnapshotMetadata.fromDirectory(TEMP_DIR, PARTITION_ID);
    assertNotNull(readMetadata);
    assertTrue(readMetadata.isCompatibleWith(context, "testTable"));
  }

  @Test
  public void testIsCompatibleWithDifferentPrimaryKeys()
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
    assertFalse(readMetadata.isCompatibleWith(newContext, "testTable"));
  }

  @Test
  public void testIsCompatibleWithDifferentComparisonColumns()
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
    assertFalse(readMetadata.isCompatibleWith(newContext, "testTable"));
  }

  @Test
  public void testIsCompatibleWithDifferentDeleteRecordColumn()
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
    assertFalse(readMetadata.isCompatibleWith(newContext, "testTable"));
  }

  @Test
  public void testIsCompatibleWithDifferentMetadataTTL()
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
    assertFalse(readMetadata.isCompatibleWith(newContext, "testTable"));
  }

  @Test
  public void testIsCompatibleWithDifferentDeletedKeysTTL()
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
    assertFalse(readMetadata.isCompatibleWith(newContext, "testTable"));
  }

  @Test
  public void testIsCompatibleWithDifferentHashFunction()
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
    assertFalse(readMetadata.isCompatibleWith(newContext, "testTable"));
  }

  @Test
  public void testIsCompatibleWithDifferentUpsertMode()
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
    assertFalse(readMetadata.isCompatibleWith(newContext, "testTable"));
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

  @Test
  public void testToString() {
    UpsertContext context = createMockUpsertContext(PRIMARY_KEY_COLUMNS, COMPARISON_COLUMNS, DELETE_RECORD_COLUMN,
        HashFunction.NONE, METADATA_TTL, DELETED_KEYS_TTL, UpsertConfig.Mode.FULL);

    LocalValidDocIdsSnapshotMetadata metadata =
        LocalValidDocIdsSnapshotMetadata.fromUpsertContext(PARTITION_ID, context);

    String str = metadata.toString();
    assertTrue(str.contains("LocalValidDocIdsSnapshotMetadata"));
    assertTrue(str.contains("_primaryKeyColumns"));
    assertTrue(str.contains("_comparisonColumns"));
  }
}
