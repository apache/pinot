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
package org.apache.pinot.segment.local.indexsegment.mutable;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.pinot.common.partition.function.ModuloPartitionFunction;
import org.apache.pinot.segment.local.PinotBuffersAfterClassCheckRule;
import org.apache.pinot.segment.local.data.manager.TableDataManager;
import org.apache.pinot.segment.local.dedup.DedupContext;
import org.apache.pinot.segment.local.dedup.DedupRecordInfo;
import org.apache.pinot.segment.local.dedup.PartitionDedupMetadataManager;
import org.apache.pinot.segment.local.realtime.impl.forward.FixedByteMVMutableForwardIndex;
import org.apache.pinot.segment.local.realtime.impl.invertedindex.RealtimeLuceneTextIndexSearcherPool;
import org.apache.pinot.segment.local.upsert.PartitionUpsertMetadataManager;
import org.apache.pinot.segment.local.upsert.RecordInfo;
import org.apache.pinot.segment.local.upsert.TableUpsertMetadataManager;
import org.apache.pinot.segment.local.upsert.TableUpsertMetadataManagerFactory;
import org.apache.pinot.segment.spi.datasource.DataSource;
import org.apache.pinot.segment.spi.index.creator.VectorIndexConfig;
import org.apache.pinot.segment.spi.index.reader.Dictionary;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.config.table.UpsertConfig;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.data.readers.PrimaryKey;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.mockito.Mockito.*;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;


/// Verifies that the mutable segment enforces the maximum number of values per fixed-byte multi-value entry. Each test
/// owns its mutable segment, so no state is shared between test invocations.
public class MutableSegmentImplNumMultiValuesValidationTest implements PinotBuffersAfterClassCheckRule {
  private static final String PARTITION_COLUMN = "partitionColumn";
  private static final String PRIMARY_KEY_COLUMN = "primaryKey";
  private static final String COMPARISON_COLUMN = "comparisonColumn";
  private static final String MV_COLUMN = "mvColumn";
  private static final int MAX_NUM_MULTI_VALUES = 1000;
  private static final int SMALL_VECTOR_DIMENSION = 768;
  private static final int VECTOR_DIMENSION = 1536;

  @BeforeClass
  public void setUpSearcherPool() {
    RealtimeLuceneTextIndexSearcherPool.init(1);
  }

  @Test
  public void testRejectOversizedMultiValueRowBeforeWrites()
      throws Exception {
    Schema schema = new Schema.SchemaBuilder().setSchemaName("numMultiValuesValidation")
        .addMultiValueDimension(MV_COLUMN, DataType.INT)
        .build();
    MutableSegmentImpl mutableSegment = MutableSegmentImplTestUtils.createMutableSegmentImpl(schema);
    try {
      Object[] firstValues = createValues(3, 0);
      mutableSegment.index(createRow(firstValues), null);

      DataSource dataSource = mutableSegment.getDataSource(MV_COLUMN);
      FixedByteMVMutableForwardIndex forwardIndex = (FixedByteMVMutableForwardIndex) dataSource.getForwardIndex();

      IllegalStateException exception = expectThrows(IllegalStateException.class,
          () -> mutableSegment.index(createRow(createValues(MAX_NUM_MULTI_VALUES + 1, 10)), null));
      assertTrue(exception.getMessage().contains(MV_COLUMN));
      assertTrue(exception.getMessage().contains(Integer.toString(MAX_NUM_MULTI_VALUES + 1)));
      assertTrue(exception.getMessage().contains("exceeds the maximum allowed: " + MAX_NUM_MULTI_VALUES),
          exception.getMessage());
      assertEquals(mutableSegment.getNumDocsIndexed(), 1);
      assertValues(dataSource, forwardIndex, 0, firstValues);

      Object[] valuesAtLimit = createValues(MAX_NUM_MULTI_VALUES, 10000);
      mutableSegment.index(createRow(valuesAtLimit), null);
      assertEquals(mutableSegment.getNumDocsIndexed(), 2);
      assertValues(dataSource, forwardIndex, 1, valuesAtLimit);
    } finally {
      mutableSegment.destroy();
    }
  }

  @Test
  public void testAcceptVectorDimensionAboveDefaultMultiValueLimit()
      throws Exception {
    Schema schema = new Schema.SchemaBuilder().setSchemaName("vectorNumMultiValuesValidation")
        .addMultiValueDimension(MV_COLUMN, DataType.FLOAT)
        .build();
    VectorIndexConfig vectorIndexConfig =
        new VectorIndexConfig(false, "HNSW", VECTOR_DIMENSION, 1, VectorIndexConfig.VectorDistanceFunction.COSINE,
            Map.of("vectorIndexType", "HNSW", "vectorDimension", Integer.toString(VECTOR_DIMENSION), "commitDocs",
                "1"));
    MutableSegmentImpl mutableSegment =
        MutableSegmentImplTestUtils.createMutableSegmentImplWithVectorIndexConfigs(schema, Set.of(MV_COLUMN), Set.of(),
            Set.of(), Map.of(MV_COLUMN, vectorIndexConfig), null);
    try {
      Object[] vector = createFloatValues(VECTOR_DIMENSION);
      mutableSegment.index(createRow(vector), null);

      DataSource dataSource = mutableSegment.getDataSource(MV_COLUMN);
      FixedByteMVMutableForwardIndex forwardIndex = (FixedByteMVMutableForwardIndex) dataSource.getForwardIndex();
      float[] queryVector = toPrimitiveFloatArray(vector);
      assertEquals(forwardIndex.getFloatMV(0), queryVector);
      assertNotNull(dataSource.getVectorIndex());
      assertEquals(dataSource.getVectorIndex().getDocIds(queryVector, 1).toArray(), new int[]{0});
      assertEquals(mutableSegment.getNumDocsIndexed(), 1);
    } finally {
      mutableSegment.destroy();
    }
  }

  @Test
  public void testRejectVectorAboveConfiguredDimensionBeforeWrites() {
    Schema schema = new Schema.SchemaBuilder().setSchemaName("smallVectorNumMultiValuesValidation")
        .addMultiValueDimension(MV_COLUMN, DataType.FLOAT)
        .build();
    VectorIndexConfig vectorIndexConfig =
        new VectorIndexConfig(false, "HNSW", SMALL_VECTOR_DIMENSION, 1, VectorIndexConfig.VectorDistanceFunction.COSINE,
            Map.of("vectorIndexType", "HNSW", "vectorDimension", Integer.toString(SMALL_VECTOR_DIMENSION), "commitDocs",
                "1"));
    MutableSegmentImpl mutableSegment =
        MutableSegmentImplTestUtils.createMutableSegmentImplWithVectorIndexConfigs(schema, Set.of(MV_COLUMN), Set.of(),
            Set.of(), Map.of(MV_COLUMN, vectorIndexConfig), null);
    try {
      expectThrows(IllegalStateException.class,
          () -> mutableSegment.index(createRow(createFloatValues(SMALL_VECTOR_DIMENSION + 1)), null));

      DataSource dataSource = mutableSegment.getDataSource(MV_COLUMN);
      assertEquals(dataSource.getDataSourceMetadata().getNumValues(), 0);
      assertTrue(dataSource.getVectorIndex().getDocIds(new float[SMALL_VECTOR_DIMENSION], 1).isEmpty());
      assertEquals(mutableSegment.getNumDocsIndexed(), 0);
    } finally {
      mutableSegment.destroy();
    }
  }

  @Test
  public void testRejectOversizedMultiValueRowBeforeDedupUpdate()
      throws Exception {
    Schema schema = new Schema.SchemaBuilder().setSchemaName("dedupNumMultiValuesValidation")
        .addSingleValueDimension(PRIMARY_KEY_COLUMN, DataType.STRING)
        .addMultiValueDimension(MV_COLUMN, DataType.INT)
        .setPrimaryKeyColumns(List.of(PRIMARY_KEY_COLUMN))
        .build();
    PartitionDedupMetadataManager dedupMetadataManager = mock(PartitionDedupMetadataManager.class);
    when(dedupMetadataManager.getContext()).thenReturn(mock(DedupContext.class));
    Set<PrimaryKey> seenPrimaryKeys = new HashSet<>();
    when(dedupMetadataManager.checkRecordPresentOrUpdate(any(DedupRecordInfo.class), any())).thenAnswer(invocation -> {
      DedupRecordInfo recordInfo = invocation.getArgument(0);
      return !seenPrimaryKeys.add(recordInfo.getPrimaryKey());
    });

    MutableSegmentImpl mutableSegment =
        MutableSegmentImplTestUtils.createMutableSegmentImpl(schema, false, null, null, dedupMetadataManager);
    try {
      String primaryKey = "same-key";
      expectThrows(IllegalStateException.class,
          () -> mutableSegment.index(createDedupRow(primaryKey, createValues(MAX_NUM_MULTI_VALUES + 1, 0)), null));
      assertTrue(seenPrimaryKeys.isEmpty());
      assertEquals(mutableSegment.getNumDocsIndexed(), 0);

      Object[] validValues = createValues(3, 10000);
      mutableSegment.index(createDedupRow(primaryKey, validValues), null);
      assertEquals(seenPrimaryKeys.size(), 1);
      assertEquals(mutableSegment.getNumDocsIndexed(), 1);

      DataSource dataSource = mutableSegment.getDataSource(MV_COLUMN);
      assertValues(dataSource, (FixedByteMVMutableForwardIndex) dataSource.getForwardIndex(), 0, validValues);
    } finally {
      mutableSegment.destroy();
    }
  }

  @Test(dataProvider = "collectionMergeStrategies")
  public void testRejectOversizedMultiValueRowProducedByPartialUpsert(UpsertConfig.Strategy strategy)
      throws Exception {
    Schema schema = new Schema.SchemaBuilder().setSchemaName("partialUpsertNumMultiValuesValidation")
        .addSingleValueDimension(PRIMARY_KEY_COLUMN, DataType.STRING)
        .addMultiValueDimension(MV_COLUMN, DataType.INT)
        .addDateTime(COMPARISON_COLUMN, DataType.LONG, "1:MILLISECONDS:EPOCH", "1:MILLISECONDS")
        .setPrimaryKeyColumns(List.of(PRIMARY_KEY_COLUMN))
        .build();
    UpsertConfig upsertConfig = new UpsertConfig(UpsertConfig.Mode.PARTIAL);
    upsertConfig.setComparisonColumns(List.of(COMPARISON_COLUMN));
    upsertConfig.setPartialUpsertStrategies(Map.of(MV_COLUMN, strategy));
    TableConfig tableConfig =
        new TableConfigBuilder(TableType.REALTIME).setTableName("partialUpsertNumMultiValuesValidation")
            .setTimeColumnName(COMPARISON_COLUMN)
            .setUpsertConfig(upsertConfig)
            .setNullHandlingEnabled(true)
            .build();
    TableUpsertMetadataManager tableUpsertMetadataManager =
        TableUpsertMetadataManagerFactory.create(new PinotConfiguration(), tableConfig, schema,
            mock(TableDataManager.class), null);
    PartitionUpsertMetadataManager upsertMetadataManager =
        spy(tableUpsertMetadataManager.getOrCreatePartitionManager(0));
    MutableSegmentImpl mutableSegment =
        MutableSegmentImplTestUtils.createMutableSegmentImpl(schema, true, COMPARISON_COLUMN, upsertMetadataManager,
            null);
    try {
      String primaryKey = "same-key";
      expectThrows(IllegalStateException.class,
          () -> mutableSegment.index(createUpsertRow(primaryKey, 0L, createValues(MAX_NUM_MULTI_VALUES + 1, 0)), null));
      assertEquals(mutableSegment.getNumDocsIndexed(), 0);
      verify(upsertMetadataManager, times(0)).updateRecord(any(GenericRow.class), any(RecordInfo.class));
      verify(upsertMetadataManager, times(0)).addRecord(eq(mutableSegment), any(RecordInfo.class));

      Object[] firstValues = createValues(600, 0);
      mutableSegment.index(createUpsertRow(primaryKey, 1L, firstValues), null);

      DataSource dataSource = mutableSegment.getDataSource(MV_COLUMN);
      FixedByteMVMutableForwardIndex forwardIndex = (FixedByteMVMutableForwardIndex) dataSource.getForwardIndex();
      int cardinalityBeforeRejection = dataSource.getDictionary().length();
      int numValuesBeforeRejection = dataSource.getDataSourceMetadata().getNumValues();
      ImmutableRoaringBitmap validDocIds = mutableSegment.getValidDocIds().getMutableRoaringBitmap();
      assertEquals(validDocIds.toArray(), new int[]{0});
      verify(upsertMetadataManager, times(1)).addRecord(eq(mutableSegment), any(RecordInfo.class));

      expectThrows(IllegalStateException.class,
          () -> mutableSegment.index(createUpsertRow(primaryKey, 2L, createValues(500, 600)), null));

      DataSource dataSourceAfterRejection = mutableSegment.getDataSource(MV_COLUMN);
      assertEquals(mutableSegment.getNumDocsIndexed(), 1);
      assertEquals(dataSourceAfterRejection.getDictionary().length(), cardinalityBeforeRejection);
      assertEquals(dataSourceAfterRejection.getDataSourceMetadata().getNumValues(), numValuesBeforeRejection);
      assertEquals(mutableSegment.getValidDocIds().getMutableRoaringBitmap().toArray(), new int[]{0});
      verify(upsertMetadataManager, times(1)).addRecord(eq(mutableSegment), any(RecordInfo.class));
      assertValues(dataSourceAfterRejection, forwardIndex, 0, firstValues);

      Object[] validUpdate = createValues(3, 600);
      mutableSegment.index(createUpsertRow(primaryKey, 3L, validUpdate), null);
      assertEquals(mutableSegment.getNumDocsIndexed(), 2);
      assertEquals(mutableSegment.getValidDocIds().getMutableRoaringBitmap().toArray(), new int[]{1});
      verify(upsertMetadataManager, times(2)).addRecord(eq(mutableSegment), any(RecordInfo.class));
      assertValues(mutableSegment.getDataSource(MV_COLUMN), forwardIndex, 1, createValues(603, 0));
    } finally {
      mutableSegment.destroy();
      upsertMetadataManager.stop();
      upsertMetadataManager.close();
    }
  }

  @Test
  public void testRejectOversizedMultiValueRowBeforePartitionTracking()
      throws Exception {
    Schema schema = new Schema.SchemaBuilder().setSchemaName("partitionNumMultiValuesValidation")
        .addSingleValueDimension(PARTITION_COLUMN, DataType.INT)
        .addMultiValueDimension(MV_COLUMN, DataType.INT)
        .build();
    MutableSegmentImpl mutableSegment = MutableSegmentImplTestUtils.createMutableSegmentImpl(schema, PARTITION_COLUMN,
        new ModuloPartitionFunction(4, null), 0, false);
    try {
      assertEquals(mutableSegment.getDataSource(PARTITION_COLUMN).getDataSourceMetadata().getPartitions(), Set.of(0));
      expectThrows(IllegalStateException.class,
          () -> mutableSegment.index(createPartitionedRow(1, createValues(MAX_NUM_MULTI_VALUES + 1, 0)), null));
      assertEquals(mutableSegment.getNumDocsIndexed(), 0);
      assertEquals(mutableSegment.getDataSource(PARTITION_COLUMN).getDataSourceMetadata().getPartitions(), Set.of(0));

      Object[] validValues = createValues(3, 0);
      mutableSegment.index(createPartitionedRow(1, validValues), null);
      assertEquals(mutableSegment.getNumDocsIndexed(), 1);
      assertEquals(mutableSegment.getDataSource(PARTITION_COLUMN).getDataSourceMetadata().getPartitions(),
          Set.of(0, 1));
      DataSource dataSource = mutableSegment.getDataSource(MV_COLUMN);
      assertValues(dataSource, (FixedByteMVMutableForwardIndex) dataSource.getForwardIndex(), 0, validValues);
    } finally {
      mutableSegment.destroy();
    }
  }

  @DataProvider(name = "collectionMergeStrategies")
  private static Object[][] collectionMergeStrategies() {
    return new Object[][]{
        {UpsertConfig.Strategy.APPEND}, {UpsertConfig.Strategy.UNION}
    };
  }

  private static GenericRow createRow(Object[] values) {
    GenericRow row = new GenericRow();
    row.putValue(MV_COLUMN, values);
    return row;
  }

  private static GenericRow createPartitionedRow(int partitionValue, Object[] values) {
    GenericRow row = createRow(values);
    row.putValue(PARTITION_COLUMN, partitionValue);
    return row;
  }

  private static GenericRow createUpsertRow(String primaryKey, long comparisonValue, Object[] values) {
    GenericRow row = createDedupRow(primaryKey, values);
    row.putValue(COMPARISON_COLUMN, comparisonValue);
    return row;
  }

  private static GenericRow createDedupRow(String primaryKey, Object[] values) {
    GenericRow row = createRow(values);
    row.putValue(PRIMARY_KEY_COLUMN, primaryKey);
    return row;
  }

  private static Object[] createValues(int numValues, int offset) {
    Object[] values = new Object[numValues];
    for (int i = 0; i < numValues; i++) {
      values[i] = offset + i;
    }
    return values;
  }

  private static Object[] createFloatValues(int numValues) {
    Object[] values = new Object[numValues];
    for (int i = 0; i < numValues; i++) {
      values[i] = (float) i;
    }
    return values;
  }

  private static float[] toPrimitiveFloatArray(Object[] values) {
    float[] result = new float[values.length];
    for (int i = 0; i < values.length; i++) {
      result[i] = (float) values[i];
    }
    return result;
  }

  private static void assertValues(DataSource dataSource, FixedByteMVMutableForwardIndex forwardIndex, int docId,
      Object[] expectedValues) {
    Dictionary dictionary = dataSource.getDictionary();
    assertNotNull(dictionary);
    int[] dictIds = forwardIndex.getDictIdMV(docId);
    assertEquals(dictIds.length, expectedValues.length);
    for (int i = 0; i < expectedValues.length; i++) {
      assertEquals(dictionary.get(dictIds[i]), expectedValues[i]);
    }
  }
}
