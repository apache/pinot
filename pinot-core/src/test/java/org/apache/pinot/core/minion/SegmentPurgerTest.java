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
package org.apache.pinot.core.minion;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import org.apache.commons.configuration2.PropertiesConfiguration;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.core.startree.StarTreeUtils;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.segment.local.segment.index.loader.IndexLoadingConfig;
import org.apache.pinot.segment.local.segment.readers.GenericRowRecordReader;
import org.apache.pinot.segment.local.segment.readers.PinotSegmentRecordReader;
import org.apache.pinot.segment.spi.AggregationFunctionType;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorCustomConfigs;
import org.apache.pinot.segment.spi.index.StandardIndexes;
import org.apache.pinot.segment.spi.index.metadata.SegmentMetadataImpl;
import org.apache.pinot.segment.spi.index.startree.AggregationFunctionColumnPair;
import org.apache.pinot.segment.spi.index.startree.AggregationSpec;
import org.apache.pinot.segment.spi.index.startree.StarTreeV2Metadata;
import org.apache.pinot.segment.spi.loader.SegmentDirectoryLoaderContext;
import org.apache.pinot.segment.spi.loader.SegmentDirectoryLoaderRegistry;
import org.apache.pinot.segment.spi.store.SegmentDirectory;
import org.apache.pinot.spi.config.instance.InstanceType;
import org.apache.pinot.spi.config.table.FieldConfig.CompressionCodec;
import org.apache.pinot.spi.config.table.StarTreeAggregationConfig;
import org.apache.pinot.spi.config.table.StarTreeIndexConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.ReadMode;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;


public class SegmentPurgerTest {
  private static final File TEMP_DIR = new File(FileUtils.getTempDirectory(), "SegmentPurgerTest");
  private static final File ORIGINAL_SEGMENT_DIR = new File(TEMP_DIR, "originalSegment");
  private static final File STAR_TREE_SEGMENT_DIR = new File(TEMP_DIR, "starTreeSegment");
  private static final File PURGED_SEGMENT_DIR = new File(TEMP_DIR, "purgedSegment");
  private static final Random RANDOM = new Random();

  private static final int NUM_ROWS = 10000;
  private static final String TABLE_NAME = "testTable";
  private static final String SEGMENT_NAME = "testSegment";
  private static final String STAR_TREE_SEGMENT_NAME = "starTreeSegment";
  private static final String D1 = "d1";
  private static final String D2 = "d2";
  private static final StarTreeIndexConfig STAR_TREE_CONFIG_A =
      new StarTreeIndexConfig(List.of(D1, D2), null, List.of("SUM__d2"), null, 10000);
  private static final StarTreeIndexConfig STAR_TREE_CONFIG_B =
      new StarTreeIndexConfig(List.of(D2, D1), null, List.of("SUM__d1"), null, 5000);

  private TableConfig _tableConfig;
  private Schema _schema;
  private File _originalIndexDir;
  private File _starTreeIndexDir;
  private int _expectedNumRecordsPurged;
  private int _expectedNumRecordsModified;

  @BeforeClass
  public void setUp()
      throws Exception {
    FileUtils.deleteDirectory(TEMP_DIR);

    _tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME)
        .setInvertedIndexColumns(Collections.singletonList(D1)).setCreateInvertedIndexDuringSegmentGeneration(true)
        .build();
    _schema = new Schema.SchemaBuilder().addSingleValueDimension(D1, FieldSpec.DataType.INT)
        .addSingleValueDimension(D2, FieldSpec.DataType.INT).build();

    List<GenericRow> rows = new ArrayList<>(NUM_ROWS);
    for (int i = 0; i < NUM_ROWS; i++) {
      GenericRow row = new GenericRow();
      int value1 = RANDOM.nextInt(100);
      int value2 = RANDOM.nextInt(100);
      if (value1 == 0) {
        _expectedNumRecordsPurged++;
      } else if (value2 == 0) {
        _expectedNumRecordsModified++;
      }
      row.putValue(D1, value1);
      row.putValue(D2, value2);
      rows.add(row);
    }
    GenericRowRecordReader genericRowRecordReader = new GenericRowRecordReader(rows);

    SegmentGeneratorConfig config = new SegmentGeneratorConfig(_tableConfig, _schema);
    config.setOutDir(ORIGINAL_SEGMENT_DIR.getPath());
    config.setSegmentName(SEGMENT_NAME);

    SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
    driver.init(config, genericRowRecordReader, InstanceType.MINION);
    driver.build();
    _originalIndexDir = new File(ORIGINAL_SEGMENT_DIR, SEGMENT_NAME);

    // Create a segment WITH star tree index
    TableConfig starTreeTableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME)
        .setStarTreeIndexConfigs(Collections.singletonList(STAR_TREE_CONFIG_A)).build();
    SegmentGeneratorConfig starTreeConfig = new SegmentGeneratorConfig(starTreeTableConfig, _schema);
    starTreeConfig.setOutDir(STAR_TREE_SEGMENT_DIR.getPath());
    starTreeConfig.setSegmentName(STAR_TREE_SEGMENT_NAME);
    genericRowRecordReader = new GenericRowRecordReader(rows);
    SegmentIndexCreationDriverImpl starTreeDriver = new SegmentIndexCreationDriverImpl();
    starTreeDriver.init(starTreeConfig, genericRowRecordReader);
    starTreeDriver.build();
    _starTreeIndexDir = new File(STAR_TREE_SEGMENT_DIR, STAR_TREE_SEGMENT_NAME);
  }

  @Test
  public void testPurgeSegment()
      throws Exception {
    // Purge records with d1 = 0
    SegmentPurger.RecordPurger recordPurger = row -> row.getValue(D1).equals(0);

    // Modify records with d2 = 0 to d2 = Integer.MAX_VALUE
    SegmentPurger.RecordModifier recordModifier = row -> {
      if (row.getValue(D2).equals(0)) {
        row.putValue(D2, Integer.MAX_VALUE);
        return true;
      } else {
        return false;
      }
    };

    SegmentPurger segmentPurger =
        new SegmentPurger(_originalIndexDir, PURGED_SEGMENT_DIR, _tableConfig, _schema, recordPurger, recordModifier,
            null);
    File purgedIndexDir = segmentPurger.purgeSegment();

    // Check the purge/modify counter in segment purger
    assertEquals(segmentPurger.getNumRecordsPurged(), _expectedNumRecordsPurged);
    assertEquals(segmentPurger.getNumRecordsModified(), _expectedNumRecordsModified);

    // Check crc and index creation time
    SegmentMetadataImpl purgedSegmentMetadata = new SegmentMetadataImpl(purgedIndexDir);
    SegmentMetadataImpl originalSegmentMetadata = new SegmentMetadataImpl(_originalIndexDir);
    assertNotEquals(purgedSegmentMetadata.getCrc(), originalSegmentMetadata.getCrc());
    assertEquals(purgedSegmentMetadata.getIndexCreationTime(), originalSegmentMetadata.getIndexCreationTime());

    try (PinotSegmentRecordReader pinotSegmentRecordReader = new PinotSegmentRecordReader(purgedIndexDir)) {
      int numRecordsRemaining = 0;
      int numRecordsModified = 0;

      GenericRow row = new GenericRow();
      while (pinotSegmentRecordReader.hasNext()) {
        row = pinotSegmentRecordReader.next(row);

        // Purged segment should not have any record with d1 = 0 or d2 = 0
        assertNotEquals(row.getValue(D1), 0);
        assertNotEquals(row.getValue(D2), 0);

        numRecordsRemaining++;
        if (row.getValue(D2).equals(Integer.MAX_VALUE)) {
          numRecordsModified++;
        }
      }

      assertEquals(numRecordsRemaining, NUM_ROWS - _expectedNumRecordsPurged);
      assertEquals(numRecordsModified, _expectedNumRecordsModified);
    }

    // Check inverted index
    Map<String, Object> props = new HashMap<>();
    props.put(IndexLoadingConfig.READ_MODE_KEY, ReadMode.mmap.toString());
    try (SegmentDirectory segmentDirectory = SegmentDirectoryLoaderRegistry.getDefaultSegmentDirectoryLoader()
        .load(purgedIndexDir.toURI(), new SegmentDirectoryLoaderContext.Builder().setTableConfig(_tableConfig)
            .setSegmentName(purgedSegmentMetadata.getName()).setSegmentDirectoryConfigs(new PinotConfiguration(props))
            .build()); SegmentDirectory.Reader reader = segmentDirectory.createReader()) {
      assertTrue(reader.hasIndexFor(D1, StandardIndexes.inverted()));
      assertFalse(reader.hasIndexFor(D2, StandardIndexes.inverted()));
    }
  }

  @Test
  public void testSegmentPurgerWithCustomSegmentGeneratorConfig()
      throws Exception {
    SegmentPurger.RecordPurger recordPurger = row -> row.getValue(D1).equals(0);
    SegmentMetadataImpl segmentMetadata = new SegmentMetadataImpl(_originalIndexDir);

    SegmentPurger segmentPurger =
        new SegmentPurger(_originalIndexDir, PURGED_SEGMENT_DIR, _tableConfig, _schema, recordPurger, null, null);
    segmentPurger.initSegmentGeneratorConfig("currentSegmentName", segmentMetadata);
    assertEquals(segmentPurger.getSegmentGeneratorConfig().getSegmentName(), "currentSegmentName");

    String newSegmentName = "myTable_segment_001";
    SegmentGeneratorCustomConfigs segmentGeneratorCustomConfigs = new SegmentGeneratorCustomConfigs();
    segmentGeneratorCustomConfigs.setSegmentName(newSegmentName);

    // test with custom segment generator configs
    SegmentPurger segmentPurger2 =
        new SegmentPurger(_originalIndexDir, PURGED_SEGMENT_DIR, _tableConfig, _schema, recordPurger, null,
            segmentGeneratorCustomConfigs);
    segmentPurger2.initSegmentGeneratorConfig("currentSegmentName", segmentMetadata);
    assertEquals(segmentPurger2.getSegmentGeneratorConfig().getSegmentName(), newSegmentName);
  }

  // Each row: {segmentHasStarTree, tableStarTreeConfigs, enableDynamic, expectedNull, expectedMaxLeaf}
  // expectedMaxLeaf is only checked when expectedNull=false
  @DataProvider(name = "starTreePreservationCases")
  public Object[][] starTreePreservationCases() {
    return new Object[][]{
        // Segment without star tree, table has config A, dynamic=false → skip (preserve no-star-tree state)
        {false, List.of(STAR_TREE_CONFIG_A), false, true, -1},
        // Segment without star tree, table has config A, dynamic=true → add from table config
        {false, List.of(STAR_TREE_CONFIG_A), true, false, 10000},
        // Segment with star tree (A), table updated to config B, dynamic=false → preserve original A
        {true, List.of(STAR_TREE_CONFIG_B), false, false, 10000},
        // Segment with star tree (A), table updated to config B, dynamic=true → use B
        {true, List.of(STAR_TREE_CONFIG_B), true, false, 5000},
        // Segment with star tree, table removed config, dynamic=false → preserve original
        {true, null, false, false, 10000},
        // Segment with star tree, table removed config, dynamic=true → remove
        {true, null, true, true, -1},
    };
  }

  @Test(dataProvider = "starTreePreservationCases")
  public void testStarTreePreservation(boolean segmentHasStarTree, List<StarTreeIndexConfig> tableStarTreeConfigs,
      boolean enableDynamic, boolean expectedNull, int expectedMaxLeaf)
      throws Exception {
    File indexDir = segmentHasStarTree ? _starTreeIndexDir : _originalIndexDir;
    String segmentName = segmentHasStarTree ? STAR_TREE_SEGMENT_NAME : SEGMENT_NAME;

    TableConfigBuilder builder = new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME);
    if (tableStarTreeConfigs != null) {
      builder.setStarTreeIndexConfigs(tableStarTreeConfigs);
    }
    TableConfig tableConfig = builder.build();
    if (enableDynamic) {
      tableConfig.getIndexingConfig().setEnableDynamicStarTreeCreation(true);
    }

    SegmentMetadataImpl segmentMetadata = new SegmentMetadataImpl(indexDir);
    SegmentPurger.RecordPurger recordPurger = row -> row.getValue(D1).equals(0);
    SegmentPurger segmentPurger =
        new SegmentPurger(indexDir, PURGED_SEGMENT_DIR, tableConfig, _schema, recordPurger, null, null);
    segmentPurger.initSegmentGeneratorConfig(segmentName, segmentMetadata);

    List<StarTreeIndexConfig> resultConfigs = segmentPurger.getSegmentGeneratorConfig().getStarTreeIndexConfigs();
    if (expectedNull) {
      assertNull(resultConfigs);
      assertFalse(segmentPurger.getSegmentGeneratorConfig().isEnableDefaultStarTree());
    } else {
      assertNotNull(resultConfigs);
      assertEquals(resultConfigs.size(), 1);
      assertEquals(resultConfigs.get(0).getMaxLeafRecords(), expectedMaxLeaf);
    }
  }

  @Test
  public void testToStarTreeIndexConfigs() {
    // Tree 1: two aggregations with default spec and skip dimensions
    TreeMap<AggregationFunctionColumnPair, AggregationSpec> specs1 = new TreeMap<>();
    specs1.put(new AggregationFunctionColumnPair(AggregationFunctionType.SUM, "d2"), AggregationSpec.DEFAULT);
    specs1.put(new AggregationFunctionColumnPair(AggregationFunctionType.COUNT, "*"), AggregationSpec.DEFAULT);
    StarTreeV2Metadata metadata1 = createStarTreeMetadata(List.of("d1", "d2"), specs1, 10000, Set.of("d2"));

    // Tree 2: single aggregation, different dimension order
    TreeMap<AggregationFunctionColumnPair, AggregationSpec> specs2 = new TreeMap<>();
    specs2.put(new AggregationFunctionColumnPair(AggregationFunctionType.SUM, "d1"), AggregationSpec.DEFAULT);
    StarTreeV2Metadata metadata2 = createStarTreeMetadata(List.of("d2", "d1"), specs2, 5000, Set.of());

    List<StarTreeIndexConfig> configs = StarTreeUtils.toStarTreeIndexConfigs(List.of(metadata1, metadata2));
    assertEquals(configs.size(), 2);

    // Verify tree 1
    StarTreeIndexConfig config1 = configs.get(0);
    assertEquals(config1.getDimensionsSplitOrder(), List.of("d1", "d2"));
    assertNotNull(config1.getSkipStarNodeCreationForDimensions());
    assertEquals(config1.getSkipStarNodeCreationForDimensions(), List.of("d2"));
    assertEquals(config1.getMaxLeafRecords(), 10000);
    assertNotNull(config1.getFunctionColumnPairs());
    assertTrue(config1.getFunctionColumnPairs().contains("count__*"));
    assertTrue(config1.getFunctionColumnPairs().contains("sum__d2"));
    assertEquals(config1.getAggregationConfigs().size(), 2);

    // Verify tree 2
    StarTreeIndexConfig config2 = configs.get(1);
    assertEquals(config2.getDimensionsSplitOrder(), List.of("d2", "d1"));
    assertNull(config2.getSkipStarNodeCreationForDimensions());
    assertEquals(config2.getMaxLeafRecords(), 5000);
  }

  @Test
  public void testToStarTreeIndexConfigsWithCustomSpec() {
    TreeMap<AggregationFunctionColumnPair, AggregationSpec> aggregationSpecs = new TreeMap<>();
    aggregationSpecs.put(new AggregationFunctionColumnPair(AggregationFunctionType.SUM, "d2"),
        new AggregationSpec(CompressionCodec.LZ4, true, 4, 4096, 1000, Map.of("param1", "value1")));
    StarTreeV2Metadata metadata = createStarTreeMetadata(List.of("d1"), aggregationSpecs, 5000, Set.of());

    List<StarTreeIndexConfig> configs = StarTreeUtils.toStarTreeIndexConfigs(List.of(metadata));
    assertEquals(configs.size(), 1);
    assertEquals(configs.get(0).getMaxLeafRecords(), 5000);

    StarTreeAggregationConfig aggConfig = configs.get(0).getAggregationConfigs().get(0);
    assertEquals(aggConfig.getColumnName(), "d2");
    assertEquals(aggConfig.getAggregationFunction(), "sum");
    assertEquals(aggConfig.getCompressionCodec(), CompressionCodec.LZ4);
    assertTrue(aggConfig.getDeriveNumDocsPerChunk());
    assertEquals((int) aggConfig.getIndexVersion(), 4);
    assertEquals((int) aggConfig.getTargetDocsPerChunk(), 1000);
    assertEquals(aggConfig.getFunctionParameters().get("param1"), "value1");
  }

  private static StarTreeV2Metadata createStarTreeMetadata(List<String> dimensions,
      TreeMap<AggregationFunctionColumnPair, AggregationSpec> aggregationSpecs, int maxLeafRecords,
      Set<String> skipStarNodeCreationForDimensions) {
    PropertiesConfiguration props = new PropertiesConfiguration();
    StarTreeV2Metadata.writeMetadata(props, 100, dimensions, aggregationSpecs, maxLeafRecords,
        skipStarNodeCreationForDimensions);
    return new StarTreeV2Metadata(props);
  }

  @AfterClass
  public void tearDown()
      throws Exception {
    FileUtils.deleteDirectory(TEMP_DIR);
  }
}
