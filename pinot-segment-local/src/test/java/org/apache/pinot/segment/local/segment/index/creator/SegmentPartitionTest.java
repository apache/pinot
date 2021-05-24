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
package org.apache.pinot.segment.local.segment.index.creator;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.common.request.FilterOperator;
import org.apache.pinot.segment.local.indexsegment.immutable.ImmutableSegmentLoader;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.segment.local.segment.readers.GenericRowRecordReader;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
import org.apache.pinot.segment.spi.index.metadata.ColumnMetadata;
import org.apache.pinot.segment.spi.index.metadata.SegmentMetadataImpl;
import org.apache.pinot.segment.spi.partition.ModuloPartitionFunction;
import org.apache.pinot.spi.config.table.ColumnPartitionConfig;
import org.apache.pinot.spi.config.table.SegmentPartitionConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.DimensionFieldSpec;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.spi.utils.ReadMode;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


/**
 * Unit test for Segment partitioning:
 * <ul>
 *   <li> Test to cover segment generation and metadata.</li>
 *   <li> Test to cover segment pruning during query execution. </li>
 * </ul>
 */
public class SegmentPartitionTest {
  private static final String SEGMENT_DIR_NAME =
      System.getProperty("java.io.tmpdir") + File.separator + "partitionTest";
  private static final String TABLE_NAME = "partitionTable";
  private static final String SEGMENT_NAME = "partition";
  private static final String SEGMENT_PATH = SEGMENT_DIR_NAME + File.separator + SEGMENT_NAME;

  private static final int NUM_ROWS = 1001;
  private static final String PARTITIONED_COLUMN_NAME = "partitionedColumn";

  private static final String NON_PARTITIONED_COLUMN_NAME = "nonPartitionedColumn";
  private static final int NUM_PARTITIONS = 20; // For modulo function
  private static final int PARTITION_DIVISOR = 5; // Allowed partition values
  private static final String PARTITION_FUNCTION_NAME = "MoDuLo";

  private final Set<Integer> _expectedPartitions = new HashSet<>();
  private IndexSegment _segment;

  @BeforeClass
  public void init()
      throws Exception {
    buildSegment();
  }

  /**
   * Clean up after test
   */
  @AfterClass
  public void cleanup() {
    FileUtils.deleteQuietly(new File(SEGMENT_DIR_NAME));
  }

  /**
   * Unit test:
   * <ul>
   *   <li> Partitioning metadata is written out correctly for column where all values comply to partition scheme. </li>
   *   <li> Partitioning metadata is dropped for column that does not comply to partitioning scheme. </li>
   *   <li> Partitioning metadata is not written out for column for which the metadata was not specified. </li>
   * </ul>
   */
  @Test
  public void testMetadata() {
    SegmentMetadataImpl metadata = (SegmentMetadataImpl) _segment.getSegmentMetadata();
    ColumnMetadata columnMetadata = metadata.getColumnMetadataFor(PARTITIONED_COLUMN_NAME);

    Assert.assertTrue(columnMetadata.getPartitionFunction() instanceof ModuloPartitionFunction);

    Set<Integer> actualPartitions = columnMetadata.getPartitions();
    Assert.assertEquals(actualPartitions, _expectedPartitions);

    columnMetadata = metadata.getColumnMetadataFor(NON_PARTITIONED_COLUMN_NAME);
    Assert.assertNull(columnMetadata.getPartitionFunction());
    Assert.assertNull(columnMetadata.getPartitions());
  }

  /**
   * Unit test for {@link SegmentPartitionConfig} that tests the following:
   * <ul>
   *   <li> Conversion from/to JSON string. </li>
   *   <li> Function names, values and ranges are as expected. </li>
   * </ul>
   * @throws IOException
   */
  @Test
  public void testSegmentPartitionConfig()
      throws IOException {
    Map<String, ColumnPartitionConfig> expectedMap = new HashMap<>();

    for (int i = 0; i < 10; i++) {
      String partitionColumn = "column_" + i;
      String partitionFunction = "function_" + i;
      expectedMap.put(partitionColumn, new ColumnPartitionConfig(partitionFunction, i + 1));
    }

    SegmentPartitionConfig expectedConfig = new SegmentPartitionConfig(expectedMap);
    SegmentPartitionConfig actualConfig =
        JsonUtils.stringToObject(expectedConfig.toJsonString(), SegmentPartitionConfig.class);

    for (Map.Entry<String, ColumnPartitionConfig> entry : actualConfig.getColumnPartitionMap().entrySet()) {
      String partitionColumn = entry.getKey();

      ColumnPartitionConfig expectedColumnConfig = expectedMap.get(partitionColumn);
      Assert.assertNotNull(expectedColumnConfig);
      ColumnPartitionConfig actualColumnConfig = entry.getValue();

      Assert.assertEquals(actualColumnConfig.getFunctionName(), expectedColumnConfig.getFunctionName());
    }

    // Test that adding new fields does not break json de-serialization.
    String jsonStringWithNewField =
        "{\"columnPartitionMap\":{\"column_0\":{\"functionName\":\"function\",\"numPartitions\":10,\"newField\":\"newValue\"}}}";
    String jsonStringWithoutNewField =
        "{\"columnPartitionMap\":{\"column_0\":{\"functionName\":\"function\",\"numPartitions\":10}}}";

    Assert.assertEquals(jsonStringWithoutNewField,
        JsonUtils.stringToObject(jsonStringWithNewField, SegmentPartitionConfig.class).toJsonString());
  }

  private String buildQuery(String tableName, String columnName, int predicateValue) {
    return "select count(*) from " + tableName + " where " + columnName + " = " + predicateValue;
  }

  private String buildAndQuery(String tableName, String partitionColumn, int partitionedColumnValue,
      String nonPartitionColumn, int nonPartitionedColumnValue, FilterOperator operator) {
    return "select count(*) from " + tableName + " where " + partitionColumn + " = " + partitionedColumnValue + " "
        + operator + " " + nonPartitionColumn + " = " + nonPartitionedColumnValue;
  }

  private List buildPartitionList(int partition) {
    return Collections.singletonList(partition);
  }

  /**
   * Helper method to build a segment for testing:
   * <ul>
   *   <li> First column is partitioned correctly as per the specification in the segment generation config. </li>
   *   <li> Second column is not partitioned as per the specification in the segment generation config. </li>
   *   <li> Third column does not have any partitioning config in the segment generation config. </li>
   * </ul>
   * @throws Exception
   */
  private void buildSegment()
      throws Exception {
    Schema schema = new Schema();
    schema.addField(new DimensionFieldSpec(PARTITIONED_COLUMN_NAME, FieldSpec.DataType.INT, true));
    schema.addField(new DimensionFieldSpec(NON_PARTITIONED_COLUMN_NAME, FieldSpec.DataType.INT, true));
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME).build();

    Random random = new Random();
    Map<String, ColumnPartitionConfig> partitionFunctionMap = new HashMap<>();

    partitionFunctionMap
        .put(PARTITIONED_COLUMN_NAME, new ColumnPartitionConfig(PARTITION_FUNCTION_NAME, NUM_PARTITIONS));

    SegmentPartitionConfig segmentPartitionConfig = new SegmentPartitionConfig(partitionFunctionMap);
    SegmentGeneratorConfig config = new SegmentGeneratorConfig(tableConfig, schema);

    config.setOutDir(SEGMENT_DIR_NAME);
    config.setSegmentName(SEGMENT_NAME);
    config.setTableName(TABLE_NAME);
    config.setSegmentPartitionConfig(segmentPartitionConfig);

    List<GenericRow> rows = new ArrayList<>(NUM_ROWS);
    for (int i = 0; i < NUM_ROWS; i++) {
      HashMap<String, Object> map = new HashMap<>();

      int partition = random.nextInt(PARTITION_DIVISOR);
      int validPartitionedValue = random.nextInt(100) * 20 + partition;
      _expectedPartitions.add(partition);
      map.put(PARTITIONED_COLUMN_NAME, validPartitionedValue);
      map.put(NON_PARTITIONED_COLUMN_NAME, validPartitionedValue);

      GenericRow genericRow = new GenericRow();
      genericRow.init(map);
      rows.add(genericRow);
    }

    SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
    driver.init(config, new GenericRowRecordReader(rows));
    driver.build();
    _segment = ImmutableSegmentLoader.load(new File(SEGMENT_PATH), ReadMode.mmap);
  }
}
