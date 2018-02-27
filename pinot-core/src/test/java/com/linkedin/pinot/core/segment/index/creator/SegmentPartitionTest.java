/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.core.segment.index.creator;

import com.linkedin.pinot.common.config.ColumnPartitionConfig;
import com.linkedin.pinot.common.config.SegmentPartitionConfig;
import com.linkedin.pinot.common.data.DimensionFieldSpec;
import com.linkedin.pinot.common.data.FieldSpec;
import com.linkedin.pinot.common.data.Schema;
import com.linkedin.pinot.common.request.BrokerRequest;
import com.linkedin.pinot.common.request.FilterOperator;
import com.linkedin.pinot.common.segment.ReadMode;
import com.linkedin.pinot.common.utils.request.FilterQueryTree;
import com.linkedin.pinot.common.utils.request.RequestUtils;
import com.linkedin.pinot.core.data.GenericRow;
import com.linkedin.pinot.core.data.readers.GenericRowRecordReader;
import com.linkedin.pinot.core.indexsegment.IndexSegment;
import com.linkedin.pinot.core.indexsegment.generator.SegmentGeneratorConfig;
import com.linkedin.pinot.core.query.pruner.PartitionSegmentPruner;
import com.linkedin.pinot.core.segment.creator.impl.SegmentIndexCreationDriverImpl;
import com.linkedin.pinot.core.segment.index.ColumnMetadata;
import com.linkedin.pinot.core.segment.index.SegmentMetadataImpl;
import com.linkedin.pinot.core.segment.index.loader.Loaders;
import com.linkedin.pinot.pql.parsers.Pql2Compiler;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.math.IntRange;
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
  private static final int MAX_PARTITION_VALUE = (PARTITION_DIVISOR - 1);
  private static final String EXPECTED_PARTITION_VALUE_STRING = "[0 " + MAX_PARTITION_VALUE + "]";
  private static final String EXPECTED_PARTITION_FUNCTION = "MoDuLo";
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
   * @throws Exception
   */
  @Test
  public void testMetadata()
      throws Exception {
    SegmentMetadataImpl metadata = (SegmentMetadataImpl) _segment.getSegmentMetadata();
    ColumnMetadata columnMetadata = metadata.getColumnMetadataFor(PARTITIONED_COLUMN_NAME);

    Assert.assertEquals(columnMetadata.getPartitionFunction().toString().toLowerCase(),
        EXPECTED_PARTITION_FUNCTION.toLowerCase());

    List<IntRange> partitionValues = columnMetadata.getPartitionRanges();
    Assert.assertEquals(partitionValues.size(), 1);
    List<IntRange> expectedPartitionValues = ColumnPartitionConfig.rangesFromString(EXPECTED_PARTITION_VALUE_STRING);

    IntRange actualValue = partitionValues.get(0);
    IntRange expectedPartitionValue = expectedPartitionValues.get(0);
    Assert.assertEquals(actualValue.getMinimumInteger(), expectedPartitionValue.getMinimumInteger());
    Assert.assertEquals(actualValue.getMaximumInteger(), expectedPartitionValue.getMaximumInteger());

    columnMetadata = metadata.getColumnMetadataFor(NON_PARTITIONED_COLUMN_NAME);
    Assert.assertNull(columnMetadata.getPartitionFunction());
    Assert.assertNull(columnMetadata.getPartitionRanges());
  }

  /**
   * Unit test for {@link PartitionSegmentPruner}.
   * <ul>
   *   <li> Generates queries with equality predicate on partitioned column with random values. </li>
   *   <li> Ensures that column values that are in partition range ([0 5]) do not prune the segment,
   *        whereas other values do. </li>
   *   <li> Ensures that predicates on non-partitioned columns do not prune the segment. </li>
   * </ul>
   */
  @Test
  public void testPruner() {
    Pql2Compiler compiler = new Pql2Compiler();
    PartitionSegmentPruner pruner = new PartitionSegmentPruner();

    Random random = new Random(System.nanoTime());
    for (int i = 0; i < 1000; i++) {
      int columnValue = Math.abs(random.nextInt());

      // Test for partitioned column.
      String query = buildQuery(TABLE_NAME, PARTITIONED_COLUMN_NAME, columnValue);
      BrokerRequest brokerRequest = compiler.compileToBrokerRequest(query);
      FilterQueryTree filterQueryTree = RequestUtils.generateFilterQueryTree(brokerRequest);

      Assert.assertEquals(pruner.prune(_segment, filterQueryTree), (columnValue % NUM_PARTITIONS > MAX_PARTITION_VALUE),
          "Failed for column value: " + columnValue);

      // Test for non partitioned column.
      query = buildQuery(TABLE_NAME, NON_PARTITIONED_COLUMN_NAME, columnValue);
      brokerRequest = compiler.compileToBrokerRequest(query);
      filterQueryTree = RequestUtils.generateFilterQueryTree(brokerRequest);
      Assert.assertFalse(pruner.prune(_segment, filterQueryTree));

      // Test for AND query: Segment can be pruned out if partitioned column has value outside of range.
      int partitionColumnValue = Math.abs(random.nextInt());
      int nonPartitionColumnValue = random.nextInt();
      query = buildAndQuery(TABLE_NAME, PARTITIONED_COLUMN_NAME, partitionColumnValue, NON_PARTITIONED_COLUMN_NAME,
          nonPartitionColumnValue, FilterOperator.AND);

      brokerRequest = compiler.compileToBrokerRequest(query);
      filterQueryTree = RequestUtils.generateFilterQueryTree(brokerRequest);
      Assert.assertEquals(pruner.prune(_segment, filterQueryTree),
          (partitionColumnValue % NUM_PARTITIONS) > MAX_PARTITION_VALUE);

      // Test for OR query: Segment should never be pruned as there's an OR with non partitioned column.
      query = buildAndQuery(TABLE_NAME, PARTITIONED_COLUMN_NAME, partitionColumnValue, NON_PARTITIONED_COLUMN_NAME,
          nonPartitionColumnValue, FilterOperator.OR);
      brokerRequest = compiler.compileToBrokerRequest(query);
      filterQueryTree = RequestUtils.generateFilterQueryTree(brokerRequest);
      Assert.assertFalse(pruner.prune(_segment, filterQueryTree));
    }
  }

  /**
   * Unit test for utility the converts String ranges into IntRanges and back.
   * <ul>
   *   <li> Generates a list of String ranges</li>
   *   <li> Ensures that conversion to IntRanges is as expected</li>
   *   <li> Ensures that the IntRanges when converted back to String ranges are as expected. </li>
   * </ul>
   */
  @Test
  public void testStringRangeConversions() {
    Random random = new Random();

    for (int i = 0; i < 1000; i++) {
      int numRanges = 1 + random.nextInt(1000);
      String[] ranges = new String[numRanges];
      List<IntRange> expected = new ArrayList<>(numRanges);
      StringBuilder builder = new StringBuilder();

      for (int j = 0; j < numRanges; j++) {
        int start = random.nextInt();
        int end = random.nextInt();

        // Generate random ranges such that start <= end.
        if (start > end) {
          start ^= end;
          end = start ^ end;
          start = start ^ end;
        }

        ranges[j] = buildRangeString(start, end);
        expected.add(new IntRange(start, end));

        builder.append(ranges[j]);
        if (j < numRanges - 1) {
          builder.append(ColumnPartitionConfig.PARTITION_VALUE_DELIMITER);
        }
      }
      String expectedString = builder.toString();

      List<IntRange> actual = ColumnPartitionConfig.rangesFromString(ranges);
      Assert.assertEquals(actual, expected);

      String actualString = ColumnPartitionConfig.rangesToString(actual);
      Assert.assertEquals(actualString, expectedString);
    }
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
    SegmentPartitionConfig actualConfig = SegmentPartitionConfig.fromJsonString(expectedConfig.toJsonString());

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
        SegmentPartitionConfig.fromJsonString(jsonStringWithNewField).toJsonString());
  }

  private String buildQuery(String tableName, String columnName, int predicateValue) {
    return "select count(*) from " + tableName + " where " + columnName + " = " + predicateValue;
  }

  private String buildAndQuery(String tableName, String partitionColumn, int partitionedColumnValue,
      String nonPartitionColumn, int nonPartitionedColumnValue, FilterOperator operator) {
    return "select count(*) from " + tableName + " where " + partitionColumn + " = " + partitionedColumnValue + " "
        + operator + " " + nonPartitionColumn + " = " + nonPartitionedColumnValue;
  }

  private String buildRangeString(int start, int end) {
    return "[" + start + " " + end + "]";
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

    Random random = new Random();
    Map<String, ColumnPartitionConfig> partitionFunctionMap = new HashMap<>();

    partitionFunctionMap.put(PARTITIONED_COLUMN_NAME, new ColumnPartitionConfig(EXPECTED_PARTITION_FUNCTION,
        NUM_PARTITIONS));

    SegmentPartitionConfig segmentPartitionConfig = new SegmentPartitionConfig(partitionFunctionMap);
    SegmentGeneratorConfig config = new SegmentGeneratorConfig(schema);

    config.setOutDir(SEGMENT_DIR_NAME);
    config.setSegmentName(SEGMENT_NAME);
    config.setTableName(TABLE_NAME);
    config.setSegmentPartitionConfig(segmentPartitionConfig);

    List<GenericRow> rows = new ArrayList<>(NUM_ROWS);
    for (int i = 0; i < NUM_ROWS; i++) {
      HashMap<String, Object> map = new HashMap<>();

      int validPartitionedValue = random.nextInt(100) * 20 + random.nextInt(PARTITION_DIVISOR);
      map.put(PARTITIONED_COLUMN_NAME, validPartitionedValue);
      map.put(NON_PARTITIONED_COLUMN_NAME, validPartitionedValue);

      GenericRow genericRow = new GenericRow();
      genericRow.init(map);
      rows.add(genericRow);
    }

    SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
    driver.init(config, new GenericRowRecordReader(rows, schema));
    driver.build();
    _segment = Loaders.IndexSegment.load(new File(SEGMENT_PATH), ReadMode.mmap);
  }
}
