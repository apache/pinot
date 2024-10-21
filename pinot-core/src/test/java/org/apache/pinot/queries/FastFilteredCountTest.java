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
package org.apache.pinot.queries;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.IntStream;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.core.common.Operator;
import org.apache.pinot.core.operator.query.FastFilteredCountOperator;
import org.apache.pinot.segment.local.indexsegment.immutable.ImmutableSegmentLoader;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.segment.local.segment.index.loader.IndexLoadingConfig;
import org.apache.pinot.segment.local.segment.readers.GenericRowRecordReader;
import org.apache.pinot.segment.spi.ImmutableSegment;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
import org.apache.pinot.spi.config.table.FieldConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;


public class FastFilteredCountTest extends BaseQueriesTest {

  private static final File INDEX_DIR = new File(FileUtils.getTempDirectory(), "FastFilteredCountTest");
  private static final String RAW_TABLE_NAME = "testTable";
  private static final String SEGMENT_NAME = "testSegment";

  private static final int NUM_RECORDS = 1000;
  private static final int BUCKET_SIZE = 8;
  private static final String SORTED_COLUMN = "sorted";
  private static final String CLASSIFICATION_COLUMN = "class";
  private static final String TEXT_COLUMN = "textCol";
  private static final String JSON_COLUMN = "jsonCol";
  private static final String INT_RANGE_COLUMN = "intRangeCol";

  private static final Schema SCHEMA = new Schema.SchemaBuilder()
      .addSingleValueDimension(SORTED_COLUMN, FieldSpec.DataType.INT)
      .addSingleValueDimension(CLASSIFICATION_COLUMN, FieldSpec.DataType.INT)
      .addSingleValueDimension(TEXT_COLUMN, FieldSpec.DataType.STRING)
      .addSingleValueDimension(JSON_COLUMN, FieldSpec.DataType.JSON)
      .addSingleValueDimension(INT_RANGE_COLUMN, FieldSpec.DataType.INT)
      .build();

  private static final TableConfig TABLE_CONFIG =
      new TableConfigBuilder(TableType.OFFLINE)
          .setTableName(RAW_TABLE_NAME)
          .setSortedColumn(SORTED_COLUMN)
          .build();

  private IndexSegment _indexSegment;
  private List<IndexSegment> _indexSegments;

  @Override
  protected String getFilter() {
    return "";
  }

  @Override
  protected IndexSegment getIndexSegment() {
    return _indexSegment;
  }

  @Override
  protected List<IndexSegment> getIndexSegments() {
    return _indexSegments;
  }

  @BeforeClass
  public void setUp()
      throws Exception {
    FileUtils.deleteQuietly(INDEX_DIR);

    List<GenericRow> records = new ArrayList<>(NUM_RECORDS);
    for (int i = 0; i < NUM_RECORDS; i++) {
      GenericRow record = new GenericRow();
      record.putValue(CLASSIFICATION_COLUMN, i % BUCKET_SIZE);
      record.putValue(SORTED_COLUMN, i);
      record.putValue(TEXT_COLUMN, "text" + (i % BUCKET_SIZE));
      record.putValue(JSON_COLUMN, "{\"field\":" + (i % BUCKET_SIZE) + "}");
      record.putValue(INT_RANGE_COLUMN, NUM_RECORDS - i);
      records.add(record);
    }

    SegmentGeneratorConfig segmentGeneratorConfig = new SegmentGeneratorConfig(TABLE_CONFIG, SCHEMA);
    segmentGeneratorConfig.setTableName(RAW_TABLE_NAME);
    segmentGeneratorConfig.setSegmentName(SEGMENT_NAME);
    segmentGeneratorConfig.setOutDir(INDEX_DIR.getPath());

    SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
    driver.init(segmentGeneratorConfig, new GenericRowRecordReader(records));
    driver.build();

    List<FieldConfig> fieldConfigs = List.of(
        new FieldConfig(TEXT_COLUMN, FieldConfig.EncodingType.DICTIONARY, FieldConfig.IndexType.TEXT, null, null));

    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(RAW_TABLE_NAME)
        .setInvertedIndexColumns(List.of(CLASSIFICATION_COLUMN, SORTED_COLUMN))
        .setJsonIndexColumns(List.of(JSON_COLUMN)).setRangeIndexColumns(List.of(INT_RANGE_COLUMN))
        .setFieldConfigList(fieldConfigs).build();

    IndexLoadingConfig indexLoadingConfig = new IndexLoadingConfig(tableConfig, SCHEMA);

    ImmutableSegment immutableSegment =
        ImmutableSegmentLoader.load(new File(INDEX_DIR, SEGMENT_NAME), indexLoadingConfig);
    _indexSegment = immutableSegment;
    _indexSegments = List.of(immutableSegment, immutableSegment);
  }

  @AfterClass
  public void tearDown()
      throws IOException {
    FileUtils.deleteDirectory(INDEX_DIR);
  }

  @DataProvider
  public Object[][] testCases() {
    int bucketCount = NUM_RECORDS / BUCKET_SIZE;
    int bucketCountComplement = NUM_RECORDS - NUM_RECORDS / BUCKET_SIZE;
    int min = 20;
    int max = NUM_RECORDS - 20;
    String allBuckets = Arrays.toString(IntStream.range(0, BUCKET_SIZE).toArray())
        .replace('[', '(').replace(']', ')');
    String twoBuckets = Arrays.toString(new int[] {0, 7})
        .replace('[', '(').replace(']', ')');
    return new Object[][] {
        {"select count(*) from " + RAW_TABLE_NAME, NUM_RECORDS},
        {"select count(*) from " + RAW_TABLE_NAME
            + " where " + CLASSIFICATION_COLUMN + " = 1", bucketCount},
        {"select count(*) from " + RAW_TABLE_NAME
            + " where JSON_MATCH(" + JSON_COLUMN + ", '\"$.field\"=1')", bucketCount},
        {"select count(*) from " + RAW_TABLE_NAME
            + " where NOT JSON_MATCH(" + JSON_COLUMN + ", '\"$.field\"=1')", bucketCountComplement},
        {"select count(*) from " + RAW_TABLE_NAME
            + " where TEXT_MATCH(" + TEXT_COLUMN + ", 'text1')", bucketCount},
        {"select count(*) from " + RAW_TABLE_NAME
            + " where NOT TEXT_MATCH(" + TEXT_COLUMN + ", 'text1')", bucketCountComplement},
        {"select count(*) from " + RAW_TABLE_NAME + " where " + SORTED_COLUMN + " = 1", 1},
        {"select count(*) from " + RAW_TABLE_NAME
            + " where " + SORTED_COLUMN + " between " + min + " and " + max, max - min + 1},
        {"select count(*) from " + RAW_TABLE_NAME
            + " where " + SORTED_COLUMN + " not between " + min + " and " + max, NUM_RECORDS - (max - min + 1)},
        {"select count(*) from " + RAW_TABLE_NAME
            + " where " + SORTED_COLUMN + " in " + allBuckets, BUCKET_SIZE},
        {"select count(*) from " + RAW_TABLE_NAME
            + " where " + SORTED_COLUMN + " in " + allBuckets
            + " and " + CLASSIFICATION_COLUMN + " in " + allBuckets, BUCKET_SIZE},
        {"select count(*) from " + RAW_TABLE_NAME
            + " where " + CLASSIFICATION_COLUMN + " <> 1", bucketCountComplement},
        {"select count(*) from " + RAW_TABLE_NAME
            + " where " + CLASSIFICATION_COLUMN + " in " + twoBuckets, 2 * bucketCount},
        {"select count(*) from " + RAW_TABLE_NAME
            + " where " + CLASSIFICATION_COLUMN + " not in " + twoBuckets, NUM_RECORDS - 2 * bucketCount},
        {"select count(*) from " + RAW_TABLE_NAME
            + " where " + CLASSIFICATION_COLUMN + " in " + twoBuckets
            + " and " + SORTED_COLUMN + " < " + (NUM_RECORDS / 2), bucketCount},
        {"select count(*) from " + RAW_TABLE_NAME
            + " where " + SORTED_COLUMN + " = 1"
            + " and " + CLASSIFICATION_COLUMN + " = 1", 1},
        {"select count(*) from " + RAW_TABLE_NAME
            + " where " + SORTED_COLUMN + " = 1"
            + " and " + CLASSIFICATION_COLUMN + " <> 1", 0},
        {"select count(*) from " + RAW_TABLE_NAME
            + " where " + SORTED_COLUMN + " = 1"
            + " and " + CLASSIFICATION_COLUMN + " <> 0", 1},
        {"select count(*) from " + RAW_TABLE_NAME
            + " where TEXT_MATCH(" + TEXT_COLUMN + ", 'text0')"
            + " and " + CLASSIFICATION_COLUMN + " <> 1", bucketCount},
        {"select count(*) from " + RAW_TABLE_NAME
            + " where TEXT_MATCH(" + TEXT_COLUMN + ", 'text0')"
            + " or " + CLASSIFICATION_COLUMN + " <> 1", bucketCountComplement},
        {"select count(*) from " + RAW_TABLE_NAME
            + " where TEXT_MATCH(" + TEXT_COLUMN + ", 'text0')"
            + " or " + CLASSIFICATION_COLUMN + " = 1", 2 * bucketCount},
        {"select count(*) from " + RAW_TABLE_NAME
            + " where not TEXT_MATCH(" + TEXT_COLUMN + ", 'text0')"
            + " or " + CLASSIFICATION_COLUMN + " = 1", bucketCountComplement},
        {"select count(*) from " + RAW_TABLE_NAME
            + " where TEXT_MATCH(" + TEXT_COLUMN + ", 'text0')"
            + " or JSON_MATCH(" + JSON_COLUMN + ", '\"$.field\"=1')"
            + " or " + CLASSIFICATION_COLUMN + " = 2", 3 * bucketCount},
        {"select count(*) from " + RAW_TABLE_NAME
            + " where not TEXT_MATCH(" + TEXT_COLUMN + ", 'text0')"
            + " or not JSON_MATCH(" + JSON_COLUMN + ", '\"$.field\"=0')"
            + " or " + CLASSIFICATION_COLUMN + " <> 0", bucketCountComplement},
        {"select count(*) from " + RAW_TABLE_NAME
            + " where TEXT_MATCH(" + TEXT_COLUMN + ", 'text0')"
            + " or JSON_MATCH(" + JSON_COLUMN + ", '\"$.field\"=1')"
            + " or " + CLASSIFICATION_COLUMN + " <> 2", bucketCountComplement},
        {"select count(*) from " + RAW_TABLE_NAME
            + " where not TEXT_MATCH(" + TEXT_COLUMN + ", 'text0')"
            + " or not JSON_MATCH(" + JSON_COLUMN + ", '\"$.field\"=1')"
            + " or " + CLASSIFICATION_COLUMN + " <> 2", NUM_RECORDS},
        {"select count(*) from " + RAW_TABLE_NAME
            + " where not TEXT_MATCH(" + TEXT_COLUMN + ", 'text0')"
            + " or JSON_MATCH(" + JSON_COLUMN + ", '\"$.field\"=1')"
            + " or " + CLASSIFICATION_COLUMN + " <> 2", NUM_RECORDS},
        {"select count(*) from " + RAW_TABLE_NAME
            + " where not TEXT_MATCH(" + TEXT_COLUMN + ", 'text0')"
            + " or JSON_MATCH(" + JSON_COLUMN + ", '\"$.field\"=1')"
            + " or " + CLASSIFICATION_COLUMN + " = 0", NUM_RECORDS},
        {"select count(*) from " + RAW_TABLE_NAME
            + " where " + SORTED_COLUMN + " <> 1"
            + " and " + CLASSIFICATION_COLUMN + " = 1", bucketCount - 1},
        {"select count(*) from " + RAW_TABLE_NAME
            + " where " + SORTED_COLUMN + " >= 0"
            + " and " + CLASSIFICATION_COLUMN + " = 1", bucketCount},
        {"select count(*) from " + RAW_TABLE_NAME
            + " where " + SORTED_COLUMN + " > 1"
            + " and " + CLASSIFICATION_COLUMN + " = 1", bucketCount - 1},
        {"select count(*) from " + RAW_TABLE_NAME
            + " where " + SORTED_COLUMN + " >= 0"
            + " and " + CLASSIFICATION_COLUMN + " <> 1", bucketCountComplement},
        {"select count(*) from " + RAW_TABLE_NAME
            + " where not TEXT_MATCH(" + TEXT_COLUMN + ", 'text0')"
            + " and " + CLASSIFICATION_COLUMN + " <> 1", NUM_RECORDS - 2 * bucketCount},
        {"select count(*) from " + RAW_TABLE_NAME
            + " where not TEXT_MATCH(" + TEXT_COLUMN + ", 'text0')"
            + " or " + CLASSIFICATION_COLUMN + " <> 1", NUM_RECORDS},
        {"select count(*) from " + RAW_TABLE_NAME
            + " where not TEXT_MATCH(" + TEXT_COLUMN + ", 'text0')"
            + " or " + CLASSIFICATION_COLUMN + " <> 0", bucketCountComplement},
        {"select count(*) from " + RAW_TABLE_NAME
            + " where " + SORTED_COLUMN + " >= 0"
            + " or " + CLASSIFICATION_COLUMN + " <> 0", NUM_RECORDS},
        {"select count(*) from " + RAW_TABLE_NAME
            + " where TEXT_MATCH(" + TEXT_COLUMN + ",  'text0')"
            + " and " + SORTED_COLUMN + " <> 1", bucketCount},
        {"select count(*) from " + RAW_TABLE_NAME
            + " where TEXT_MATCH(" + TEXT_COLUMN + ",  'text1')"
            + " and " + SORTED_COLUMN + " <> 1", bucketCount - 1},
        {"select count(*) from " + RAW_TABLE_NAME
            + " where TEXT_MATCH(" + TEXT_COLUMN + ",  'text0')"
            + " and " + CLASSIFICATION_COLUMN + " <> 1", bucketCount},
        {"select count(*) from " + RAW_TABLE_NAME
            + " where " + SORTED_COLUMN + " >= 500"
            + " and " + CLASSIFICATION_COLUMN + " <> 0"
            + " and not TEXT_MATCH(" + TEXT_COLUMN + ", 'text0')", bucketCountComplement / 2 + 1},
        {"select count(*) from " + RAW_TABLE_NAME
            + " where " + SORTED_COLUMN + " >= 500"
            + " and " + CLASSIFICATION_COLUMN + " <> 0"
            + " and TEXT_MATCH(" + TEXT_COLUMN + ", 'text0')", 0},
        {"select count(*) from " + RAW_TABLE_NAME
            + " where " + SORTED_COLUMN + " < " + bucketCount
            + " and " + CLASSIFICATION_COLUMN + " <> 0", bucketCount - bucketCount / BUCKET_SIZE - 1},
        {"select count(*) from " + RAW_TABLE_NAME
            + " where " + SORTED_COLUMN + " >= " + bucketCount
            + " and " + CLASSIFICATION_COLUMN + " <> 0", bucketCountComplement - bucketCountComplement / BUCKET_SIZE},
        {"select count(*) from " + RAW_TABLE_NAME
            + " where " + SORTED_COLUMN + " < " + (BUCKET_SIZE - 1)
            + " and " + CLASSIFICATION_COLUMN + " = " + (BUCKET_SIZE - 1), 0},
        {"select count(*) from " + RAW_TABLE_NAME
            + " where " + SORTED_COLUMN + " >= " + (BUCKET_SIZE - 2)
            + " and " + CLASSIFICATION_COLUMN + " = " + (BUCKET_SIZE - 2), bucketCount},
        {"select count(*) from " + RAW_TABLE_NAME
            + " where " + SORTED_COLUMN + " >= " + min
            + " and " + SORTED_COLUMN + " < " + max
            + " and " + CLASSIFICATION_COLUMN + " = 0", bucketCount - (min + NUM_RECORDS - max) / BUCKET_SIZE},
        {"select count(*) from " + RAW_TABLE_NAME
            + " where " + SORTED_COLUMN + " >= 500"
            + " and " + CLASSIFICATION_COLUMN + " <> 0"
            + " and not JSON_MATCH(" + JSON_COLUMN + ", '\"$.field\"=0')"
            + " and not TEXT_MATCH(" + TEXT_COLUMN + ", 'text0')", bucketCountComplement / 2 + 1},
        {"select count(*) from " + RAW_TABLE_NAME
            + " where " + INT_RANGE_COLUMN + " >= " + min
            + " and " + INT_RANGE_COLUMN + " < " + max, max - min},
        {"select count(*) from " + RAW_TABLE_NAME
            + " where " + INT_RANGE_COLUMN + " < " + max, max - 1},
        {"select count(*) from " + RAW_TABLE_NAME
            + " where " + INT_RANGE_COLUMN + " not between " + min + " and " + max, NUM_RECORDS - max + min - 1},
        {"select count(*) from " + RAW_TABLE_NAME
            + " where " + INT_RANGE_COLUMN + " between " + min + " and " + max
            + " and " + CLASSIFICATION_COLUMN + " = 0", bucketCount - (min + NUM_RECORDS - max) / BUCKET_SIZE},
        {"select count(*) from " + RAW_TABLE_NAME
            + " where " + INT_RANGE_COLUMN + " not between " + min + " and " + max
            + " and " + CLASSIFICATION_COLUMN + " = 0", (min + NUM_RECORDS - max) / BUCKET_SIZE}
    };
  }

  @Test(dataProvider = "testCases")
  public void test(String query, int expectedCount) {
    Operator<?> operator = getOperator(query);
    assertTrue(operator instanceof FastFilteredCountOperator);
    List<Object> aggregationResult = ((FastFilteredCountOperator) operator).nextBlock().getResults();
    assertNotNull(aggregationResult);
    assertEquals(aggregationResult.size(), 1);
    assertEquals(((Number) aggregationResult.get(0)).intValue(), expectedCount, query);
  }
}
