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
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.core.operator.blocks.IntermediateResultsBlock;
import org.apache.pinot.core.operator.query.AggregationOperator;
import org.apache.pinot.segment.local.indexsegment.immutable.ImmutableSegmentLoader;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.segment.local.segment.index.loader.IndexLoadingConfig;
import org.apache.pinot.segment.local.segment.readers.GenericRowRecordReader;
import org.apache.pinot.segment.local.utils.GeometrySerializer;
import org.apache.pinot.segment.local.utils.GeometryUtils;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
import org.apache.pinot.segment.spi.index.creator.H3IndexConfig;
import org.apache.pinot.spi.config.table.FieldConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.locationtech.jts.geom.Coordinate;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


/**
 * Queries test for H3 index.
 */
public class H3IndexQueriesTest extends BaseQueriesTest {
  private static final File INDEX_DIR = new File(FileUtils.getTempDirectory(), "H3IndexQueriesTest");
  private static final String RAW_TABLE_NAME = "testTable";
  private static final String SEGMENT_NAME = "testSegment";
  private static final Random RANDOM = new Random();

  private static final int NUM_RECORDS = 10000;

  private static final String H3_INDEX_COLUMN = "h3Column";
  private static final String NON_H3_INDEX_COLUMN = "nonH3Column";
  private static final Schema SCHEMA =
      new Schema.SchemaBuilder().setSchemaName(RAW_TABLE_NAME).addSingleValueDimension(H3_INDEX_COLUMN, DataType.BYTES)
          .addSingleValueDimension(NON_H3_INDEX_COLUMN, DataType.BYTES).build();
  private static final Map<String, String> H3_INDEX_PROPERTIES = Collections.singletonMap("resolutions", "5");
  private static final TableConfig TABLE_CONFIG = new TableConfigBuilder(TableType.OFFLINE).setTableName(RAW_TABLE_NAME)
      .setFieldConfigList(Collections.singletonList(
          new FieldConfig(H3_INDEX_COLUMN, FieldConfig.EncodingType.DICTIONARY, FieldConfig.IndexType.H3,
              H3_INDEX_PROPERTIES))).build();

  private IndexSegment _indexSegment;

  @Override
  protected String getFilter() {
    throw new UnsupportedOperationException();
  }

  @Override
  protected IndexSegment getIndexSegment() {
    return _indexSegment;
  }

  @Override
  protected List<IndexSegment> getIndexSegments() {
    throw new UnsupportedOperationException();
  }

  @BeforeClass
  public void setUp()
      throws Exception {
    FileUtils.deleteDirectory(INDEX_DIR);

    List<GenericRow> records = new ArrayList<>(NUM_RECORDS);
    for (int i = 0; i < NUM_RECORDS; i++) {
      double longitude = -122.5 + RANDOM.nextDouble();
      double latitude = 37 + RANDOM.nextDouble();
      byte[] value = GeometrySerializer
          .serialize(GeometryUtils.GEOGRAPHY_FACTORY.createPoint(new Coordinate(longitude, latitude)));
      GenericRow record = new GenericRow();
      record.putValue(H3_INDEX_COLUMN, value);
      record.putValue(NON_H3_INDEX_COLUMN, value);
      records.add(record);
    }

    SegmentGeneratorConfig segmentGeneratorConfig = new SegmentGeneratorConfig(TABLE_CONFIG, SCHEMA);
    segmentGeneratorConfig.setTableName(RAW_TABLE_NAME);
    segmentGeneratorConfig.setSegmentName(SEGMENT_NAME);
    segmentGeneratorConfig.setOutDir(INDEX_DIR.getPath());

    SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
    driver.init(segmentGeneratorConfig, new GenericRowRecordReader(records));
    driver.build();

    IndexLoadingConfig indexLoadingConfig = new IndexLoadingConfig();
    indexLoadingConfig
        .setH3IndexConfigs(Collections.singletonMap(H3_INDEX_COLUMN, new H3IndexConfig(H3_INDEX_PROPERTIES)));
    _indexSegment = ImmutableSegmentLoader.load(new File(INDEX_DIR, SEGMENT_NAME), indexLoadingConfig);
  }

  @Test
  public void testH3Index()
      throws IOException {
    // Invalid upper bound
    {
      for (String query : Arrays
          .asList("SELECT COUNT(*) FROM testTable WHERE ST_Distance(h3Column, ST_Point(-122, 37.5, 1)) < -1",
              "SELECT COUNT(*) FROM testTable WHERE ST_Distance(h3Column, ST_Point(-122, 37.5, 1)) BETWEEN 100 AND 50")) {
        AggregationOperator aggregationOperator = getOperatorForSqlQuery(query);
        IntermediateResultsBlock resultsBlock = aggregationOperator.nextBlock();
        // Expect 0 entries scanned in filter
        QueriesTestUtils
            .testInnerSegmentExecutionStatistics(aggregationOperator.getExecutionStatistics(), 0, 0, 0, NUM_RECORDS);
        List<Object> aggregationResult = resultsBlock.getAggregationResult();
        Assert.assertNotNull(aggregationResult);
        Assert.assertEquals((long) aggregationResult.get(0), 0);
      }
    }

    // No bound
    {
      String query = "SELECT COUNT(*) FROM testTable WHERE ST_Distance(h3Column, ST_Point(-122, 37.5, 1)) > -1";
      AggregationOperator aggregationOperator = getOperatorForSqlQuery(query);
      IntermediateResultsBlock resultsBlock = aggregationOperator.nextBlock();
      // Expect 0 entries scanned in filter
      QueriesTestUtils
          .testInnerSegmentExecutionStatistics(aggregationOperator.getExecutionStatistics(), NUM_RECORDS, 0, 0,
              NUM_RECORDS);
      List<Object> aggregationResult = resultsBlock.getAggregationResult();
      Assert.assertNotNull(aggregationResult);
      Assert.assertEquals((long) aggregationResult.get(0), NUM_RECORDS);
    }

    // Lower bound only
    // 1km
    testQuery("SELECT COUNT(*) FROM testTable WHERE ST_Distance(%s, ST_Point(-122, 37.5, 1)) > 1000");
    // 5km
    testQuery("SELECT COUNT(*) FROM testTable WHERE ST_Distance(%s, ST_Point(-122, 37.5, 1)) > 5000");
    // 10km
    testQuery("SELECT COUNT(*) FROM testTable WHERE ST_Distance(%s, ST_Point(-122, 37.5, 1)) > 10000");
    // 20km
    testQuery("SELECT COUNT(*) FROM testTable WHERE ST_Distance(%s, ST_Point(-122, 37.5, 1)) > 20000");
    // 50km
    testQuery("SELECT COUNT(*) FROM testTable WHERE ST_Distance(%s, ST_Point(-122, 37.5, 1)) > 50000");
    // 100km
    testQuery("SELECT COUNT(*) FROM testTable WHERE ST_Distance(%s, ST_Point(-122, 37.5, 1)) > 100000");

    // Upper bound only
    // 1km
    testQuery("SELECT COUNT(*) FROM testTable WHERE ST_Distance(%s, ST_Point(-122, 37.5, 1)) < 1000");
    // 5km
    testQuery("SELECT COUNT(*) FROM testTable WHERE ST_Distance(%s, ST_Point(-122, 37.5, 1)) < 5000");
    // 10km
    testQuery("SELECT COUNT(*) FROM testTable WHERE ST_Distance(%s, ST_Point(-122, 37.5, 1)) < 10000");
    // 20km
    testQuery("SELECT COUNT(*) FROM testTable WHERE ST_Distance(%s, ST_Point(-122, 37.5, 1)) < 20000");
    // 50km
    testQuery("SELECT COUNT(*) FROM testTable WHERE ST_Distance(%s, ST_Point(-122, 37.5, 1)) < 50000");
    // 100km
    testQuery("SELECT COUNT(*) FROM testTable WHERE ST_Distance(%s, ST_Point(-122, 37.5, 1)) < 100000");

    // Both lower and upper bound
    // 1-5km
    testQuery("SELECT COUNT(*) FROM testTable WHERE ST_Distance(%s, ST_Point(-122, 37.5, 1)) BETWEEN 1000 AND 5000");
    // 5-10km
    testQuery("SELECT COUNT(*) FROM testTable WHERE ST_Distance(%s, ST_Point(-122, 37.5, 1)) BETWEEN 5000 AND 10000");
    // 10-20km
    testQuery("SELECT COUNT(*) FROM testTable WHERE ST_Distance(%s, ST_Point(-122, 37.5, 1)) BETWEEN 10000 AND 20000");
    // 20-50km
    testQuery("SELECT COUNT(*) FROM testTable WHERE ST_Distance(%s, ST_Point(-122, 37.5, 1)) BETWEEN 20000 AND 50000");
    // 50-100km
    testQuery("SELECT COUNT(*) FROM testTable WHERE ST_Distance(%s, ST_Point(-122, 37.5, 1)) BETWEEN 50000 AND 100000");

    // Distance is too large, should fall back to scan-based ExpressionFilterOperator
    {
      String query = "SELECT COUNT(*) FROM testTable WHERE ST_Distance(h3Column, ST_Point(-122, 37.5, 1)) < 10000000";
      AggregationOperator aggregationOperator = getOperatorForSqlQuery(query);
      IntermediateResultsBlock resultsBlock = aggregationOperator.nextBlock();
      // Expect 10000 entries scanned in filter
      QueriesTestUtils
          .testInnerSegmentExecutionStatistics(aggregationOperator.getExecutionStatistics(), NUM_RECORDS, NUM_RECORDS,
              0, NUM_RECORDS);
      List<Object> aggregationResult = resultsBlock.getAggregationResult();
      Assert.assertNotNull(aggregationResult);
      Assert.assertEquals((long) aggregationResult.get(0), NUM_RECORDS);
    }
  }

  private void testQuery(String queryTemplate) {
    String h3IndexQuery = String.format(queryTemplate, H3_INDEX_COLUMN);
    String nonH3IndexQuery = String.format(queryTemplate, NON_H3_INDEX_COLUMN);
    AggregationOperator h3IndexOperator = getOperatorForSqlQuery(h3IndexQuery);
    AggregationOperator nonH3IndexOperator = getOperatorForSqlQuery(nonH3IndexQuery);
    IntermediateResultsBlock h3IndexResultsBlock = h3IndexOperator.nextBlock();
    IntermediateResultsBlock nonH3IndexResultsBlock = nonH3IndexOperator.nextBlock();
    // Expect less than 10000 entries scanned in filter
    Assert.assertTrue(h3IndexOperator.getExecutionStatistics().getNumEntriesScannedInFilter() < NUM_RECORDS);
    Assert.assertEquals(nonH3IndexOperator.getExecutionStatistics().getNumEntriesScannedInFilter(), NUM_RECORDS);
    Assert.assertEquals(h3IndexResultsBlock.getAggregationResult(), nonH3IndexResultsBlock.getAggregationResult());
  }

  @AfterClass
  public void tearDown()
      throws IOException {
    _indexSegment.destroy();
    FileUtils.deleteDirectory(INDEX_DIR);
  }
}
