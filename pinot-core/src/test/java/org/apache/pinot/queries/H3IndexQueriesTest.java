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

import com.google.common.collect.ImmutableList;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.core.operator.blocks.results.AggregationResultsBlock;
import org.apache.pinot.core.operator.query.AggregationOperator;
import org.apache.pinot.segment.local.indexsegment.immutable.ImmutableSegmentLoader;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.segment.local.segment.index.loader.IndexLoadingConfig;
import org.apache.pinot.segment.local.segment.readers.GenericRowRecordReader;
import org.apache.pinot.segment.local.utils.GeometrySerializer;
import org.apache.pinot.segment.local.utils.GeometryUtils;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
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
  private static final String H3_INDEX_GEOMETRY_COLUMN = "h3Column_geometry";
  private static final String NON_H3_INDEX_COLUMN = "nonH3Column";
  private static final String NON_H3_INDEX_GEOMETRY_COLUMN = "nonH3Column_geometry";
  private static final Schema SCHEMA =
      new Schema.SchemaBuilder().setSchemaName(RAW_TABLE_NAME).addSingleValueDimension(H3_INDEX_COLUMN, DataType.BYTES)
          .addSingleValueDimension(NON_H3_INDEX_COLUMN, DataType.BYTES)
          .addSingleValueDimension(H3_INDEX_GEOMETRY_COLUMN, DataType.BYTES)
          .addSingleValueDimension(NON_H3_INDEX_GEOMETRY_COLUMN, DataType.BYTES).build();
  private static final Map<String, String> H3_INDEX_PROPERTIES = Collections.singletonMap("resolutions", "5");
  private static final TableConfig TABLE_CONFIG = new TableConfigBuilder(TableType.OFFLINE).setTableName(RAW_TABLE_NAME)
      .setFieldConfigList(ImmutableList
          .of(new FieldConfig(H3_INDEX_COLUMN, FieldConfig.EncodingType.DICTIONARY, FieldConfig.IndexType.H3, null,
                  H3_INDEX_PROPERTIES),
              new FieldConfig(H3_INDEX_GEOMETRY_COLUMN, FieldConfig.EncodingType.DICTIONARY, FieldConfig.IndexType.H3,
                  null, H3_INDEX_PROPERTIES))).build();

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

  public void setUp(List<GenericRow> records)
      throws Exception {
    FileUtils.deleteDirectory(INDEX_DIR);

    SegmentGeneratorConfig segmentGeneratorConfig = new SegmentGeneratorConfig(TABLE_CONFIG, SCHEMA);
    segmentGeneratorConfig.setTableName(RAW_TABLE_NAME);
    segmentGeneratorConfig.setSegmentName(SEGMENT_NAME);
    segmentGeneratorConfig.setOutDir(INDEX_DIR.getPath());

    SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
    driver.init(segmentGeneratorConfig, new GenericRowRecordReader(records));
    driver.build();

    IndexLoadingConfig indexLoadingConfig = new IndexLoadingConfig(TABLE_CONFIG, SCHEMA);
    _indexSegment = ImmutableSegmentLoader.load(new File(INDEX_DIR, SEGMENT_NAME), indexLoadingConfig);
  }

  private void addRecord(List<GenericRow> records, double longitude, double latitude) {
    byte[] value =
        GeometrySerializer.serialize(GeometryUtils.GEOGRAPHY_FACTORY.createPoint(new Coordinate(longitude, latitude)));
    byte[] geometryValue =
        GeometrySerializer.serialize(GeometryUtils.GEOMETRY_FACTORY.createPoint(new Coordinate(longitude, latitude)));
    GenericRow record = new GenericRow();
    record.putValue(H3_INDEX_COLUMN, value);
    record.putValue(NON_H3_INDEX_COLUMN, value);
    record.putValue(H3_INDEX_GEOMETRY_COLUMN, geometryValue);
    record.putValue(NON_H3_INDEX_GEOMETRY_COLUMN, geometryValue);
    records.add(record);
  }

  @Test
  public void testH3Index()
      throws Exception {
    List<GenericRow> records = new ArrayList<>(NUM_RECORDS);
    for (int i = 0; i < NUM_RECORDS; i++) {
      double longitude = -122.5 + RANDOM.nextDouble();
      double latitude = 37 + RANDOM.nextDouble();
      addRecord(records, longitude, latitude);
    }
    setUp(records);

    // Invalid upper bound
    {
      for (String query : Arrays
          .asList("SELECT COUNT(*) FROM testTable WHERE ST_Distance(h3Column, ST_Point(-122, 37.5, 1)) < -1",
              "SELECT COUNT(*) FROM testTable WHERE ST_Distance(h3Column, ST_Point(-122, 37.5, 1)) BETWEEN 100 AND "
                  + "50")) {
        AggregationOperator aggregationOperator = getOperator(query);
        AggregationResultsBlock resultsBlock = aggregationOperator.nextBlock();
        // Expect 0 entries scanned in filter
        QueriesTestUtils
            .testInnerSegmentExecutionStatistics(aggregationOperator.getExecutionStatistics(), 0, 0, 0, NUM_RECORDS);
        List<Object> aggregationResult = resultsBlock.getResults();
        Assert.assertNotNull(aggregationResult);
        Assert.assertEquals((long) aggregationResult.get(0), 0);
      }
    }

    // No bound
    {
      String query = "SELECT COUNT(*) FROM testTable WHERE ST_Distance(h3Column, ST_Point(-122, 37.5, 1)) > -1";
      AggregationOperator aggregationOperator = getOperator(query);
      AggregationResultsBlock resultsBlock = aggregationOperator.nextBlock();
      // Expect 0 entries scanned in filter
      QueriesTestUtils
          .testInnerSegmentExecutionStatistics(aggregationOperator.getExecutionStatistics(), NUM_RECORDS, 0, 0,
              NUM_RECORDS);
      List<Object> aggregationResult = resultsBlock.getResults();
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
      AggregationOperator aggregationOperator = getOperator(query);
      AggregationResultsBlock resultsBlock = aggregationOperator.nextBlock();
      // Expect 10000 entries scanned in filter
      QueriesTestUtils
          .testInnerSegmentExecutionStatistics(aggregationOperator.getExecutionStatistics(), NUM_RECORDS, NUM_RECORDS,
              0, NUM_RECORDS);
      List<Object> aggregationResult = resultsBlock.getResults();
      Assert.assertNotNull(aggregationResult);
      Assert.assertEquals((long) aggregationResult.get(0), NUM_RECORDS);
    }

    {
      // Test st contains in polygon
      testQueryStContain("SELECT COUNT(*) FROM testTable WHERE ST_Contains(ST_GeomFromText('POLYGON ((\n"
          + "             -122.0008564 37.5004316, \n"
          + "             -121.9991291 37.5005168, \n"
          + "             -121.9990325 37.4995294, \n"
          + "             -122.0001268 37.4993506,  \n"
          + "             -122.0008564 37.5004316))'), %s) = 1");

      // negative test
      testQueryStContain("SELECT COUNT(*) FROM testTable WHERE ST_Contains(ST_GeomFromText('POLYGON ((\n"
          + "             -122.0008564 37.5004316, \n"
          + "             -121.9991291 37.5005168, \n"
          + "             -121.9990325 37.4995294, \n"
          + "             -122.0001268 37.4993506,  \n"
          + "             -122.0008564 37.5004316))'), %s) = 0");
    }
    {
      // Test st contains in polygon, doesn't have
      String query = "SELECT COUNT(*) FROM testTable WHERE ST_Contains(ST_GeomFromText('POLYGON ((\n"
          + "             122.0008564 -37.5004316, \n"
          + "             121.9991291 -37.5005168, \n"
          + "             121.9990325 -37.4995294, \n"
          + "             122.0001268 -37.4993506,  \n"
          + "             122.0008564 -37.5004316))'), h3Column_geometry) = 1";
      AggregationOperator aggregationOperator = getOperator(query);
      AggregationResultsBlock resultsBlock = aggregationOperator.nextBlock();
      // Expect 0 entries scanned in filter
      QueriesTestUtils
          .testInnerSegmentExecutionStatistics(aggregationOperator.getExecutionStatistics(), 0, 0, 0,
              NUM_RECORDS);
      List<Object> aggregationResult = resultsBlock.getResults();
      Assert.assertNotNull(aggregationResult);
      Assert.assertEquals((long) aggregationResult.get(0), 0);
    }

    {
      // Test st within in polygon
      testQueryStContain("SELECT COUNT(*) FROM testTable WHERE ST_Within(%s, ST_GeomFromText('POLYGON ((\n"
          + "             -122.0008564 37.5004316, \n"
          + "             -121.9991291 37.5005168, \n"
          + "             -121.9990325 37.4995294, \n"
          + "             -122.0001268 37.4993506,  \n"
          + "             -122.0008564 37.5004316))')) = 1");

      // negative test
      testQueryStContain("SELECT COUNT(*) FROM testTable WHERE ST_Within(%s, ST_GeomFromText('POLYGON ((\n"
          + "             -122.0008564 37.5004316, \n"
          + "             -121.9991291 37.5005168, \n"
          + "             -121.9990325 37.4995294, \n"
          + "             -122.0001268 37.4993506,  \n"
          + "             -122.0008564 37.5004316))')) = 0");
    }
    {
      // Test st within in polygon, doesn't have
      String query = "SELECT COUNT(*) FROM testTable WHERE ST_Within(h3Column_geometry, ST_GeomFromText('POLYGON ((\n"
          + "             122.0008564 -37.5004316, \n"
          + "             121.9991291 -37.5005168, \n"
          + "             121.9990325 -37.4995294, \n"
          + "             122.0001268 -37.4993506,  \n"
          + "             122.0008564 -37.5004316))')) = 1";
      AggregationOperator aggregationOperator = getOperator(query);
      AggregationResultsBlock resultsBlock = aggregationOperator.nextBlock();
      // Expect 0 entries scanned in filter
      QueriesTestUtils
          .testInnerSegmentExecutionStatistics(aggregationOperator.getExecutionStatistics(), 0, 0, 0,
              NUM_RECORDS);
      List<Object> aggregationResult = resultsBlock.getResults();
      Assert.assertNotNull(aggregationResult);
      Assert.assertEquals((long) aggregationResult.get(0), 0);
    }
  }

  @Test
  public void stContainPointVeryCloseToBorderTest()
      throws Exception {
    List<GenericRow> records = new ArrayList<>(1);
    addRecord(records, -122.0008081, 37.5004231);
    setUp(records);
    // Test point is closed to border of a polygon but inside.
    String query = "SELECT COUNT(*) FROM testTable WHERE ST_Contains(ST_GeomFromText('POLYGON ((\n"
        + "             -122.0008564 37.5004316, \n"
        + "             -121.9991291 37.5005168, \n"
        + "             -121.9990325 37.4995294, \n"
        + "             -122.0001268 37.4993506,  \n"
        + "             -122.0008564 37.5004316))'), h3Column_geometry) = 1";
    AggregationOperator aggregationOperator = getOperator(query);
    AggregationResultsBlock resultsBlock = aggregationOperator.nextBlock();
    QueriesTestUtils.testInnerSegmentExecutionStatistics(aggregationOperator.getExecutionStatistics(), 1, 1, 0, 1);
    List<Object> aggregationResult = resultsBlock.getResults();
    Assert.assertNotNull(aggregationResult);
    Assert.assertEquals((long) aggregationResult.get(0), 1);
  }

  @Test
  public void stWithinPointVeryCloseToBorderTest()
      throws Exception {
    List<GenericRow> records = new ArrayList<>(1);
    addRecord(records, -122.0008081, 37.5004231);
    setUp(records);
    // Test point is closed to border of a polygon but inside.
    String query = "SELECT COUNT(*) FROM testTable WHERE ST_Within(h3Column_geometry, ST_GeomFromText('POLYGON ((\n"
        + "             -122.0008564 37.5004316, \n"
        + "             -121.9991291 37.5005168, \n"
        + "             -121.9990325 37.4995294, \n"
        + "             -122.0001268 37.4993506,  \n"
        + "             -122.0008564 37.5004316))')) = 1";
    AggregationOperator aggregationOperator = getOperator(query);
    AggregationResultsBlock resultsBlock = aggregationOperator.nextBlock();
    QueriesTestUtils.testInnerSegmentExecutionStatistics(aggregationOperator.getExecutionStatistics(), 1, 1, 0, 1);
    List<Object> aggregationResult = resultsBlock.getResults();
    Assert.assertNotNull(aggregationResult);
    Assert.assertEquals((long) aggregationResult.get(0), 1);
  }

  @Test
  public void stContainPointVeryCloseToBorderButOutsideTest()
      throws Exception {
    List<GenericRow> records = new ArrayList<>(1);
    addRecord(records, -122.0007277, 37.5005785);
    setUp(records);
    // Test point is closed to border of a polygon but outside.
    String query = "SELECT COUNT(*) FROM testTable WHERE ST_Contains(ST_GeomFromText('POLYGON ((\n"
        + "             -122.0008564 37.5004316, \n"
        + "             -121.9991291 37.5005168, \n"
        + "             -121.9990325 37.4995294, \n"
        + "             -122.0001268 37.4993506,  \n"
        + "             -122.0008564 37.5004316))'), h3Column_geometry) = 1";
    AggregationOperator aggregationOperator = getOperator(query);
    AggregationResultsBlock resultsBlock = aggregationOperator.nextBlock();
    QueriesTestUtils.testInnerSegmentExecutionStatistics(aggregationOperator.getExecutionStatistics(), 0, 1, 0, 1);
    List<Object> aggregationResult = resultsBlock.getResults();
    Assert.assertNotNull(aggregationResult);
    Assert.assertEquals((long) aggregationResult.get(0), 0);
  }

  @Test
  public void stWithinPointVeryCloseToBorderButOutsideTest()
      throws Exception {
    List<GenericRow> records = new ArrayList<>(1);
    addRecord(records, -122.0007277, 37.5005785);
    setUp(records);
    // Test point is closed to border of a polygon but outside.
    String query = "SELECT COUNT(*) FROM testTable WHERE ST_Within(h3Column_geometry, ST_GeomFromText('POLYGON ((\n"
        + "             -122.0008564 37.5004316, \n"
        + "             -121.9991291 37.5005168, \n"
        + "             -121.9990325 37.4995294, \n"
        + "             -122.0001268 37.4993506,  \n"
        + "             -122.0008564 37.5004316))')) = 1";
    AggregationOperator aggregationOperator = getOperator(query);
    AggregationResultsBlock resultsBlock = aggregationOperator.nextBlock();
    QueriesTestUtils.testInnerSegmentExecutionStatistics(aggregationOperator.getExecutionStatistics(), 0, 1, 0, 1);
    List<Object> aggregationResult = resultsBlock.getResults();
    Assert.assertNotNull(aggregationResult);
    Assert.assertEquals((long) aggregationResult.get(0), 0);
  }

  @Test
  public void queryStContainsWithMultipleFiltersFirstFilterEmpty()
      throws Exception {
    List<GenericRow> records = new ArrayList<>(1);
    addRecord(records, -122.0007277, 37.5005785);
    setUp(records);
    // Test point is close to border of a polygon but outside.
    String query = "SELECT COUNT(*) FROM testTable WHERE ST_Contains(ST_GeomFromText('POLYGON ((\n"
        + "             -122.0008564 37.5004316, \n"
        + "             -121.9991291 37.5005168, \n"
        + "             -121.9990325 37.4995294, \n"
        + "             -122.0001268 37.4993506,  \n"
        + "             -122.0008564 37.5004316))'), h3Column_geometry) = 1 AND "
        + "             ST_Contains(ST_GeomFromText('POLYGON (( \n"
        + "             -122.0008564 37.5004316, \n"
        + "             -121.9991291 37.5005168, \n"
        + "             -121.9990325 37.4995294, \n"
        + "             -122.0001268 37.4993506, \n"
        + "             -122.0008564 37.5004316))'), h3Column_geometry) = 0";

    AggregationOperator aggregationOperator = getOperator(query);
    AggregationResultsBlock resultsBlock = aggregationOperator.nextBlock();
    QueriesTestUtils.testInnerSegmentExecutionStatistics(aggregationOperator.getExecutionStatistics(), 0, 0, 0, 1);
    List<Object> aggregationResult = resultsBlock.getResults();
    Assert.assertNotNull(aggregationResult);
    Assert.assertEquals((long) aggregationResult.get(0), 0);
  }

  @Test
  public void queryStContainsWithMultipleFiltersSecondFilterEmpty()
      throws Exception {
    List<GenericRow> records = new ArrayList<>(1);
    addRecord(records, -122.0007277, 37.5005785);
    setUp(records);
    // Test point is close to border of a polygon but outside.
    String query = "SELECT COUNT(*) FROM testTable WHERE ST_Contains(ST_GeomFromText('POLYGON ((\n"
        + "             -122.0008564 37.5004316, \n"
        + "             -121.9991291 37.5005168, \n"
        + "             -121.9990325 37.4995294, \n"
        + "             -122.0001268 37.4993506,  \n"
        + "             -122.0008564 37.5004316))'), h3Column_geometry) = 0 AND "
        + "             ST_Contains(ST_GeomFromText('POLYGON (( \n"
        + "             -122.0008564 37.5004316, \n"
        + "             -121.9991291 37.5005168, \n"
        + "             -121.9990325 37.4995294, \n"
        + "             -122.0001268 37.4993506, \n"
        + "             -122.0008564 37.5004316))'), h3Column_geometry) = 1";

    AggregationOperator aggregationOperator = getOperator(query);
    AggregationResultsBlock resultsBlock = aggregationOperator.nextBlock();
    QueriesTestUtils.testInnerSegmentExecutionStatistics(aggregationOperator.getExecutionStatistics(), 0, 1, 0, 1);
    List<Object> aggregationResult = resultsBlock.getResults();
    Assert.assertNotNull(aggregationResult);
    Assert.assertEquals((long) aggregationResult.get(0), 0);
  }

  private void testQuery(String queryTemplate) {
    String h3IndexQuery = String.format(queryTemplate, H3_INDEX_COLUMN);
    String nonH3IndexQuery = String.format(queryTemplate, NON_H3_INDEX_COLUMN);
    validateQueryResult(h3IndexQuery, nonH3IndexQuery);
  }

  private void testQueryStContain(String queryTemplate) {
    String h3IndexQuery = String.format(queryTemplate, H3_INDEX_GEOMETRY_COLUMN);
    String nonH3IndexQuery = String.format(queryTemplate, NON_H3_INDEX_GEOMETRY_COLUMN);
    validateQueryResult(h3IndexQuery, nonH3IndexQuery);
  }

  private void validateQueryResult(String h3IndexQuery, String nonH3IndexQuery) {
    AggregationOperator h3IndexOperator = getOperator(h3IndexQuery);
    AggregationOperator nonH3IndexOperator = getOperator(nonH3IndexQuery);
    AggregationResultsBlock h3IndexResultsBlock = h3IndexOperator.nextBlock();
    AggregationResultsBlock nonH3IndexResultsBlock = nonH3IndexOperator.nextBlock();
    // Expect less than 10000 entries scanned in filter
    Assert.assertTrue(h3IndexOperator.getExecutionStatistics().getNumEntriesScannedInFilter() < NUM_RECORDS);
    Assert.assertEquals(nonH3IndexOperator.getExecutionStatistics().getNumEntriesScannedInFilter(), NUM_RECORDS);
    Assert.assertEquals(h3IndexResultsBlock.getResults(), nonH3IndexResultsBlock.getResults());
  }

  @AfterClass
  public void tearDown()
      throws IOException {
    _indexSegment.destroy();
    FileUtils.deleteDirectory(INDEX_DIR);
  }
}
