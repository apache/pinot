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
import com.uber.h3core.LengthUnit;
import com.uber.h3core.util.LatLng;
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
import org.apache.pinot.segment.local.utils.H3Utils;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
import org.apache.pinot.spi.config.table.FieldConfig;
import org.apache.pinot.spi.config.table.FieldConfig.EncodingType;
import org.apache.pinot.spi.config.table.FieldConfig.IndexType;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.locationtech.jts.geom.Coordinate;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.apache.pinot.segment.local.utils.H3Utils.H3_CORE;

/**
 * Queries test for H3 index.
 */
public class H3IndexQueriesTest extends BaseQueriesTest {
  private static final File INDEX_DIR = new File(FileUtils.getTempDirectory(), "H3IndexQueriesTest");
  private static final String RAW_TABLE_NAME = "testTable";
  private static final String SEGMENT_NAME = "testSegment";

  // this test is only useful when number of records is high, e.g. 1 mil or higher ( but it's much too slow for CI )
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
    setUp(records, 5);
  }

  public void setUp(List<GenericRow> records, int resolution)
      throws Exception {
    FileUtils.deleteDirectory(INDEX_DIR);

    Map<String, String> h3IndexProps = Collections.singletonMap("resolutions", String.valueOf(resolution));

    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(RAW_TABLE_NAME)
        .setFieldConfigList(ImmutableList
            .of(new FieldConfig(H3_INDEX_COLUMN, EncodingType.DICTIONARY, IndexType.H3, null,
                    h3IndexProps),
                new FieldConfig(H3_INDEX_GEOMETRY_COLUMN, EncodingType.DICTIONARY, IndexType.H3,
                    null, h3IndexProps))).build();

    SegmentGeneratorConfig segmentGeneratorConfig = new SegmentGeneratorConfig(tableConfig, SCHEMA);
    segmentGeneratorConfig.setTableName(RAW_TABLE_NAME);
    segmentGeneratorConfig.setSegmentName(SEGMENT_NAME);
    segmentGeneratorConfig.setOutDir(INDEX_DIR.getPath());

    SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
    driver.init(segmentGeneratorConfig, new GenericRowRecordReader(records));
    driver.build();

    IndexLoadingConfig indexLoadingConfig = new IndexLoadingConfig(tableConfig, SCHEMA);
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

  @DataProvider
  public Integer[] getResolutions() {
    Integer[] result = new Integer[16];
    for (int i = 0; i < result.length; i++) {
      result[i] = i;
    }
    return result;
  }

  // no really a test but a calculation of distance stats per level
  @Test(dataProvider = "getResolutions", enabled = false)
  public void testH3IndexDistances(int resolution) {
    double latitude = 37;
    double longitude = -122.5;
    double maxDistance = 1000;

    double edgeLength;
    long h3Id = H3Utils.H3_CORE.geoToH3(latitude, longitude, resolution);
    LatLng h3Coords = H3Utils.H3_CORE.h3ToGeo(h3Id);

    double coreDist = H3_CORE.pointDist(h3Coords, new LatLng(latitude, longitude), LengthUnit.m);

    double maxLength = 0;
    List<Long> edges = H3_CORE.getH3UnidirectionalEdgesFromHexagon(h3Id);
    for (int i = 0, len = edges.size(); i < len; i++) {
      maxLength = Math.max(maxLength, H3Utils.H3_CORE.exactEdgeLength(edges.get(i), LengthUnit.m));
    }
    edgeLength = maxLength;

    System.out.println(" core dist is " + coreDist + ", edge length " + edgeLength);

    int fullN = (int) Math.floor((maxDistance / edgeLength - 2) / 1.7321);
    int partialN = (int) Math.floor((maxDistance / edgeLength + 2) / 1.44 + 0.001);

    List<List<Long>> rings = H3_CORE.kRingDistances(h3Id, 100);
    List<double[]> stats = new ArrayList<>();

    for (int i = 0, len = rings.size(); i < len; i++) {

      List<Long> ring = rings.get(i);
      int jlen = ring.size();
      int full = 0;
      int partial = 0;
      double minDist = Double.MAX_VALUE;
      double maxDist = 0;

      for (int j = 0; j < jlen; j++) {
        double dist = H3_CORE.pointDist(h3Coords, H3_CORE.h3ToGeo(ring.get(j)), LengthUnit.m);

        if (dist > maxDistance + edgeLength) {
          // beyond maxDistance
        } else if (dist <= maxDistance - edgeLength) {
          // fully contained
          full++;
        } else if (dist < maxDistance) {
          // partially contained, need additional check
          partial++;
        }

        minDist = Math.min(minDist, dist);
        maxDist = Math.max(maxDist, dist);
      }
      if (minDist == Double.MAX_VALUE) {
        minDist = 0;
      }

      stats.add(new double[]{
          i, jlen, full, partial, minDist,
          minDist / edgeLength, minDist / (edgeLength * i),
          maxDist, maxDist / edgeLength, maxDist / (edgeLength * i),
          i <= fullN ? 1.0 : 0.0, i <= partialN ? 1.0 : 0.0
      });
    }

    for (int i = 0; i < stats.size(); i++) {
      System.out.println(Arrays.toString(stats.get(i)));
    }
  }

  @Test(dataProvider = "getResolutions")
  public void testH3Index(int resolution)
      throws Exception {
    final Random random = new Random(0L);

    List<GenericRow> records = new ArrayList<>(NUM_RECORDS);
    for (int i = 0; i < NUM_RECORDS; i++) {
      // one degree amounts to about 100km
      double longitude = -122.5 + random.nextDouble();
      double latitude = 37 + random.nextDouble();
      addRecord(records, longitude, latitude);
    }
    setUp(records, resolution);

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
    for (int i = 1; i <= 100; i += 2) {
      testQuery("SELECT COUNT(*) FROM testTable WHERE ST_Distance(%s, ST_Point(-122, 37.5, 1)) > " + i * 1000);
    }

    // Upper bound only
    for (int i = 1; i <= 100; i += 2) {
      testQuery("SELECT COUNT(*) FROM testTable WHERE ST_Distance(%s, ST_Point(-122, 37.5, 1)) < " + (i * 1000));
    }

    // Both lower and upper bound
    testQuery("SELECT COUNT(*) FROM testTable WHERE ST_Distance(%s, ST_Point(-122, 37.5, 1)) BETWEEN 1000 AND 5000");

    for (int i = 1; i < 20; i += 2) {
      for (int j = i + 1; j <= 20; j += 2) {
        testQuery("SELECT COUNT(*) FROM testTable WHERE ST_Distance(%s, ST_Point(-122, 37.5, 1)) BETWEEN " + (i * 5000)
            + " AND " + (j * 5000));
      }
    }

    // Distance is too large, should fall back to scan-based ExpressionFilterOperator
    {
      String query = "SELECT COUNT(*) FROM testTable WHERE ST_Distance(h3Column, ST_Point(-122, 37.5, 1)) < 10000000";
      AggregationOperator aggregationOperator = getOperator(query);
      AggregationResultsBlock resultsBlock = aggregationOperator.nextBlock();
      QueriesTestUtils
          .testInnerSegmentExecutionStatistics(aggregationOperator.getExecutionStatistics(), NUM_RECORDS,
              //at coarse-grained resolutions 100 hexagon rings can easily cover 10k km or even whole of earth
              resolution < 4 ? 0 : NUM_RECORDS,
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
  public void queryStContainsWithMultipleFilters()
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
        + "             -122.0008564 37.5004316))'), h3Column_geometry) = 1 AND "
        + "             ST_Contains(ST_GeomFromText('POLYGON (( \n"
        + "             -122.0008564 37.5004316, \n"
        + "             -121.9991291 37.5005168, \n"
        + "             -121.9990325 37.4995294, \n"
        + "             -122.0001268 37.4993506, \n"
        + "             -122.0008564 37.5004316))'), h3Column_geometry) = 0";

    AggregationOperator aggregationOperator = getOperator(query);
    AggregationResultsBlock resultsBlock = aggregationOperator.nextBlock();
    QueriesTestUtils.testInnerSegmentExecutionStatistics(aggregationOperator.getExecutionStatistics(), 0, 2, 0, 1);
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
    try {
      Assert.assertTrue(h3IndexOperator.getExecutionStatistics().getNumEntriesScannedInFilter() <= NUM_RECORDS);
      Assert.assertEquals(nonH3IndexOperator.getExecutionStatistics().getNumEntriesScannedInFilter(), NUM_RECORDS);
      Assert.assertEquals(h3IndexResultsBlock.getResults(), nonH3IndexResultsBlock.getResults());
    } catch (AssertionError ae) {
      throw new AssertionError(ae.getMessage() + "\nQuery: " + h3IndexQuery, ae);
    }
  }

  @AfterMethod
  public void tearDown()
      throws IOException {
    if (_indexSegment != null) {
      _indexSegment.destroy();
      _indexSegment = null;
    }
    FileUtils.deleteDirectory(INDEX_DIR);
  }
}
