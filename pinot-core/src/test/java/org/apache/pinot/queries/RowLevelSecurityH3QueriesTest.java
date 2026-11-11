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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.function.IntPredicate;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.common.response.broker.BrokerResponseNative;
import org.apache.pinot.segment.local.indexsegment.immutable.ImmutableSegmentLoader;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.segment.local.segment.index.loader.IndexLoadingConfig;
import org.apache.pinot.segment.local.segment.readers.GenericRowRecordReader;
import org.apache.pinot.segment.local.utils.GeometrySerializer;
import org.apache.pinot.segment.local.utils.GeometryUtils;
import org.apache.pinot.segment.spi.ImmutableSegment;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
import org.apache.pinot.spi.config.table.FieldConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.apache.pinot.sql.parsers.rewriter.RlsUtils;
import org.locationtech.jts.geom.Coordinate;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;


/// Row-level security (RLS) on H3-indexed filters.
///
/// An H3 filter matches the documents of the cells that the shape fully covers through the index, and evaluates the
/// predicate expression on the documents of the cells that the shape only partly covers. Under RLS, the expression
/// must only run on the partial-cell documents that the RLS filter allows. Otherwise `numEntriesScannedInFilter`
/// counts the hidden rows near any shape that the user picks.
///
/// The test compares two segments with the same tenant rows at the same locations: `mixed` also holds the rows of a
/// hidden tenant, and `tenantOnly` holds only the tenant rows. The query on `mixed` with the RLS filter must return
/// the same count and scan the same number of entries as the query on `tenantOnly` without it.
public class RowLevelSecurityH3QueriesTest extends BaseQueriesTest {
  private static final File INDEX_DIR = new File(FileUtils.getTempDirectory(), "RowLevelSecurityH3QueriesTest");
  private static final String RAW_TABLE_NAME = "testTable";
  private static final int NUM_ROWS = 10000;
  private static final String RLS_FILTER = "tenant = 'A'";
  private static final String GEOGRAPHY_COLUMN = "location";
  private static final String GEOMETRY_COLUMN = "location_geometry";

  private ImmutableSegment _mixedSegment;
  private ImmutableSegment _tenantOnlySegment;
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
    FileUtils.deleteDirectory(INDEX_DIR);
    Schema schema = new Schema.SchemaBuilder().setSchemaName(RAW_TABLE_NAME)
        .addSingleValueDimension("tenant", DataType.STRING)
        .addSingleValueDimension(GEOGRAPHY_COLUMN, DataType.BYTES)
        .addSingleValueDimension(GEOMETRY_COLUMN, DataType.BYTES)
        .build();
    Map<String, String> h3Properties = Map.of("resolutions", "5");
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(RAW_TABLE_NAME)
        .setFieldConfigList(List.of(
            new FieldConfig(GEOGRAPHY_COLUMN, FieldConfig.EncodingType.DICTIONARY, List.of(FieldConfig.IndexType.H3),
                null, h3Properties),
            new FieldConfig(GEOMETRY_COLUMN, FieldConfig.EncodingType.DICTIONARY, List.of(FieldConfig.IndexType.H3),
                null, h3Properties)))
        .build();

    Random random = new Random(42);
    double[] longitudes = new double[NUM_ROWS];
    double[] latitudes = new double[NUM_ROWS];
    for (int i = 0; i < NUM_ROWS; i++) {
      longitudes[i] = -122.5 + random.nextDouble();
      latitudes[i] = 37 + random.nextDouble();
    }
    _mixedSegment = buildSegment("mixed", tableConfig, schema, longitudes, latitudes, i -> true);
    _tenantOnlySegment = buildSegment("tenantOnly", tableConfig, schema, longitudes, latitudes,
        RowLevelSecurityH3QueriesTest::isTenantA);
  }

  private static ImmutableSegment buildSegment(String segmentName, TableConfig tableConfig, Schema schema,
      double[] longitudes, double[] latitudes, IntPredicate rowFilter)
      throws Exception {
    List<GenericRow> rows = new ArrayList<>(NUM_ROWS);
    for (int i = 0; i < NUM_ROWS; i++) {
      if (!rowFilter.test(i)) {
        continue;
      }
      Coordinate coordinate = new Coordinate(longitudes[i], latitudes[i]);
      GenericRow row = new GenericRow();
      row.putValue("tenant", isTenantA(i) ? "A" : "B");
      row.putValue(GEOGRAPHY_COLUMN,
          GeometrySerializer.serialize(GeometryUtils.GEOGRAPHY_FACTORY.createPoint(coordinate)));
      row.putValue(GEOMETRY_COLUMN,
          GeometrySerializer.serialize(GeometryUtils.GEOMETRY_FACTORY.createPoint(coordinate)));
      rows.add(row);
    }
    SegmentGeneratorConfig config = new SegmentGeneratorConfig(tableConfig, schema);
    config.setTableName(RAW_TABLE_NAME);
    config.setSegmentName(segmentName);
    config.setOutDir(INDEX_DIR.getPath());
    SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
    driver.init(config, new GenericRowRecordReader(rows));
    driver.build();
    return ImmutableSegmentLoader.load(new File(INDEX_DIR, segmentName), new IndexLoadingConfig(tableConfig, schema));
  }

  private static boolean isTenantA(int i) {
    return i % 2 == 0;
  }

  @AfterClass
  public void tearDown() {
    _mixedSegment.destroy();
    _tenantOnlySegment.destroy();
    FileUtils.deleteQuietly(INDEX_DIR);
  }

  /// Each filter goes through an H3 operator: `ST_Distance` through `H3IndexFilterOperator` (upper bound, lower
  /// bound, both), `ST_Contains` through `H3InclusionIndexFilterOperator` (positive and negative check).
  @DataProvider
  public static Object[][] h3Filters() {
    String polygon = "ST_GeomFromText('POLYGON ((-122.2 37.3, -121.8 37.3, -121.8 37.7, -122.2 37.7, -122.2 37.3))')";
    return new Object[][]{
        {"ST_Distance(" + GEOGRAPHY_COLUMN + ", ST_Point(-122, 37.5, 1)) < 20000"},
        {"ST_Distance(" + GEOGRAPHY_COLUMN + ", ST_Point(-122, 37.5, 1)) > 20000"},
        {"ST_Distance(" + GEOGRAPHY_COLUMN + ", ST_Point(-122, 37.5, 1)) BETWEEN 10000 AND 20000"},
        {"ST_Contains(" + polygon + ", " + GEOMETRY_COLUMN + ") = 1"},
        {"ST_Contains(" + polygon + ", " + GEOMETRY_COLUMN + ") = 0"}
    };
  }

  @Test(dataProvider = "h3Filters")
  public void testExpressionDoesNotRunOnHiddenRows(String h3Filter) {
    String query = "SELECT COUNT(*) FROM testTable WHERE " + h3Filter;

    useSegment(_tenantOnlySegment);
    BrokerResponseNative expected = getBrokerResponse(query);
    useSegment(_mixedSegment);
    BrokerResponseNative admin = getBrokerResponse(query);
    BrokerResponseNative user =
        getBrokerResponse(query, Map.of(RlsUtils.buildRlsFilterKey(RAW_TABLE_NAME), RLS_FILTER));

    assertTrue(user.getExceptions().isEmpty(), user.getExceptions().toString());
    long expectedCount = ((Number) expected.getResultTable().getRows().get(0)[0]).longValue();
    assertTrue(expectedCount > 0, "The filter must match some rows to be meaningful");
    assertEquals(((Number) user.getResultTable().getRows().get(0)[0]).longValue(), expectedCount);
    // The hidden rows add to the partial-cell scan of the admin query, so the test can tell the two apart
    assertTrue(admin.getNumEntriesScannedInFilter() > expected.getNumEntriesScannedInFilter(),
        admin.getNumEntriesScannedInFilter() + " vs " + expected.getNumEntriesScannedInFilter());
    assertEquals(user.getNumEntriesScannedInFilter(), expected.getNumEntriesScannedInFilter());
  }

  private void useSegment(IndexSegment segment) {
    _indexSegment = segment;
    _indexSegments = List.of(segment, segment);
  }
}
