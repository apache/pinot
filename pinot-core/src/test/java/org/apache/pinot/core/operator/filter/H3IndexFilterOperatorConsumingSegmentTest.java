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
package org.apache.pinot.core.operator.filter;

import java.io.File;
import java.util.List;
import java.util.Set;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.core.plan.FilterPlanNode;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.core.query.request.context.utils.QueryContextConverterUtils;
import org.apache.pinot.segment.local.indexsegment.mutable.MutableSegmentImpl;
import org.apache.pinot.segment.local.io.writer.impl.DirectMemoryManager;
import org.apache.pinot.segment.local.realtime.impl.RealtimeSegmentConfig;
import org.apache.pinot.segment.local.realtime.impl.RealtimeSegmentStatsHistory;
import org.apache.pinot.segment.local.utils.GeometrySerializer;
import org.apache.pinot.segment.local.utils.GeometryUtils;
import org.apache.pinot.segment.spi.MutableSegment;
import org.apache.pinot.segment.spi.SegmentContext;
import org.apache.pinot.segment.spi.index.DictionaryIndexConfig;
import org.apache.pinot.segment.spi.index.StandardIndexes;
import org.apache.pinot.segment.spi.index.creator.H3IndexConfig;
import org.apache.pinot.segment.spi.index.reader.H3IndexResolution;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.locationtech.jts.geom.Coordinate;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;


/// Tests [H3IndexFilterOperator] and [H3InclusionIndexFilterOperator] on a consuming segment that keeps indexing
/// between query planning and execution. numDocs is read at planning time, but the mutable H3 index returns its live
/// bitmaps at execution time, so the documents ingested in between must be ignored. Otherwise the filters that flip
/// the H3 index result (distance lower bound, negative inclusion check) return the new documents near the point as
/// matches.
public class H3IndexFilterOperatorConsumingSegmentTest {
  private static final File TEMP_DIR =
      new File(FileUtils.getTempDirectory(), "H3IndexFilterOperatorConsumingSegmentTest");
  private static final String SEGMENT_NAME = "testSegment";
  private static final String LOCATION_COLUMN = "location";
  private static final String LOCATION_GEOMETRY_COLUMN = "locationGeometry";
  private static final String TIME_COLUMN = "ts";
  private static final long TIME_VALUE = 1000L;
  private static final Schema SCHEMA = new Schema.SchemaBuilder().setSchemaName("testTable")
      .addSingleValueDimension(LOCATION_COLUMN, DataType.BYTES)
      .addSingleValueDimension(LOCATION_GEOMETRY_COLUMN, DataType.BYTES)
      .addSingleValueDimension(TIME_COLUMN, DataType.LONG)
      .build();
  // Scan-based filter on a raw column: the AND then applies the H3 filter bitmap as is, without iterating it within
  // [0, numDocs)
  private static final String SCAN_FILTER = "ts >= 0 AND ts <= 2000";
  // Point at the center of the queries, and one about 100 km away
  private static final Coordinate NEAR = new Coordinate(-122, 37.5);
  private static final Coordinate FAR = new Coordinate(-121, 38);
  private static final String DISTANCE = "ST_Distance(location, ST_Point(-122, 37.5, 1))";
  private static final String WITHIN = "ST_Within(locationGeometry, ST_GeomFromText('POLYGON ((-122.01 37.49, "
      + "-121.99 37.49, -121.99 37.51, -122.01 37.51, -122.01 37.49))'))";

  private MutableSegment _segment;

  @BeforeMethod
  public void setUp() {
    RealtimeSegmentStatsHistory statsHistory = mock(RealtimeSegmentStatsHistory.class);
    when(statsHistory.getEstimatedCardinality(anyString())).thenReturn(200);
    when(statsHistory.getEstimatedAvgColSize(anyString())).thenReturn(32);
    H3IndexConfig h3IndexConfig = new H3IndexConfig(new H3IndexResolution(List.of(5)));
    RealtimeSegmentConfig segmentConfig = new RealtimeSegmentConfig.Builder()
        .setTableNameWithType("testTable_REALTIME")
        .setSegmentName(SEGMENT_NAME)
        .setStreamName("testStream")
        .setSchema(SCHEMA)
        .setCapacity(1000)
        .setAvgNumMultiValues(2)
        .setIndex(Set.of(LOCATION_COLUMN, LOCATION_GEOMETRY_COLUMN, TIME_COLUMN), StandardIndexes.dictionary(),
            DictionaryIndexConfig.DISABLED)
        .setIndex(Set.of(LOCATION_COLUMN, LOCATION_GEOMETRY_COLUMN), StandardIndexes.h3(), h3IndexConfig)
        .setSegmentZKMetadata(new SegmentZKMetadata(SEGMENT_NAME))
        .setMemoryManager(new DirectMemoryManager(SEGMENT_NAME))
        .setStatsHistory(statsHistory)
        .setConsumerDir(TEMP_DIR.getAbsolutePath())
        .build();
    _segment = new MutableSegmentImpl(segmentConfig, null);
    // Documents visible at planning time: 0 far, 1 near, 2 far, 3 near
    index(FAR, NEAR, FAR, NEAR);
  }

  @AfterMethod
  public void tearDown() {
    _segment.destroy();
    FileUtils.deleteQuietly(TEMP_DIR);
  }

  @DataProvider
  public Object[][] filters() {
    return new Object[][]{
        // Lower bound only: the documents in the cells that might be within the distance are flipped
        {DISTANCE + " > 1000", List.of(0, 2)},
        {DISTANCE + " < 1000", List.of(1, 3)},
        // Negative check: the documents in the cells that might be covered by the polygon are flipped
        {WITHIN + " = 0", List.of(0, 2)},
        {WITHIN + " = 1", List.of(1, 3)}
    };
  }

  @Test(dataProvider = "filters")
  public void testFilterOnlyReturnsDocsVisibleAtPlanning(String predicate, List<Integer> expectedDocIds) {
    QueryContext queryContext = QueryContextConverterUtils.getQueryContext(
        "SELECT * FROM testTable WHERE " + SCAN_FILTER + " AND " + predicate);
    BaseFilterOperator filterOperator = new FilterPlanNode(new SegmentContext(_segment), queryContext).run();
    ingestAfterPlanning();
    assertEquals(TestUtils.getDocIds(filterOperator.nextBlock().getBlockDocIdSet()), expectedDocIds);
  }

  /// The consuming segment keeps indexing between query planning and execution: documents 4 near, 5 far
  private void ingestAfterPlanning() {
    index(NEAR, FAR);
  }

  private void index(Coordinate... coordinates) {
    for (Coordinate coordinate : coordinates) {
      GenericRow row = new GenericRow();
      row.putValue(LOCATION_COLUMN,
          GeometrySerializer.serialize(GeometryUtils.GEOGRAPHY_FACTORY.createPoint(coordinate)));
      row.putValue(LOCATION_GEOMETRY_COLUMN,
          GeometrySerializer.serialize(GeometryUtils.GEOMETRY_FACTORY.createPoint(coordinate)));
      row.putValue(TIME_COLUMN, TIME_VALUE);
      try {
        _segment.index(row, null);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
  }
}
