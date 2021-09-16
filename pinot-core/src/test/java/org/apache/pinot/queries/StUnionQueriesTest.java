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
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.common.response.broker.AggregationResult;
import org.apache.pinot.common.response.broker.BrokerResponseNative;
import org.apache.pinot.common.response.broker.GroupByResult;
import org.apache.pinot.common.response.broker.ResultTable;
import org.apache.pinot.common.utils.HashUtil;
import org.apache.pinot.core.common.Operator;
import org.apache.pinot.core.geospatial.transform.function.ScalarFunctions;
import org.apache.pinot.core.operator.blocks.IntermediateResultsBlock;
import org.apache.pinot.core.operator.query.AggregationGroupByOperator;
import org.apache.pinot.core.operator.query.AggregationOperator;
import org.apache.pinot.core.query.aggregation.groupby.AggregationGroupByResult;
import org.apache.pinot.core.query.aggregation.groupby.GroupKeyGenerator;
import org.apache.pinot.segment.local.indexsegment.immutable.ImmutableSegmentLoader;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.segment.local.segment.readers.GenericRowRecordReader;
import org.apache.pinot.segment.local.utils.GeometrySerializer;
import org.apache.pinot.segment.local.utils.GeometryUtils;
import org.apache.pinot.segment.spi.ImmutableSegment;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.utils.BytesUtils;
import org.apache.pinot.spi.utils.DataSchema;
import org.apache.pinot.spi.utils.DataSchema.ColumnDataType;
import org.apache.pinot.spi.utils.ReadMode;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.Point;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.AssertJUnit.assertNotNull;


/**
 * Queries test for ST_UNION queries.
 */
@SuppressWarnings("rawtypes")
public class StUnionQueriesTest extends BaseQueriesTest {
  private static final File INDEX_DIR = new File(FileUtils.getTempDirectory(), "StUnionQueriesTest");
  private static final String RAW_TABLE_NAME = "testTable";
  private static final String SEGMENT_NAME = "testSegment";
  private static final Random RANDOM = new Random();

  private static final int NUM_RECORDS = 200;
  private static final int MAX_VALUE = 100000;

  private static final String POINT_COLUMN = "pointColumn";
  private static final String INT_COLUMN = "intColumn";
  private static final Schema SCHEMA =
      new Schema.SchemaBuilder().addSingleValueDimension(POINT_COLUMN, FieldSpec.DataType.BYTES)
          .addSingleValueDimension(INT_COLUMN, FieldSpec.DataType.INT).build();
  private static final TableConfig TABLE_CONFIG =
      new TableConfigBuilder(TableType.OFFLINE).setTableName(RAW_TABLE_NAME).build();

  private Map<Integer, Geometry> _values;
  private Geometry _intermediateResult;
  private byte[] _expectedResults;
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
    int hashMapCapacity = HashUtil.getHashMapCapacity(MAX_VALUE);
    _values = new HashMap<>(hashMapCapacity);
    List<GenericRow> records = new ArrayList<>(NUM_RECORDS);
    for (int i = 0; i < NUM_RECORDS; i++) {
      GenericRow record = new GenericRow();

      int x = RANDOM.nextInt(MAX_VALUE);
      int y = RANDOM.nextInt(MAX_VALUE);
      Point point = GeometryUtils.GEOMETRY_FACTORY.createPoint(new Coordinate(x, y));
      byte[] pointBytes = GeometrySerializer.serialize(point);
      _intermediateResult = _intermediateResult == null ? point : point.union(_intermediateResult);
      record.putValue(POINT_COLUMN, pointBytes);

      int value = RANDOM.nextInt(MAX_VALUE);
      record.putValue(INT_COLUMN, value);
      int key = Integer.hashCode(value);
      _values.put(key, _values.containsKey(key) ? _values.get(key).union(point) : point);
      records.add(record);
    }
    _expectedResults = GeometrySerializer.serialize(_intermediateResult);

    SegmentGeneratorConfig segmentGeneratorConfig = new SegmentGeneratorConfig(TABLE_CONFIG, SCHEMA);
    segmentGeneratorConfig.setTableName(RAW_TABLE_NAME);
    segmentGeneratorConfig.setSegmentName(SEGMENT_NAME);
    segmentGeneratorConfig.setOutDir(INDEX_DIR.getPath());

    SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
    driver.init(segmentGeneratorConfig, new GenericRowRecordReader(records));
    driver.build();

    ImmutableSegment immutableSegment = ImmutableSegmentLoader.load(new File(INDEX_DIR, SEGMENT_NAME), ReadMode.mmap);
    _indexSegment = immutableSegment;
    _indexSegments = Arrays.asList(immutableSegment, immutableSegment);
  }

  @Test
  public void testAggregationOnly() {
    String query = "SELECT ST_UNION(pointColumn) FROM testTable";

    // Inner segment
    Operator operator = getOperatorForPqlQuery(query);
    assertTrue(operator instanceof AggregationOperator);
    IntermediateResultsBlock resultsBlock = ((AggregationOperator) operator).nextBlock();
    QueriesTestUtils.testInnerSegmentExecutionStatistics(operator.getExecutionStatistics(), NUM_RECORDS, 0, NUM_RECORDS,
        NUM_RECORDS);
    List<Object> aggregationResult = resultsBlock.getAggregationResult();
    assertNotNull(aggregationResult);
    assertEquals(aggregationResult.size(), 1);
    assertEquals(aggregationResult.get(0), _intermediateResult);

    // Inter segments
    String[] expectedResults = new String[1];
    expectedResults[0] = BytesUtils.toHexString(_expectedResults);
    BrokerResponseNative brokerResponse = getBrokerResponseForPqlQuery(query);
    QueriesTestUtils
        .testInterSegmentAggregationResult(brokerResponse, 4 * NUM_RECORDS, 0, 4 * NUM_RECORDS, 4 * NUM_RECORDS,
            expectedResults);
  }

  @Test
  public void testPostAggregation() {
    String query =
        "SELECT ST_AS_TEXT(ST_UNION(pointColumn)), TO_GEOMETRY(ST_UNION(pointColumn)), TO_SPHERICAL_GEOGRAPHY"
            + "(ST_UNION(pointColumn)), ST_AS_TEXT(TO_SPHERICAL_GEOGRAPHY(ST_UNION(pointColumn))) FROM testTable";

    // Inner segment
    Operator operator = getOperatorForPqlQuery(query);
    assertTrue(operator instanceof AggregationOperator);
    IntermediateResultsBlock resultsBlock = ((AggregationOperator) operator).nextBlock();
    QueriesTestUtils.testInnerSegmentExecutionStatistics(operator.getExecutionStatistics(), NUM_RECORDS, 0, NUM_RECORDS,
        NUM_RECORDS);
    List<Object> aggregationResult = resultsBlock.getAggregationResult();
    assertNotNull(aggregationResult);
    assertEquals(aggregationResult.size(), 4);
    for (Object value : aggregationResult) {
      assertEquals(value, _intermediateResult);
    }

    // Inter segment
    BrokerResponseNative brokerResponse = getBrokerResponseForSqlQuery(query);
    ResultTable resultTable = brokerResponse.getResultTable();
    DataSchema expectedDataSchema = new DataSchema(new String[]{
        "st_as_text(st_union(pointColumn))", "to_geometry(st_union(pointColumn))",
        "to_spherical_geography(st_union(pointColumn))", "st_as_text(to_spherical_geography(st_union(pointColumn)))"
    }, new ColumnDataType[]{ColumnDataType.STRING, ColumnDataType.BYTES, ColumnDataType.BYTES, ColumnDataType.STRING});
    assertEquals(resultTable.getDataSchema(), expectedDataSchema);
    List<Object[]> rows = resultTable.getRows();
    assertEquals(rows.size(), 1);
    assertEquals(rows.get(0), new Object[]{
        ScalarFunctions.stAsText(_expectedResults),
        BytesUtils.toHexString(ScalarFunctions.toGeometry(_expectedResults)),
        BytesUtils.toHexString(ScalarFunctions.toSphericalGeography(_expectedResults)),
        ScalarFunctions.stAsText(ScalarFunctions.toSphericalGeography(_expectedResults))
    });
  }

  @Test
  public void testAggregationOnlyOnEmptyResultSet() {
    String query = "SELECT ST_UNION(pointColumn) FROM testTable where intColumn=-1";

    // Inner segment
    Operator operator = getOperatorForPqlQuery(query);
    assertTrue(operator instanceof AggregationOperator);
    IntermediateResultsBlock resultsBlock = ((AggregationOperator) operator).nextBlock();
    QueriesTestUtils.testInnerSegmentExecutionStatistics(operator.getExecutionStatistics(), 0, 0, 0, NUM_RECORDS);
    List<Object> aggregationResult = resultsBlock.getAggregationResult();
    assertNotNull(aggregationResult);
    assertEquals(aggregationResult.size(), 1);
    assertEquals(aggregationResult.get(0), GeometryUtils.EMPTY_POINT);

    // Inter segments
    String[] expectedResults = new String[1];
    expectedResults[0] = BytesUtils.toHexString(GeometrySerializer.serialize(GeometryUtils.EMPTY_POINT));
    BrokerResponseNative brokerResponse = getBrokerResponseForPqlQuery(query);
    QueriesTestUtils.testInterSegmentAggregationResult(brokerResponse, 0, 0, 0, 4 * NUM_RECORDS, expectedResults);
  }

  @Test
  public void testAggregationGroupBy() {
    String query = "SELECT ST_UNION(pointColumn) FROM testTable GROUP BY intColumn";

    // Inner segment
    Operator operator = getOperatorForPqlQuery(query);
    assertTrue(operator instanceof AggregationGroupByOperator);
    IntermediateResultsBlock resultsBlock = ((AggregationGroupByOperator) operator).nextBlock();
    QueriesTestUtils
        .testInnerSegmentExecutionStatistics(operator.getExecutionStatistics(), NUM_RECORDS, 0, 2 * NUM_RECORDS,
            NUM_RECORDS);
    AggregationGroupByResult aggregationGroupByResult = resultsBlock.getAggregationGroupByResult();
    assertNotNull(aggregationGroupByResult);
    int numGroups = 0;
    Iterator<GroupKeyGenerator.GroupKey> groupKeyIterator = aggregationGroupByResult.getGroupKeyIterator();
    while (groupKeyIterator.hasNext()) {
      numGroups++;
      GroupKeyGenerator.GroupKey groupKey = groupKeyIterator.next();
      Integer key = (Integer) groupKey._keys[0];
      assertTrue(_values.containsKey(key));
    }
    assertEquals(numGroups, _values.size());

    // Inter segments
    BrokerResponseNative brokerResponse = getBrokerResponseForPqlQuery(query);
    Assert.assertEquals(brokerResponse.getNumDocsScanned(), 4 * NUM_RECORDS);
    Assert.assertEquals(brokerResponse.getNumEntriesScannedInFilter(), 0);
    Assert.assertEquals(brokerResponse.getNumEntriesScannedPostFilter(), 4 * 2 * NUM_RECORDS);
    Assert.assertEquals(brokerResponse.getTotalDocs(), 4 * NUM_RECORDS);
    // size of this array will be equal to number of aggregation functions since
    // we return each aggregation function separately
    List<AggregationResult> aggregationResults = brokerResponse.getAggregationResults();
    int numAggregationColumns = aggregationResults.size();
    Assert.assertEquals(numAggregationColumns, 1);
    for (AggregationResult aggregationResult : aggregationResults) {
      Assert.assertNull(aggregationResult.getValue());
      List<GroupByResult> groupByResults = aggregationResult.getGroupByResult();
      numGroups = groupByResults.size();
      for (int i = 0; i < numGroups; i++) {
        GroupByResult groupByResult = groupByResults.get(i);
        List<String> group = groupByResult.getGroup();
        assertEquals(group.size(), 1);
        int key = Integer.parseInt(group.get(0));
        assertTrue(_values.containsKey(key));
        assertEquals(groupByResult.getValue(), BytesUtils.toHexString(GeometrySerializer.serialize(_values.get(key))));
      }
    }
  }

  @AfterClass
  public void tearDown()
      throws IOException {
    FileUtils.deleteDirectory(INDEX_DIR);
    _indexSegment.destroy();
  }
}
