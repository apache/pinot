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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.core.operator.query.AggregationOperator;
import org.apache.pinot.core.operator.query.GroupByOperator;
import org.apache.pinot.core.query.aggregation.groupby.AggregationGroupByResult;
import org.apache.pinot.core.query.aggregation.groupby.GroupKeyGenerator;
import org.apache.pinot.segment.local.customobject.AvgPair;
import org.apache.pinot.segment.local.indexsegment.immutable.ImmutableSegmentLoader;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.segment.local.segment.readers.GenericRowRecordReader;
import org.apache.pinot.segment.spi.ImmutableSegment;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.config.table.ingestion.IngestionConfig;
import org.apache.pinot.spi.config.table.ingestion.TransformConfig;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.TimeGranularitySpec;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.data.readers.RecordReader;
import org.apache.pinot.spi.utils.ReadMode;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;


public class TransformQueriesTest extends BaseQueriesTest {
  private static final File INDEX_DIR = new File(FileUtils.getTempDirectory(), "TransformQueriesTest");
  private static final String TABLE_NAME = "testTable";
  private static final String SEGMENT_NAME = "testSegment";

  private static final String D1 = "STRING_COL";
  private static final String M1 = "INT_COL1";
  private static final String M1_V2 = "INT_COL1_V2";
  private static final String M1_V3 = "INT_COL1_V3";
  private static final String M2 = "INT_COL2";
  private static final String M3 = "LONG_COL1";
  private static final String M4 = "LONG_COL2";
  private static final String TIME = "T";

  private static final int NUM_ROWS = 10;

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

    buildSegment();
    ImmutableSegment immutableSegment = ImmutableSegmentLoader.load(new File(INDEX_DIR, SEGMENT_NAME), ReadMode.mmap);
    _indexSegment = immutableSegment;
    _indexSegments = Arrays.asList(immutableSegment, immutableSegment);
  }

  protected void buildSegment()
      throws Exception {
    GenericRow row = new GenericRow();
    row.putValue(D1, "Pinot");
    row.putValue(M1, 1000);
    row.putValue(M2, 2000);
    row.putValue(M3, 500000);
    row.putValue(M4, 1000000);
    row.putValue(TIME, new DateTime(1973, 1, 8, 14, 6, 4, 3, DateTimeZone.UTC).getMillis());

    List<GenericRow> rows = new ArrayList<>(NUM_ROWS);
    for (int i = 0; i < NUM_ROWS - 1; i++) {
      rows.add(row);
    }
    // modifying the last row
    row = new GenericRow();
    row.putValue(D1, "Pinot");
    row.putValue(M1, 1000);
    row.putValue(M1_V2, null); // column for adding the groovy tranformed value
    row.putValue(M1_V3, null); // M1_V3 doesn't exist in table schema
    row.putValue(M2, 2000);
    row.putValue(M3, 500000);
    row.putValue(M4, 1000000);
    row.putValue(TIME, new DateTime(1973, 1, 8, 14, 6, 4, 3, DateTimeZone.UTC).getMillis());
    rows.add(row);

    TableConfig tableConfig =
        new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME).setTimeColumnName(TIME)
        .setIngestionConfig(new IngestionConfig(null, null, null,
            Arrays.asList(new TransformConfig(M1_V2, "Groovy({INT_COL1_V3  == null || "
                + "INT_COL1_V3 == Integer.MIN_VALUE ? INT_COL1 : INT_COL1_V3 }, INT_COL1, INT_COL1_V3)")),
            null, null, null))
        .build();
    Schema schema =
        new Schema.SchemaBuilder().setSchemaName(TABLE_NAME).addSingleValueDimension(D1, FieldSpec.DataType.STRING)
            .addSingleValueDimension(M1, FieldSpec.DataType.INT).addSingleValueDimension(M2, FieldSpec.DataType.INT)
            .addSingleValueDimension(M3, FieldSpec.DataType.LONG).addSingleValueDimension(M4, FieldSpec.DataType.LONG)
            .addSingleValueDimension(M1_V2, FieldSpec.DataType.INT)
            .addTime(new TimeGranularitySpec(FieldSpec.DataType.LONG, TimeUnit.MILLISECONDS, TIME), null).build();
    SegmentGeneratorConfig config = new SegmentGeneratorConfig(tableConfig, schema);
    config.setOutDir(INDEX_DIR.getPath());
    config.setTableName(TABLE_NAME);
    config.setSegmentName(SEGMENT_NAME);

    SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
    try (RecordReader recordReader = new GenericRowRecordReader(rows)) {
      driver.init(config, recordReader);
      driver.build();
    }
  }

  @AfterClass
  public void tearDown() {
    _indexSegment.destroy();
    FileUtils.deleteQuietly(INDEX_DIR);
  }

  @Test
  public void testTransformWithAvgInnerSegment() {
    String query = "SELECT AVG(SUB(INT_COL1, INT_COL2)) FROM testTable";
    runAndVerifyInnerSegmentQuery(query, -10000.0, 10);

    query = "SELECT AVG(SUB(LONG_COL1, INT_COL1)) FROM testTable";
    runAndVerifyInnerSegmentQuery(query, 4990000.0, 10);

    query = "SELECT AVG(SUB(LONG_COL2, LONG_COL1)) FROM testTable";
    runAndVerifyInnerSegmentQuery(query, 5000000.0, 10);

    query = "SELECT AVG(ADD(INT_COL1, INT_COL2)) FROM testTable";
    runAndVerifyInnerSegmentQuery(query, 30000.0, 10);

    query = "SELECT AVG(ADD(INT_COL1, LONG_COL1)) FROM testTable";
    runAndVerifyInnerSegmentQuery(query, 5010000.0, 10);

    query = "SELECT AVG(ADD(LONG_COL1, LONG_COL2)) FROM testTable";
    runAndVerifyInnerSegmentQuery(query, 15000000.0, 10);

    query = "SELECT AVG(ADD(DIV(INT_COL1, INT_COL2), DIV(LONG_COL1, LONG_COL2))) FROM testTable";
    runAndVerifyInnerSegmentQuery(query, 10.0, 10);

    try {
      query = "SELECT AVG(SUB(INT_COL1, STRING_COL)) FROM testTable";
      runAndVerifyInnerSegmentQuery(query, -10000.0, 10);
      Assert.fail("Query should have failed");
    } catch (Exception e) {
      // Expected
    }

    try {
      query = "SELECT AVG(ADD(DIV(INT_COL1, INT_COL2), DIV(LONG_COL1, STRING_COL))) FROM testTable";
      runAndVerifyInnerSegmentQuery(query, 10.00, 10);
      Assert.fail("Query should have failed");
    } catch (Exception e) {
      // Expected
    }
  }

  private void runAndVerifyInnerSegmentQuery(String query, double expectedSum, int expectedCount) {
    AggregationOperator aggregationOperator = getOperator(query);
    List<Object> aggregationResult = aggregationOperator.nextBlock().getResults();
    assertNotNull(aggregationResult);
    assertEquals(aggregationResult.size(), 1);
    AvgPair avgPair = (AvgPair) aggregationResult.get(0);
    assertEquals(avgPair.getSum(), expectedSum);
    assertEquals(avgPair.getCount(), expectedCount);
  }

  @Test
  public void testTransformWithDateTruncInnerSegment() {
    String query =
        "SELECT COUNT(*) FROM testTable GROUP BY DATETRUNC('week', ADD(SUB(DIV(T, 1000), INT_COL2), INT_COL2), "
            + "'SECONDS', 'Europe/Berlin')";
    verifyDateTruncationResult(query, new Object[]{95295600L});

    query =
        "SELECT COUNT(*) FROM testTable GROUP BY DATETRUNC('week', DIV(MULT(DIV(ADD(SUB(T, 5), 5), 1000), INT_COL2), "
            + "INT_COL2), 'SECONDS', 'Europe/Berlin', 'MILLISECONDS')";
    verifyDateTruncationResult(query, new Object[]{95295600000L});

    query = "SELECT COUNT(*) FROM testTable GROUP BY DATETRUNC('quarter', T, 'MILLISECONDS')";
    verifyDateTruncationResult(query, new Object[]{94694400000L});
  }

  private void verifyDateTruncationResult(String query, Object[] expectedGroupKey) {
    GroupByOperator groupByOperator = getOperator(query);
    AggregationGroupByResult aggregationGroupByResult = groupByOperator.nextBlock().getAggregationGroupByResult();
    assertNotNull(aggregationGroupByResult);
    List<GroupKeyGenerator.GroupKey> groupKeys = ImmutableList.copyOf(aggregationGroupByResult.getGroupKeyIterator());
    assertEquals(groupKeys.size(), 1);
    assertEquals(groupKeys.get(0)._keys, expectedGroupKey);
    Object resultForKey = aggregationGroupByResult.getResultForGroupId(groupKeys.get(0)._groupId, 0);
    assertEquals(resultForKey, (long) NUM_ROWS);
  }

  @Test
  public void testTransformWithAvgInterSegmentInterServer() {
    String query = "SELECT AVG(SUB(INT_COL1, INT_COL2)) FROM testTable";
    runAndVerifyInterSegmentQuery(query, -1000.0);

    query = "SELECT AVG(SUB(LONG_COL1, INT_COL1)) FROM testTable";
    runAndVerifyInterSegmentQuery(query, 499000.0);

    query = "SELECT AVG(SUB(LONG_COL2, LONG_COL1)) FROM testTable";
    runAndVerifyInterSegmentQuery(query, 500000.0);

    query = "SELECT AVG(ADD(INT_COL1, INT_COL2)) FROM testTable";
    runAndVerifyInterSegmentQuery(query, 3000.0);

    query = "SELECT AVG(ADD(INT_COL1, LONG_COL1)) FROM testTable";
    runAndVerifyInterSegmentQuery(query, 501000.0);

    query = "SELECT AVG(ADD(LONG_COL1, LONG_COL2)) FROM testTable";
    runAndVerifyInterSegmentQuery(query, 1500000.0);

    query = "SELECT AVG(ADD(DIV(INT_COL1, INT_COL2), DIV(LONG_COL1, LONG_COL2))) FROM testTable";
    runAndVerifyInterSegmentQuery(query, 1.0);
  }

  /**
   * This test checks the groovy transform when generic raw data could have some values that can be used to
   * ingest values into pinot column with a different name.
   */
  @Test
  public void testGroovyTransformQuery() {
    String query = "SELECT INT_COL1, INT_COL1_V2 FROM testTable";
    List<Object[]> result = getBrokerResponse(query).getResultTable().getRows();
    for (Object[] obj : result) {
      assertEquals(obj[0], obj[1]);
    }
  }

  private void runAndVerifyInterSegmentQuery(String query, double expectedResult) {
    assertEquals(getBrokerResponse(query).getResultTable().getRows().get(0)[0], expectedResult);
  }
}
