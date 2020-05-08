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
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.common.response.broker.AggregationResult;
import org.apache.pinot.common.response.broker.BrokerResponseNative;
import org.apache.pinot.common.segment.ReadMode;
import org.apache.pinot.spi.data.TimeGranularitySpec;
import org.apache.pinot.spi.utils.TimeUtils;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.core.data.manager.SegmentDataManager;
import org.apache.pinot.core.data.manager.offline.ImmutableSegmentDataManager;
import org.apache.pinot.core.data.readers.GenericRowRecordReader;
import org.apache.pinot.spi.data.readers.RecordReader;
import org.apache.pinot.core.indexsegment.IndexSegment;
import org.apache.pinot.core.indexsegment.generator.SegmentGeneratorConfig;
import org.apache.pinot.core.indexsegment.immutable.ImmutableSegment;
import org.apache.pinot.core.indexsegment.immutable.ImmutableSegmentLoader;
import org.apache.pinot.core.operator.blocks.IntermediateResultsBlock;
import org.apache.pinot.core.operator.query.AggregationGroupByOperator;
import org.apache.pinot.core.operator.query.AggregationOperator;
import org.apache.pinot.core.query.aggregation.function.customobject.AvgPair;
import org.apache.pinot.core.query.aggregation.groupby.AggregationGroupByResult;
import org.apache.pinot.core.query.aggregation.groupby.GroupKeyGenerator;
import org.apache.pinot.core.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class TransformQueriesTest extends BaseQueriesTest {

  private static final String D1 = "STRING_COL";
  private static final String M1 = "INT_COL1";
  private static final String M2 = "INT_COL2";
  private static final String M3 = "LONG_COL1";
  private static final String M4 = "LONG_COL2";
  private static final String TIME = "T";
  private static final String TABLE_NAME = "FOO";
  private static final String SEGMENT_NAME_1 = "FOO_SEGMENT_1";
  private static final String SEGMENT_NAME_2 = "FOO_SEGMENT_2";

  private static final File INDEX_DIR = new File(FileUtils.getTempDirectory(), "TransformQueriesTest");

  private Schema _schema;
  private TableConfig _tableConfig;
  private List<IndexSegment> _indexSegments = new ArrayList<>();
  private List<SegmentDataManager> _segmentDataManagers;

  @BeforeClass
  public void setUp() {
    _schema =
        new Schema.SchemaBuilder().setSchemaName(TABLE_NAME).addSingleValueDimension(D1, FieldSpec.DataType.STRING)
            .addSingleValueDimension(M1, FieldSpec.DataType.INT).addSingleValueDimension(M2, FieldSpec.DataType.INT)
            .addSingleValueDimension(M3, FieldSpec.DataType.LONG).addSingleValueDimension(M4, FieldSpec.DataType.LONG)
            .addTime(new TimeGranularitySpec(FieldSpec.DataType.LONG, TimeUnit.MILLISECONDS, TIME), null)
            .build();
    _tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName("test").setTimeColumnName(TIME).build();
  }

  @AfterClass
  public void tearDown() {
    FileUtils.deleteQuietly(INDEX_DIR);
  }

  private void destroySegments() {
    for (IndexSegment indexSegment : _indexSegments) {
      if (indexSegment != null) {
        indexSegment.destroy();
      }
    }
    _indexSegments.clear();
  }

  @Test
  public void testTransformWithAvgInnerSegment()
      throws Exception {
    try {
      final List<GenericRow> rows = createDataSet(10);
      try (final RecordReader recordReader = new GenericRowRecordReader(rows)) {
        createSegment(_tableConfig, _schema, recordReader, SEGMENT_NAME_1, TABLE_NAME);
        final ImmutableSegment segment = loadSegment(SEGMENT_NAME_1);
        _indexSegments.add(segment);

        String query = "SELECT AVG(SUB(INT_COL1, INT_COL2)) FROM foo";
        runAndVerifyInnerSegmentQuery(query, -10000.00, 10);

        query = "SELECT AVG(SUB(LONG_COL1, INT_COL1)) FROM foo";
        runAndVerifyInnerSegmentQuery(query, 4990000.00, 10);

        query = "SELECT AVG(SUB(LONG_COL2, LONG_COL1)) FROM foo";
        runAndVerifyInnerSegmentQuery(query, 5000000.00, 10);

        query = "SELECT AVG(ADD(INT_COL1, INT_COL2)) FROM foo";
        runAndVerifyInnerSegmentQuery(query, 30000.00, 10);

        query = "SELECT AVG(ADD(INT_COL1, LONG_COL1)) FROM foo";
        runAndVerifyInnerSegmentQuery(query, 5010000.00, 10);

        query = "SELECT AVG(ADD(LONG_COL1, LONG_COL2)) FROM foo";
        runAndVerifyInnerSegmentQuery(query, 15000000.00, 10);

        query = "SELECT AVG(ADD(DIV(INT_COL1, INT_COL2), DIV(LONG_COL1, LONG_COL2))) FROM foo";
        runAndVerifyInnerSegmentQuery(query, 10.00, 10);

        try {
          query = "SELECT AVG(SUB(INT_COL1, STRING_COL)) FROM foo";
          runAndVerifyInnerSegmentQuery(query, -10000.00, 10);
          Assert.fail("Query should have failed");
        } catch (Exception e) {
        }

        try {
          query = "SELECT AVG(ADD(DIV(INT_COL1, INT_COL2), DIV(LONG_COL1, STRING_COL))) FROM foo";
          runAndVerifyInnerSegmentQuery(query, 10.00, 10);
          Assert.fail("Query should have failed");
        } catch (Exception e) {
        }
      }
    } finally {
      destroySegments();
    }
  }

  @Test
  public void testTransformWithDateTruncInnerSegment()
      throws Exception {
    Object[] columns = new Object[]{"Pinot", 1000, 2000, 500000, 1000000};
    long start = new DateTime(1973, 1, 8, 14, 6, 4, 3, DateTimeZone.UTC).getMillis();

    List<GenericRow> rows = new ArrayList<>();
    for (int i = 0; i < 7; i++) {
      GenericRow row = new GenericRow();
      row.putField(D1, columns[0]);
      row.putField(M1, columns[1]);
      row.putField(M2, columns[2]);
      row.putField(M3, columns[3]);
      row.putField(M4, columns[4]);
      row.putField(TIME, ThreadLocalRandom.current().nextLong(start, start + 5000));
      rows.add(row);
    }

    try (final RecordReader recordReader = new GenericRowRecordReader(rows)) {
      createSegment(_tableConfig, _schema, recordReader, SEGMENT_NAME_1, TABLE_NAME);
      final ImmutableSegment segment = loadSegment(SEGMENT_NAME_1);
      _indexSegments.add(segment);

      String query =
          "SELECT COUNT(*) FROM foo GROUP BY DATETRUNC('week', ADD(SUB(DIV(T, 1000), INT_COL2), INT_COL2), 'SECONDS', 'Europe/Berlin')";
      verifyDateTruncationResult(query, rows.size(), "95295600");

      query =
          "SELECT COUNT(*) FROM foo GROUP BY DATETRUNC('week', DIV(MULT(DIV(ADD(SUB(T, 5), 5), 1000), INT_COL2), INT_COL2), 'SECONDS', 'Europe/Berlin', 'MILLISECONDS')";
      verifyDateTruncationResult(query, rows.size(), "95295600000");

      query = "SELECT COUNT(*) FROM foo GROUP BY DATETRUNC('quarter', T, 'MILLISECONDS')";
      verifyDateTruncationResult(query, rows.size(), "94694400000");
    } finally {
      destroySegments();
    }
  }

  private void verifyDateTruncationResult(String query, long countResult, String stringKey) {
    AggregationGroupByOperator aggregtionGroupByOperator = getOperatorForQuery(query);
    IntermediateResultsBlock resultsBlock = aggregtionGroupByOperator.nextBlock();
    final AggregationGroupByResult aggregationGroupByResult = resultsBlock.getAggregationGroupByResult();
    List<GroupKeyGenerator.GroupKey> groupKeys = ImmutableList.copyOf(aggregationGroupByResult.getGroupKeyIterator());
    Assert.assertEquals(groupKeys.size(), 1);
    Assert.assertEquals(groupKeys.get(0)._stringKey, stringKey);
    Object resultForKey = aggregationGroupByResult.getResultForKey(groupKeys.get(0), 0);
    Assert.assertEquals(resultForKey, countResult);
  }

  @Test
  public void testTransformWithAvgInterSegmentInterServer()
      throws Exception {
    try {
      final List<GenericRow> segmentOneRows = createDataSet(10);
      final List<GenericRow> segmentTwoRows = createDataSet(10);

      try (final RecordReader recordReaderOne = new GenericRowRecordReader(segmentOneRows);
          final RecordReader recordReaderTwo = new GenericRowRecordReader(segmentTwoRows)) {
        createSegment(_tableConfig, _schema, recordReaderOne, SEGMENT_NAME_1, TABLE_NAME);
        createSegment(_tableConfig, _schema, recordReaderTwo, SEGMENT_NAME_2, TABLE_NAME);

        final ImmutableSegment segmentOne = loadSegment(SEGMENT_NAME_1);
        final ImmutableSegment segmentTwo = loadSegment(SEGMENT_NAME_2);

        _indexSegments.add(segmentOne);
        _indexSegments.add(segmentTwo);

        _segmentDataManagers =
            Arrays.asList(new ImmutableSegmentDataManager(segmentOne), new ImmutableSegmentDataManager(segmentTwo));

        String query = "SELECT AVG(SUB(INT_COL1, INT_COL2)) FROM foo";
        runAndVerifyInterSegmentQuery(query, "-1000.00000");

        query = "SELECT AVG(SUB(LONG_COL1, INT_COL1)) FROM foo";
        runAndVerifyInterSegmentQuery(query, "499000.00000");

        query = "SELECT AVG(SUB(LONG_COL2, LONG_COL1)) FROM foo";
        runAndVerifyInterSegmentQuery(query, "500000.00000");

        query = "SELECT AVG(ADD(INT_COL1, INT_COL2)) FROM foo";
        runAndVerifyInterSegmentQuery(query, "3000.00000");

        query = "SELECT AVG(ADD(INT_COL1, LONG_COL1)) FROM foo";
        runAndVerifyInterSegmentQuery(query, "501000.00000");

        query = "SELECT AVG(ADD(LONG_COL1, LONG_COL2)) FROM foo";
        runAndVerifyInterSegmentQuery(query, "1500000.00000");

        query = "SELECT AVG(ADD(DIV(INT_COL1, INT_COL2), DIV(LONG_COL1, LONG_COL2))) FROM foo";
        runAndVerifyInterSegmentQuery(query, "1.00000");
      }
    } finally {
      destroySegments();
    }
  }

  private List<GenericRow> createDataSet(final int numRows) {
    final ThreadLocalRandom random = ThreadLocalRandom.current();
    final List<GenericRow> rows = new ArrayList<>(numRows);
    Object[] columns;

    // ROW
    GenericRow row = new GenericRow();
    columns = new Object[]{"Pinot", 1000, 2000, 500000, 1000000};
    row.putField(D1, columns[0]);
    row.putField(M1, columns[1]);
    row.putField(M2, columns[2]);
    row.putField(M3, columns[3]);
    row.putField(M4, columns[4]);
    row.putField(TIME, random.nextLong(TimeUtils.getValidMinTimeMillis(), TimeUtils.getValidMaxTimeMillis()));

    for (int i = 0; i < numRows; i++) {
      rows.add(row);
    }
    return rows;
  }

  private void runAndVerifyInnerSegmentQuery(String query, double sum, long count) {
    AggregationOperator aggregationOperator = getOperatorForQuery(query);
    IntermediateResultsBlock resultsBlock = aggregationOperator.nextBlock();
    final List<Object> aggregationResult = resultsBlock.getAggregationResult();
    AvgPair avgPair = (AvgPair) aggregationResult.get(0);
    Assert.assertEquals(avgPair.getSum(), sum);
    Assert.assertEquals(avgPair.getCount(), count);
  }

  private void runAndVerifyInterSegmentQuery(String query, String serialized) {
    BrokerResponseNative brokerResponse = getBrokerResponseForPqlQuery(query);
    List<AggregationResult> aggregationResults = brokerResponse.getAggregationResults();
    Assert.assertEquals(aggregationResults.size(), 1);
    Serializable value = aggregationResults.get(0).getValue();
    Assert.assertEquals(value.toString(), serialized);
  }

  @Override
  protected String getFilter() {
    return "";
  }

  @Override
  protected IndexSegment getIndexSegment() {
    return _indexSegments.get(0);
  }

  @Override
  protected List<SegmentDataManager> getSegmentDataManagers() {
    return _segmentDataManagers;
  }

  private void createSegment(TableConfig tableConfig, Schema schema, RecordReader recordReader, String segmentName,
      String tableName)
      throws Exception {
    SegmentGeneratorConfig segmentGeneratorConfig = new SegmentGeneratorConfig(tableConfig, schema);
    segmentGeneratorConfig.setTableName(tableName);
    segmentGeneratorConfig.setOutDir(INDEX_DIR.getAbsolutePath());
    segmentGeneratorConfig.setSegmentName(segmentName);

    SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
    driver.init(segmentGeneratorConfig, recordReader);
    driver.build();

    File segmentIndexDir = new File(INDEX_DIR.getAbsolutePath(), segmentName);
    if (!segmentIndexDir.exists()) {
      throw new IllegalStateException("Segment generation failed");
    }
  }

  private ImmutableSegment loadSegment(String segmentName)
      throws Exception {
    return ImmutableSegmentLoader.load(new File(INDEX_DIR, segmentName), ReadMode.heap);
  }
}
