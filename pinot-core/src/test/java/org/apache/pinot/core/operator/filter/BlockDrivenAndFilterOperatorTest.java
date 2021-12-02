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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.core.common.BlockDocIdIterator;
import org.apache.pinot.core.operator.DocIdSetOperator;
import org.apache.pinot.core.operator.ProjectionOperator;
import org.apache.pinot.core.operator.VisitableOperator;
import org.apache.pinot.core.operator.blocks.TransformBlock;
import org.apache.pinot.core.operator.transform.TransformOperator;
import org.apache.pinot.core.plan.DocIdSetPlanNode;

import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.core.query.request.context.utils.QueryContextConverterUtils;
import org.apache.pinot.segment.local.indexsegment.immutable.ImmutableSegmentLoader;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.segment.local.segment.readers.GenericRowRecordReader;
import org.apache.pinot.segment.spi.Constants;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
import org.apache.pinot.segment.spi.datasource.DataSource;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.MetricFieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.utils.ReadMode;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;

import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertTrue;

public class BlockDrivenAndFilterOperatorTest {
  private static final File INDEX_DIR = new File(FileUtils.getTempDirectory(),
      "BlockDrivenAndFilterOperatorTest");
  private static final String SEGMENT_NAME = "TestBlockDrivenAndOperator";

  private static final String METRIC_PREFIX = "metric_";
  private static final int NUM_METRIC_COLUMNS = 2;
  private static final int NUM_ROWS = 20000;

  public static IndexSegment _indexSegment;
  private String[] _columns;
  private QueryContext _queryContext;
  private double[][] _inputData;

  /**
   * Initializations prior to the test:
   * - Build a segment with metric columns (that will be aggregated) containing
   *  randomly generated data.
   *
   * @throws Exception
   */
  @BeforeClass
  public void setUp()
      throws Exception {
    int numColumns = NUM_METRIC_COLUMNS;

    _inputData = new double[numColumns][NUM_ROWS];

    _columns = new String[numColumns];

    setupSegment();

    StringBuilder queryBuilder = new StringBuilder("SELECT SUM(INT_COL)");

    queryBuilder.append(" FROM testTable");

    _queryContext = QueryContextConverterUtils.getQueryContextFromSQL(queryBuilder.toString());
  }

  /**
   * Helper method to setup the index segment on which to perform aggregation tests.
   * - Generates a segment with {@link #NUM_METRIC_COLUMNS} and {@link #NUM_ROWS}
   * - Random 'double' data filled in the metric columns. The data is also populated
   *   into the _inputData[], so it can be used to test the results.
   *
   * @throws Exception
   */
  private void setupSegment()
      throws Exception {
    if (INDEX_DIR.exists()) {
      FileUtils.deleteQuietly(INDEX_DIR);
    }

    SegmentGeneratorConfig config =
        new SegmentGeneratorConfig(new TableConfigBuilder(TableType.OFFLINE).setTableName("test")
            .build(),
            buildSchema());
    config.setSegmentName(SEGMENT_NAME);
    config.setOutDir(INDEX_DIR.getAbsolutePath());

    List<GenericRow> rows = new ArrayList<>(NUM_ROWS);
    for (int i = 0; i < NUM_ROWS; i++) {
      Map<String, Object> map = new HashMap<>();

      for (int j = 0; j < _columns.length; j++) {
        String metricName = _columns[j];
        int value = i;
        _inputData[j][i] = value;
        map.put(metricName, value);
      }

      GenericRow genericRow = new GenericRow();
      genericRow.init(map);
      rows.add(genericRow);
    }

    SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
    driver.init(config, new GenericRowRecordReader(rows));
    driver.build();

    _indexSegment = ImmutableSegmentLoader.load(new File(INDEX_DIR, driver.getSegmentName()), ReadMode.heap);
  }

  /**
   * Helper method to build schema for the segment on which aggregation tests will be run.
   *
   * @return
   */
  private Schema buildSchema() {
    Schema schema = new Schema();

    for (int i = 0; i < NUM_METRIC_COLUMNS; i++) {
      String metricName = METRIC_PREFIX + i;
      MetricFieldSpec metricFieldSpec = new MetricFieldSpec(metricName, FieldSpec.DataType.DOUBLE);
      schema.addField(metricFieldSpec);
      _columns[i] = metricName;
    }
    return schema;
  }

  @AfterClass
  void tearDown() {
    FileUtils.deleteQuietly(INDEX_DIR);
  }

  @Test
  void testBlockFilteredAggregation() {
    int totalDocs = _indexSegment.getSegmentMetadata().getTotalDocs();

    executeTest(new MatchAllFilterOperator(totalDocs), null);

    int limit = NUM_ROWS / 4;
    int[] dataArray = new int[limit];

    Set<Integer> seenNumber = new HashSet<>();

    for (int i = 0; i < limit; i++) {
      dataArray[i] = getRandomNumber(i, NUM_ROWS, seenNumber);
    }

    BitmapBasedFilterOperator bitmapBasedFilterOperator = new BitmapBasedFilterOperator(
        ImmutableRoaringBitmap.bitmapOf(dataArray), false, totalDocs);

    executeTest(bitmapBasedFilterOperator, dataArray);
  }

  private void executeTest(BaseFilterOperator baseFilterOperator, int[] addedDocIDs) {
    Map<String, DataSource> dataSourceMap = new HashMap<>();
    List<ExpressionContext> expressions = new ArrayList<>();
    for (String column : _indexSegment.getPhysicalColumnNames()) {
      dataSourceMap.put(column, _indexSegment.getDataSource(column));
      expressions.add(ExpressionContext.forIdentifier(column));
    }
    int totalDocs = _indexSegment.getSegmentMetadata().getTotalDocs();

    DocIdSetOperator docIdSetOperator = new DocIdSetOperator(baseFilterOperator,
        DocIdSetPlanNode.MAX_DOC_PER_CALL);
    ProjectionOperator projectionOperator = new ProjectionOperator(dataSourceMap, docIdSetOperator);
    TransformOperator transformOperator = new TransformOperator(_queryContext, projectionOperator, expressions);
    TransformBlock transformBlock = transformOperator.nextBlock();

    int limit = NUM_ROWS / 2;
    int[] dataArray = new int[limit];

    Set<Integer> seenNumber = new HashSet<>();

    for (int i = 0; i < limit; i++) {
      dataArray[i] = getRandomNumber(i, NUM_ROWS, seenNumber);
    }

    int[] finalArray;

    if (addedDocIDs != null) {
      finalArray = performIntersection(dataArray, addedDocIDs);
    } else {
      finalArray = dataArray;
    }

    assertTrue(finalArray != null);

    BitmapBasedFilterOperator bitmapBasedFilterOperator = new BitmapBasedFilterOperator(
        ImmutableRoaringBitmap.bitmapOf(finalArray), false, totalDocs);
    BlockDrivenAndFilterOperator blockDrivenAndFilterOperator = new BlockDrivenAndFilterOperator(
        bitmapBasedFilterOperator, totalDocs);

    Set<Integer> resultSet = new HashSet<>();

    while (transformBlock != null) {
      ((VisitableOperator) blockDrivenAndFilterOperator).accept(transformBlock);

      BlockDocIdIterator blockDocIdIterator = blockDrivenAndFilterOperator.getNextBlock()
          .getBlockDocIdSet().iterator();

      int currentValue = blockDocIdIterator.next();

      while (currentValue != Constants.EOF) {
        resultSet.add(currentValue);

        currentValue = blockDocIdIterator.next();
      }

      transformBlock = transformOperator.nextBlock();
    }

    assert resultSet.size() == finalArray.length;

    for (int i = 0; i < finalArray.length; i++) {
      assertTrue(resultSet.contains(finalArray[i]), "Result does not contain " + finalArray[i]);
    }
  }

  public static int[] performIntersection(int[] a, int[] b) {
    return Arrays.stream(Stream.of(a)
        .filter(Arrays.asList(b)::contains)
        .toArray(Integer[]::new))
        .mapToInt(Integer::intValue)
        .toArray();
  }

  private int getRandomNumber(int min, int max, Set<Integer> seenNumbers) {
    int result = (int) ((Math.random() * (max - min)) + min);

    while (seenNumbers.contains(result)) {
      result = (int) ((Math.random() * (max - min)) + min);
    }

    seenNumbers.add(result);

    return result;
  }
}
