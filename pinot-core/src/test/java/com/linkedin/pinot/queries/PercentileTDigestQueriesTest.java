/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.queries;

import com.clearspring.analytics.stream.quantile.TDigest;
import com.linkedin.pinot.common.data.DimensionFieldSpec;
import com.linkedin.pinot.common.data.FieldSpec;
import com.linkedin.pinot.common.data.MetricFieldSpec;
import com.linkedin.pinot.common.data.Schema;
import com.linkedin.pinot.common.response.broker.AggregationResult;
import com.linkedin.pinot.common.response.broker.BrokerResponseNative;
import com.linkedin.pinot.common.segment.ReadMode;
import com.linkedin.pinot.common.utils.DataSchema;
import com.linkedin.pinot.common.utils.primitive.ByteArray;
import com.linkedin.pinot.core.common.BlockValSet;
import com.linkedin.pinot.core.common.DataSource;
import com.linkedin.pinot.core.data.GenericRow;
import com.linkedin.pinot.core.data.manager.SegmentDataManager;
import com.linkedin.pinot.core.data.manager.offline.ImmutableSegmentDataManager;
import com.linkedin.pinot.core.data.readers.GenericRowRecordReader;
import com.linkedin.pinot.core.data.readers.RecordReader;
import com.linkedin.pinot.core.indexsegment.IndexSegment;
import com.linkedin.pinot.core.indexsegment.generator.SegmentGeneratorConfig;
import com.linkedin.pinot.core.indexsegment.immutable.ImmutableSegment;
import com.linkedin.pinot.core.indexsegment.immutable.ImmutableSegmentLoader;
import com.linkedin.pinot.core.io.compression.ChunkCompressorFactory;
import com.linkedin.pinot.core.operator.DocIdSetOperator;
import com.linkedin.pinot.core.operator.ProjectionOperator;
import com.linkedin.pinot.core.operator.blocks.IntermediateResultsBlock;
import com.linkedin.pinot.core.operator.blocks.ProjectionBlock;
import com.linkedin.pinot.core.operator.filter.MatchEntireSegmentOperator;
import com.linkedin.pinot.core.operator.query.AggregationGroupByOperator;
import com.linkedin.pinot.core.operator.query.AggregationOperator;
import com.linkedin.pinot.core.operator.query.SelectionOrderByOperator;
import com.linkedin.pinot.core.plan.DocIdSetPlanNode;
import com.linkedin.pinot.core.query.aggregation.AggregationResultHolder;
import com.linkedin.pinot.core.query.aggregation.ObjectAggregationResultHolder;
import com.linkedin.pinot.core.query.aggregation.function.PercentileTDigestAggregationFunction;
import com.linkedin.pinot.core.query.aggregation.groupby.AggregationGroupByResult;
import com.linkedin.pinot.core.query.aggregation.groupby.GroupKeyGenerator;
import com.linkedin.pinot.core.segment.creator.impl.SegmentIndexCreationDriverImpl;
import com.linkedin.pinot.core.segment.store.SegmentDirectory;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.stream.Collectors;
import org.apache.commons.io.FileUtils;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


/**
 * Test for PercentileEst aggregation feature.
 *
 * <ul>
 *   <li> Generates a segment with a TDigest column and a Group-by column. </li>
 *   <li> Runs aggregation and group-by queries on the generated segment. </li>
 *   <li> Asserts that results of queries are as expected. </li>
 * </ul>
 *
 */
public class PercentileTDigestQueriesTest extends BaseQueriesTest {
  private static final int NUM_ROWS = 100; // With TDigest compression of 100, test becomes brittle with large num rows.

  private static final String SEGMENT_DIR_NAME =
      System.getProperty("java.io.tmpdir") + File.separator + "tDigestSegTest";
  private static final String SEGMENT_NAME = "testSegment";

  private static final String TDIGEST_COLUMN = "tDigestColumn";
  private static final String GROUPBY_COLUMN = "groupByColumn";
  private static final String TABLE_NAME = "TDIGEST_TABLE";

  private static final String[] groups = new String[]{"abc", "def", "ghij", "klmno", "pqrst"};
  private static final String GROUP_BY_CLAUSE = " group by " + GROUPBY_COLUMN + " TOP " + groups.length;
  private static final int[] PERCENTILES_TO_COMPUTE = new int[]{5, 10, 20, 25, 30, 40, 50, 60, 70, 75, 80, 90, 95, 99};
  private static final long RANDOM_SEED = System.nanoTime();

  private Random _random;
  private RecordReader _recordReader;
  private ImmutableSegment _segment;
  private TDigest _expectedTDigest;
  private List<SegmentDataManager> _segmentDataManagers;
  private HashMap<String, TDigest> _expectedGroupBy;
  private List<String> _expectedSelectionTDigests;

  @Override
  protected String getFilter() {
    return ""; // No filtering required for this test.
  }

  @Override
  protected IndexSegment getIndexSegment() {
    return _segment;
  }

  @Override
  protected List<SegmentDataManager> getSegmentDataManagers() {
    return _segmentDataManagers;
  }

  /**
   * Setup to build a segment with raw indexes (no-dictionary) of various data types.
   *
   * @throws Exception
   */
  @BeforeClass
  public void setup()
      throws Exception {

    Schema schema = new Schema();
    schema.addField(new MetricFieldSpec(TDIGEST_COLUMN, FieldSpec.DataType.BYTES));
    schema.addField(new DimensionFieldSpec(GROUPBY_COLUMN, FieldSpec.DataType.STRING, true));

    _random = new Random(RANDOM_SEED);
    _recordReader = buildIndex(schema);
    _segment = ImmutableSegmentLoader.load(new File(SEGMENT_DIR_NAME, SEGMENT_NAME), ReadMode.heap);
    _segmentDataManagers =
        Arrays.asList(new ImmutableSegmentDataManager(_segment), new ImmutableSegmentDataManager(_segment));
  }

  /**
   * Clean up after test
   */
  @AfterClass
  public void cleanup()
      throws IOException {
    _recordReader.close();
    _segment.destroy();
    FileUtils.deleteQuietly(new File(SEGMENT_DIR_NAME));
  }

  /**
   * Unit test for {@link PercentileTDigestAggregationFunction}.
   */
  @Test
  public void testAggregationFunction() {
    Map<String, DataSource> dataSourceMap = new HashMap<>();
    dataSourceMap.put(TDIGEST_COLUMN, _segment.getDataSource(TDIGEST_COLUMN));

    MatchEntireSegmentOperator matchEntireSegmentOperator =
        new MatchEntireSegmentOperator(_segment.getSegmentMetadata().getTotalRawDocs());
    DocIdSetOperator docIdSetOperator =
        new DocIdSetOperator(matchEntireSegmentOperator, DocIdSetPlanNode.MAX_DOC_PER_CALL);
    ProjectionOperator projectionOperator = new ProjectionOperator(dataSourceMap, docIdSetOperator);

    ProjectionBlock projectionBlock = projectionOperator.nextBlock();

    for (int percentile : PERCENTILES_TO_COMPUTE) {
      PercentileTDigestAggregationFunction aggregationFunction = new PercentileTDigestAggregationFunction(percentile);
      BlockValSet blockValueSet = projectionBlock.getBlockValueSet(TDIGEST_COLUMN);

      AggregationResultHolder resultHolder = new ObjectAggregationResultHolder();
      aggregationFunction.aggregate(NUM_ROWS, resultHolder, blockValueSet);
      TDigest tDigest = aggregationFunction.extractAggregationResult(resultHolder);

      double actual = aggregationFunction.extractFinalResult(tDigest);
      double expected = _expectedTDigest.quantile(((double) percentile) / 100.0);

      Assert.assertEquals(actual, expected, 1e-5,
          "Failed for percentile: " + percentile + ". Random seed: " + RANDOM_SEED);
    }
  }

  /**
   * Test for selection queries.
   */
  @Test
  public void testSelectionQueries() {
    String query = "select * from " + TABLE_NAME + " order by " + TDIGEST_COLUMN + " LIMIT " + NUM_ROWS;

    SelectionOrderByOperator selectionOnlyOperator = getOperatorForQuery(query);
    IntermediateResultsBlock resultsBlock = selectionOnlyOperator.nextBlock();

    DataSchema dataSchema = resultsBlock.getSelectionDataSchema();
    int colIndex = Collections.singletonList(dataSchema.getColumnName(0)).indexOf(TDIGEST_COLUMN);

    List<String> _actualSelectedTDigests = resultsBlock.getSelectionResult()
        .stream()
        .map(selections -> (String) selections[colIndex])
        .sorted()
        .collect(Collectors.toList());

    Collections.sort(_expectedSelectionTDigests);

    Assert.assertEquals(_actualSelectedTDigests.size(), NUM_ROWS);
    Assert.assertEquals(_actualSelectedTDigests, _expectedSelectionTDigests);
  }

  /**
   * Test for aggregation queries.
   */
  @Test
  public void testAggregationQueries() {
    for (int percentile : PERCENTILES_TO_COMPUTE) {
      String query = buildBaseQuery(percentile);

      // Server side aggregation test:
      AggregationOperator aggregationOperator = getOperatorForQuery(query);
      IntermediateResultsBlock resultsBlock = aggregationOperator.nextBlock();

      double q = ((double) percentile) / 100.0;
      for (Object result : resultsBlock.getAggregationResult()) {
        TDigest actual = (TDigest) result;
        Assert.assertEquals(actual.quantile(q), _expectedTDigest.quantile(q), 1e-5);
      }

      // Broker side aggregation test:
      BrokerResponseNative brokerResponse = getBrokerResponseForQuery(query);
      AggregationResult aggregationResult = brokerResponse.getAggregationResults().get(0);
      double actual = Double.parseDouble((String) aggregationResult.getValue());

      // Expected result is as there were 2 servers with 2 segments each (see BaseQueriesTest).
      TDigest expected = new TDigest(PercentileTDigestAggregationFunction.DEFAULT_TDIGEST_COMPRESSION);
      for (int i = 0; i < 4; i++) {
        expected.add(_expectedTDigest);
      }
      Assert.assertEquals(actual, expected.quantile(q), 1e-5);
    }
  }

  /**
   * Test for group-by queries.
   */
  @Test
  public void testGroupByQueries() {
    for (int percentile : PERCENTILES_TO_COMPUTE) {
      String query = buildBaseQuery(percentile) + GROUP_BY_CLAUSE;

      AggregationGroupByOperator aggregationOperator = getOperatorForQuery(query);
      IntermediateResultsBlock resultsBlock = aggregationOperator.nextBlock();

      AggregationGroupByResult groupByResult = resultsBlock.getAggregationGroupByResult();
      Iterator<GroupKeyGenerator.GroupKey> groupKeyIterator = groupByResult.getGroupKeyIterator();

      int numGroups = 0;
      while (groupKeyIterator.hasNext()) {
        GroupKeyGenerator.GroupKey groupKey = groupKeyIterator.next();
        TDigest actual = (TDigest) groupByResult.getResultForKey(groupKey, 0);
        double q = ((double) percentile) / 100.0;
        Assert.assertEquals(actual.quantile(q), _expectedGroupBy.get(groupKey._stringKey).quantile(q), 1e-5);
        numGroups++;
      }

      Assert.assertEquals(numGroups, groups.length);
    }
  }

  /**
   * Helper method to build a segment containing a single valued string column with RAW (no-dictionary) index.
   *
   * @return Array of string values for the rows in the generated index.
   * @throws Exception
   */

  private RecordReader buildIndex(Schema schema)
      throws Exception {
    SegmentGeneratorConfig config = new SegmentGeneratorConfig(schema);

    config.setOutDir(SEGMENT_DIR_NAME);
    config.setSegmentName(SEGMENT_NAME);
    config.setTableName(TABLE_NAME);
    config.setRawIndexCreationColumns(Arrays.asList(TDIGEST_COLUMN));

    List<GenericRow> rows = new ArrayList<>(NUM_ROWS);
    _expectedTDigest = new TDigest(PercentileTDigestAggregationFunction.DEFAULT_TDIGEST_COMPRESSION);

    _expectedGroupBy = new HashMap<>();
    _expectedSelectionTDigests = new ArrayList<>(NUM_ROWS);

    for (int i = 0; i < NUM_ROWS; i++) {
      HashMap<String, Object> map = new HashMap<>();

      // Set the value for fixed-byte sorted column.
      double value = _random.nextDouble();
      TDigest tDigest = new TDigest(PercentileTDigestAggregationFunction.DEFAULT_TDIGEST_COMPRESSION);
      tDigest.add(value);
      _expectedTDigest.add(tDigest);

      ByteBuffer byteBuffer = ByteBuffer.allocate(tDigest.byteSize());
      tDigest.asBytes(byteBuffer);
      _expectedSelectionTDigests.add(ByteArray.toHexString(byteBuffer.array()));

      // Add the TDigest column.
      byte[] bytes = byteBuffer.array();
      map.put(TDIGEST_COLUMN, bytes);

      // Add the GroupBy column.
      String group = groups[_random.nextInt(groups.length)];
      map.put(GROUPBY_COLUMN, group);

      // Generate the groups.
      TDigest groupTDigest = _expectedGroupBy.get(group);
      if (groupTDigest == null) {
        _expectedGroupBy.put(group, tDigest);
      } else {
        groupTDigest.add(tDigest);
      }

      GenericRow genericRow = new GenericRow();
      genericRow.init(map);
      rows.add(genericRow);
    }

    RecordReader recordReader = new GenericRowRecordReader(rows, schema);
    SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
    driver.init(config, recordReader);
    driver.build();

    SegmentDirectory.createFromLocalFS(driver.getOutputDirectory(), ReadMode.mmap);
    recordReader.rewind();

    TDigest actual = new TDigest(PercentileTDigestAggregationFunction.DEFAULT_TDIGEST_COMPRESSION);
    GenericRow reuse = new GenericRow();
    while (recordReader.hasNext()) {
      reuse = recordReader.next(reuse);
      actual.add(TDigest.fromBytes(ByteBuffer.wrap((byte[]) reuse.getValue(TDIGEST_COLUMN))));
    }
    return recordReader;
  }

  private String buildBaseQuery(int percentile) {
    return "select percentiletdigest" + percentile +
        "(" +
        TDIGEST_COLUMN +
        ")" +
        " from " +
        TABLE_NAME;
  }
}
