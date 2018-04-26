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
package com.linkedin.pinot.query.aggregation;

import com.clearspring.analytics.stream.quantile.TDigest;
import com.linkedin.pinot.common.data.DimensionFieldSpec;
import com.linkedin.pinot.common.data.FieldSpec;
import com.linkedin.pinot.common.data.Schema;
import com.linkedin.pinot.common.segment.ReadMode;
import com.linkedin.pinot.core.common.BlockValSet;
import com.linkedin.pinot.core.common.DataSource;
import com.linkedin.pinot.core.data.GenericRow;
import com.linkedin.pinot.core.data.readers.GenericRowRecordReader;
import com.linkedin.pinot.core.data.readers.RecordReader;
import com.linkedin.pinot.core.indexsegment.generator.SegmentGeneratorConfig;
import com.linkedin.pinot.core.indexsegment.immutable.ImmutableSegment;
import com.linkedin.pinot.core.indexsegment.immutable.ImmutableSegmentLoader;
import com.linkedin.pinot.core.operator.DocIdSetOperator;
import com.linkedin.pinot.core.operator.ProjectionOperator;
import com.linkedin.pinot.core.operator.blocks.ProjectionBlock;
import com.linkedin.pinot.core.operator.filter.MatchEntireSegmentOperator;
import com.linkedin.pinot.core.plan.DocIdSetPlanNode;
import com.linkedin.pinot.core.query.aggregation.AggregationResultHolder;
import com.linkedin.pinot.core.query.aggregation.ObjectAggregationResultHolder;
import com.linkedin.pinot.core.query.aggregation.function.PercentileEstAggregationFunction;
import com.linkedin.pinot.core.segment.creator.impl.SegmentIndexCreationDriverImpl;
import com.linkedin.pinot.core.segment.store.SegmentDirectory;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import org.apache.commons.io.FileUtils;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


/**
 * Unit test for {@link PercentileEstAggregationFunction}.
 */
public class PercentileEstAggregationFunctionTest {
  private static final int NUM_ROWS = 1001;

  private static final String SEGMENT_DIR_NAME =
      System.getProperty("java.io.tmpdir") + File.separator + "tDigestSegTest";
  private static final String SEGMENT_NAME = "testSegment";

  private static final String TDIGEST_COLUMN = "tDigestColumn";
  private static final int[] PERCENTILES_TO_COMPUTE = new int[]{10, 20, 30, 40, 50, 60, 70, 80, 90, 95, 99};
  private static final long RANDOM_SEED = System.nanoTime();

  private Random _random;
  private RecordReader _recordReader;
  private ImmutableSegment _segment;
  private TDigest _expected;

  /**
   * Setup to build a segment with raw indexes (no-dictionary) of various data types.
   *
   * @throws Exception
   */
  @BeforeClass
  public void setup()
      throws Exception {

    Schema schema = new Schema();
    schema.addField(new DimensionFieldSpec(TDIGEST_COLUMN, FieldSpec.DataType.BYTES, true));

    _random = new Random(RANDOM_SEED);
    _recordReader = buildIndex(schema);
    _segment = ImmutableSegmentLoader.load(new File(SEGMENT_DIR_NAME, SEGMENT_NAME), ReadMode.heap);
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
   * Unit test for {@link PercentileEstAggregationFunction}:
   * <ul>
   *   <li> Generates a segment with tDigest byte[] for randomly generated doubles. </li>
   *   <li> Computes various percentiles of the raw data directly using TDigest, and asserts that the
   *        percentiles returned by the aggregation function via the generated segment match. </li>
   * </ul>
   * @throws Exception
   */
  @Test
  public void test()
      throws Exception {
    Map<String, DataSource> dataSourceMap = new HashMap<>();
    dataSourceMap.put(TDIGEST_COLUMN, _segment.getDataSource(TDIGEST_COLUMN));

    MatchEntireSegmentOperator matchEntireSegmentOperator =
        new MatchEntireSegmentOperator(_segment.getSegmentMetadata().getTotalRawDocs());
    DocIdSetOperator docIdSetOperator =
        new DocIdSetOperator(matchEntireSegmentOperator, DocIdSetPlanNode.MAX_DOC_PER_CALL);
    ProjectionOperator projectionOperator = new ProjectionOperator(dataSourceMap, docIdSetOperator);

    ProjectionBlock projectionBlock = projectionOperator.nextBlock();

    for (int percentile : PERCENTILES_TO_COMPUTE) {
      PercentileEstAggregationFunction aggregationFunction = new PercentileEstAggregationFunction(percentile);
      BlockValSet blockValueSet = projectionBlock.getBlockValueSet(TDIGEST_COLUMN);

      AggregationResultHolder resultHolder = new ObjectAggregationResultHolder();
      aggregationFunction.aggregate(NUM_ROWS, resultHolder, blockValueSet);
      TDigest tDigest = aggregationFunction.extractAggregationResult(resultHolder);

      double actual = aggregationFunction.extractFinalResult(tDigest);
      double expected = _expected.quantile(((double) percentile) / 100.0);

      Assert.assertEquals(actual, expected, 1e-5,
          "Failed for percentile: " + percentile + ". Random seed: " + RANDOM_SEED);
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

    List<GenericRow> rows = new ArrayList<>(NUM_ROWS);
    _expected = new TDigest(PercentileEstAggregationFunction.DEFAULT_TDIGEST_COMPRESSION);

    for (int i = 0; i < NUM_ROWS; i++) {
      HashMap<String, Object> map = new HashMap<>();

      // Set the value for fixed-byte sorted column.
      double value = _random.nextDouble();
      TDigest tDigest = new TDigest(PercentileEstAggregationFunction.DEFAULT_TDIGEST_COMPRESSION);
      tDigest.add(value);
      _expected.add(tDigest);

      // TDigest's with just one element should be much smaller than 1000 bytes.
      ByteBuffer byteBuffer = ByteBuffer.allocate(tDigest.byteSize());

      tDigest.asBytes(byteBuffer);
      byte[] bytes = byteBuffer.array();
      map.put(TDIGEST_COLUMN, bytes);

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

    TDigest actual = new TDigest(PercentileEstAggregationFunction.DEFAULT_TDIGEST_COMPRESSION);
    GenericRow reuse = new GenericRow();
    while (recordReader.hasNext()) {
      reuse = recordReader.next(reuse);
      actual.add(TDigest.fromBytes(ByteBuffer.wrap((byte[]) reuse.getValue(TDIGEST_COLUMN))));
    }
    return recordReader;
  }
}
