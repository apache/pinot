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
package com.linkedin.pinot.core.startree;

import com.linkedin.pinot.common.data.DimensionFieldSpec;
import com.linkedin.pinot.common.data.FieldSpec;
import com.linkedin.pinot.common.data.MetricFieldSpec;
import com.linkedin.pinot.common.data.Schema;
import com.linkedin.pinot.common.data.StarTreeIndexSpec;
import com.linkedin.pinot.common.data.TimeFieldSpec;
import com.linkedin.pinot.common.request.BrokerRequest;
import com.linkedin.pinot.common.segment.ReadMode;
import com.linkedin.pinot.common.segment.SegmentMetadata;
import com.linkedin.pinot.common.utils.request.FilterQueryTree;
import com.linkedin.pinot.common.utils.request.RequestUtils;
import com.linkedin.pinot.core.common.BlockDocIdIterator;
import com.linkedin.pinot.core.common.BlockSingleValIterator;
import com.linkedin.pinot.core.common.Constants;
import com.linkedin.pinot.core.common.DataSource;
import com.linkedin.pinot.core.common.Operator;
import com.linkedin.pinot.core.data.GenericRow;
import com.linkedin.pinot.core.data.readers.FileFormat;
import com.linkedin.pinot.core.data.readers.RecordReader;
import com.linkedin.pinot.core.indexsegment.IndexSegment;
import com.linkedin.pinot.core.indexsegment.generator.SegmentGeneratorConfig;
import com.linkedin.pinot.core.operator.filter.StarTreeIndexOperator;
import com.linkedin.pinot.core.plan.FilterPlanNode;
import com.linkedin.pinot.core.segment.creator.impl.SegmentIndexCreationDriverImpl;
import com.linkedin.pinot.core.segment.index.loader.Loaders;
import com.linkedin.pinot.core.segment.index.readers.Dictionary;
import com.linkedin.pinot.pql.parsers.Pql2Compiler;
import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang.mutable.MutableLong;
import org.apache.commons.math.util.MathUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;


/**
 * Base class containing common functionality for all star-tree integration tests.
 */
public class BaseStarTreeIndexTest {
  private static final Logger LOGGER = LoggerFactory.getLogger(BaseStarTreeIndexTest.class);

  private static final String TIME_COLUMN_NAME = "daysSinceEpoch";
  private static final int NUM_DIMENSIONS = 4;
  private static final int NUM_METRICS = 2;
  private static final int METRIC_MAX_VALUE = 10000;
  protected final long _randomSeed = System.nanoTime();


  protected String[] _hardCodedQueries =
      new String[]{
          "select sum(m1) from T",
          "select sum(m1) from T where d1 = 'd1-v1'",
          "select sum(m1) from T where d1 <> 'd1-v1'",
          "select sum(m1) from T where d1 between 'd1-v1' and 'd1-v3'",
          "select sum(m1) from T where d1 in ('d1-v1', 'd1-v2')",
          "select sum(m1) from T where d1 in ('d1-v1', 'd1-v2') and d2 not in ('d2-v1')",
          "select sum(m1) from T group by d1", "select sum(m1) from T group by d1, d2",
          "select sum(m1) from T where d1 = 'd1-v2' group by d1",
          "select sum(m1) from T where d1 between 'd1-v1' and 'd1-v3' group by d2",
          "select sum(m1) from T where d1 = 'd1-v2' group by d2, d3",
          "select sum(m1) from T where d1 <> 'd1-v1' group by d2",
          "select sum(m1) from T where d1 in ('d1-v1', 'd1-v2') group by d2",
          "select sum(m1) from T where d1 in ('d1-v1', 'd1-v2') and d2 not in ('d2-v1') group by d3",
          "select sum(m1) from T where d1 in ('d1-v1', 'd1-v2') and d2 not in ('d2-v1') group by d3, d4"};

  protected void testHardCodedQueries(IndexSegment segment, Schema schema) {
    // Test against all metric columns, instead of just the aggregation column in the query.
    List<String> metricNames = schema.getMetricNames();
    SegmentMetadata segmentMetadata = segment.getSegmentMetadata();

    for (int i = 0; i < _hardCodedQueries.length; i++) {
      Pql2Compiler compiler = new Pql2Compiler();
      BrokerRequest brokerRequest = compiler.compileToBrokerRequest(_hardCodedQueries[i]);

      FilterQueryTree filterQueryTree = RequestUtils.generateFilterQueryTree(brokerRequest);
      Assert.assertTrue(RequestUtils.isFitForStarTreeIndex(segmentMetadata, filterQueryTree, brokerRequest));

      Map<String, double[]> expectedResult = computeSumUsingRawDocs(segment, metricNames, brokerRequest);
      Map<String, double[]> actualResult = computeSumUsingAggregatedDocs(segment, metricNames, brokerRequest);

      Assert.assertEquals(expectedResult.size(), actualResult.size(), "Mis-match in number of groups");
      for (Map.Entry<String, double[]> entry : expectedResult.entrySet()) {
        String expectedKey = entry.getKey();
        Assert.assertTrue(actualResult.containsKey(expectedKey));

        double[] expectedSums = entry.getValue();
        double[] actualSums = actualResult.get(expectedKey);

        for (int j = 0; j < expectedSums.length; j++) {
          Assert.assertEquals(actualSums[j], expectedSums[j],
              "Mis-match sum for key '" + expectedKey + "', Metric: " + metricNames.get(j) + ", Random Seed: "
                  + _randomSeed);
        }
      }
    }
  }

  /**
   * Helper method to compute the sums using raw index.
   *  @param metricNames
   * @param brokerRequest
   */
  protected Map<String, double[]> computeSumUsingRawDocs(IndexSegment segment, List<String> metricNames,
      BrokerRequest brokerRequest) {
    FilterPlanNode planNode = new FilterPlanNode(segment, brokerRequest);
    Operator rawOperator = planNode.run();
    BlockDocIdIterator rawDocIdIterator = rawOperator.nextBlock().getBlockDocIdSet().iterator();

    List<String> groupByColumns = Collections.EMPTY_LIST;
    if (brokerRequest.isSetAggregationsInfo() && brokerRequest.isSetGroupBy()) {
      groupByColumns = brokerRequest.getGroupBy().getColumns();
    }
    return computeSum(segment, rawDocIdIterator, metricNames, groupByColumns);
  }

  /**
   * Helper method to compute the sum using aggregated docs.
   * @param metricNames
   * @param brokerRequest
   * @return
   */
  Map<String, double[]> computeSumUsingAggregatedDocs(IndexSegment segment, List<String> metricNames,
      BrokerRequest brokerRequest) {
    StarTreeIndexOperator starTreeOperator = new StarTreeIndexOperator(segment, brokerRequest);
    starTreeOperator.open();
    BlockDocIdIterator starTreeDocIdIterator = starTreeOperator.nextBlock().getBlockDocIdSet().iterator();

    List<String> groupByColumns = Collections.EMPTY_LIST;
    if (brokerRequest.isSetAggregationsInfo() && brokerRequest.isSetGroupBy()) {
      groupByColumns = brokerRequest.getGroupBy().getColumns();
    }

    return computeSum(segment, starTreeDocIdIterator, metricNames, groupByColumns);
  }

  /**
   * Helper method to build the segment.
   *
   * @param segmentDirName
   * @param segmentName
   * @throws Exception
   */
  Schema buildSegment(String segmentDirName, String segmentName, boolean enableOffHeapFormat)
      throws Exception {
    int ROWS = (int) MathUtils.factorial(NUM_DIMENSIONS);
    Schema schema = new Schema();

    for (int i = 0; i < NUM_DIMENSIONS; i++) {
      String dimName = "d" + (i + 1);
      DimensionFieldSpec dimensionFieldSpec = new DimensionFieldSpec(dimName, FieldSpec.DataType.STRING, true);
      schema.addField(dimName, dimensionFieldSpec);
    }

    schema.setTimeFieldSpec(new TimeFieldSpec(TIME_COLUMN_NAME, FieldSpec.DataType.INT, TimeUnit.DAYS));
    for (int i = 0; i < NUM_METRICS; i++) {
      String metricName = "m" + (i + 1);
      MetricFieldSpec metricFieldSpec = new MetricFieldSpec(metricName, FieldSpec.DataType.INT);
      schema.addField(metricName, metricFieldSpec);
    }

    SegmentGeneratorConfig config = new SegmentGeneratorConfig(schema);
    config.setEnableStarTreeIndex(true);
    config.setOutDir(segmentDirName);
    config.setFormat(FileFormat.AVRO);
    config.setSegmentName(segmentName);
    config.setStarTreeIndexSpec(buildStarTreeIndexSpec(enableOffHeapFormat));

    final List<GenericRow> data = new ArrayList<>();
    for (int row = 0; row < ROWS; row++) {
      HashMap<String, Object> map = new HashMap<>();
      for (int i = 0; i < NUM_DIMENSIONS; i++) {
        String dimName = schema.getDimensionFieldSpecs().get(i).getName();
        map.put(dimName, dimName + "-v" + row % (NUM_DIMENSIONS - i));
      }

      Random random = new Random(_randomSeed);
      for (int i = 0; i < NUM_METRICS; i++) {
        String metName = schema.getMetricFieldSpecs().get(i).getName();
        map.put(metName, random.nextInt(METRIC_MAX_VALUE));
      }

      // Time column.
      map.put("daysSinceEpoch", row % 7);

      GenericRow genericRow = new GenericRow();
      genericRow.init(map);
      data.add(genericRow);
    }

    SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
    RecordReader reader = createReader(schema, data);
    driver.init(config, reader);
    driver.build();

    LOGGER.info("Built segment {} at {}", segmentName, segmentDirName);
    return schema;
  }

  /**
   * Builds a star tree index spec for the test.
   * - Use MaxLeafRecords as 1 to stress test.
   * @return
   * @param enableOffHeapFormat
   */
  private StarTreeIndexSpec buildStarTreeIndexSpec(boolean enableOffHeapFormat) {
    StarTreeIndexSpec spec = new StarTreeIndexSpec();
    spec.setMaxLeafRecords(1);
    spec.setEnableOffHeapFormat(enableOffHeapFormat);
    return spec;
  }

  /**
   * Helper method to load the segment.
   *
   * @param segmentDirName
   * @param segmentName
   * @throws Exception
   */
  IndexSegment loadSegment(String segmentDirName, String segmentName)
      throws Exception {
    LOGGER.info("Loading segment {}", segmentName);
    return Loaders.IndexSegment.load(new File(segmentDirName, segmentName), ReadMode.heap);
  }

  /**
   * Compute 'sum' for a given list of metrics, by scanning the given set of doc-ids.
   *
   * @param segment
   * @param docIdIterator
   * @param metricNames
   * @return
   */
  private Map<String, double[]> computeSum(IndexSegment segment, BlockDocIdIterator docIdIterator,
      List<String> metricNames, List<String> groupByColumns) {
    int docId;
    int numMetrics = metricNames.size();
    Dictionary[] metricDictionaries = new Dictionary[numMetrics];
    BlockSingleValIterator[] metricValIterators = new BlockSingleValIterator[numMetrics];

    int numGroupByColumns = groupByColumns.size();
    Dictionary[] groupByDictionaries = new Dictionary[numGroupByColumns];
    BlockSingleValIterator[] groupByValIterators = new BlockSingleValIterator[numGroupByColumns];

    for (int i = 0; i < numMetrics; i++) {
      String metricName = metricNames.get(i);
      DataSource dataSource = segment.getDataSource(metricName);
      metricDictionaries[i] = dataSource.getDictionary();
      metricValIterators[i] = (BlockSingleValIterator) dataSource.getNextBlock().getBlockValueSet().iterator();
    }

    for (int i = 0; i < numGroupByColumns; i++) {
      String groupByColumn = groupByColumns.get(i);
      DataSource dataSource = segment.getDataSource(groupByColumn);
      groupByDictionaries[i] = dataSource.getDictionary();
      groupByValIterators[i] = (BlockSingleValIterator) dataSource.getNextBlock().getBlockValueSet().iterator();
    }

    Map<String, double[]> result = new HashMap<String, double[]>();
    while ((docId = docIdIterator.next()) != Constants.EOF) {

      StringBuilder stringBuilder = new StringBuilder();
      for (int i = 0; i < numGroupByColumns; i++) {
        groupByValIterators[i].skipTo(docId);
        int dictId = groupByValIterators[i].nextIntVal();
        stringBuilder.append(groupByDictionaries[i].getStringValue(dictId));
        stringBuilder.append("_");
      }

      String key = stringBuilder.toString();
      if (!result.containsKey(key)) {
        result.put(key, new double[numMetrics]);
      }

      double[] sumsSoFar = result.get(key);
      for (int i = 0; i < numMetrics; i++) {
        metricValIterators[i].skipTo(docId);
        int dictId = metricValIterators[i].nextIntVal();
        sumsSoFar[i] += metricDictionaries[i].getDoubleValue(dictId);
      }
    }

    return result;
  }

  private RecordReader createReader(final Schema schema, final List<GenericRow> data) {
    return new RecordReader() {

      int counter = 0;

      @Override
      public void rewind()
          throws Exception {
        counter = 0;
      }

      @Override
      public GenericRow next() {
        return data.get(counter++);
      }

      @Override
      public void init()
          throws Exception {

      }

      @Override
      public boolean hasNext() {
        return counter < data.size();
      }

      @Override
      public Schema getSchema() {
        return schema;
      }

      @Override
      public Map<String, MutableLong> getNullCountMap() {
        return null;
      }

      @Override
      public void close()
          throws Exception {

      }
    };
  }
}
