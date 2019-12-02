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
package org.apache.pinot.core.startree.v2;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.common.function.AggregationFunctionType;
import org.apache.pinot.common.request.AggregationInfo;
import org.apache.pinot.common.request.BrokerRequest;
import org.apache.pinot.common.request.GroupBy;
import org.apache.pinot.common.request.transform.TransformExpressionTree;
import org.apache.pinot.common.segment.ReadMode;
import org.apache.pinot.common.utils.request.FilterQueryTree;
import org.apache.pinot.common.utils.request.RequestUtils;
import org.apache.pinot.core.common.BlockDocIdIterator;
import org.apache.pinot.core.common.BlockSingleValIterator;
import org.apache.pinot.core.common.Constants;
import org.apache.pinot.core.common.DataSource;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.core.data.aggregator.ValueAggregator;
import org.apache.pinot.core.data.readers.GenericRowRecordReader;
import org.apache.pinot.core.indexsegment.IndexSegment;
import org.apache.pinot.core.indexsegment.generator.SegmentGeneratorConfig;
import org.apache.pinot.core.indexsegment.immutable.ImmutableSegmentLoader;
import org.apache.pinot.core.plan.FilterPlanNode;
import org.apache.pinot.core.plan.PlanNode;
import org.apache.pinot.core.query.aggregation.function.AggregationFunctionUtils;
import org.apache.pinot.core.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.core.segment.index.readers.Dictionary;
import org.apache.pinot.core.startree.plan.StarTreeFilterPlanNode;
import org.apache.pinot.core.startree.v2.builder.MultipleTreesBuilder;
import org.apache.pinot.core.startree.v2.builder.MultipleTreesBuilder.BuildMode;
import org.apache.pinot.core.startree.v2.builder.StarTreeV2BuilderConfig;
import org.apache.pinot.pql.parsers.Pql2Compiler;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;


/**
 * The {@code BaseStarTreeV2Test} is the base class to test star-tree index by scanning and aggregating records filtered
 * out from the {@link StarTreeFilterPlanNode} against normal {@link FilterPlanNode}.
 *
 * @param <R> Type or raw value
 * @param <A> Type of aggregated value
 */
abstract class BaseStarTreeV2Test<R, A> {
  private static final Pql2Compiler COMPILER = new Pql2Compiler();
  private static final Random RANDOM = new Random();

  private static final File TEMP_DIR = new File(FileUtils.getTempDirectory(), "BaseStarTreeV2Test");
  private static final String TABLE_NAME = "testTable";
  private static final String SEGMENT_NAME = "testSegment";

  private static final int NUM_SEGMENT_RECORDS = 100_000;
  private static final int MAX_LEAF_RECORDS = RANDOM.nextInt(100) + 1;
  private static final String DIMENSION_D1 = "d1";
  private static final String DIMENSION_D2 = "d2";
  private static final int DIMENSION_CARDINALITY = 100;
  private static final String METRIC = "m";
  private static final String QUERY_FILTER = " WHERE d1 = 0 AND d2 < 10";
  private static final String QUERY_GROUP_BY = " GROUP BY d2";

  private ValueAggregator _valueAggregator;
  private DataType _aggregatedValueType;
  private IndexSegment _indexSegment;
  private StarTreeV2 _starTreeV2;

  @BeforeClass
  public void setUp()
      throws Exception {
    _valueAggregator = getValueAggregator();
    _aggregatedValueType = _valueAggregator.getAggregatedValueType();

    Schema.SchemaBuilder schemaBuilder = new Schema.SchemaBuilder().addSingleValueDimension(DIMENSION_D1, DataType.INT)
        .addSingleValueDimension(DIMENSION_D2, DataType.INT);
    DataType rawValueType = getRawValueType();
    // Raw value type will be null for COUNT aggregation function
    if (rawValueType != null) {
      schemaBuilder.addMetric(METRIC, rawValueType);
    }
    Schema schema = schemaBuilder.build();

    List<GenericRow> segmentRecords = new ArrayList<>(NUM_SEGMENT_RECORDS);
    for (int i = 0; i < NUM_SEGMENT_RECORDS; i++) {
      Map<String, Object> fieldMap = new HashMap<>();
      fieldMap.put(DIMENSION_D1, RANDOM.nextInt(DIMENSION_CARDINALITY));
      fieldMap.put(DIMENSION_D2, RANDOM.nextInt(DIMENSION_CARDINALITY));
      if (rawValueType != null) {
        fieldMap.put(METRIC, getRandomRawValue(RANDOM));
      }
      GenericRow segmentRecord = new GenericRow();
      segmentRecord.init(fieldMap);
      segmentRecords.add(segmentRecord);
    }

    SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
    SegmentGeneratorConfig segmentGeneratorConfig = new SegmentGeneratorConfig(schema);
    segmentGeneratorConfig.setOutDir(TEMP_DIR.getPath());
    segmentGeneratorConfig.setSegmentName(SEGMENT_NAME);
    driver.init(segmentGeneratorConfig, new GenericRowRecordReader(segmentRecords, schema));
    driver.build();

    StarTreeV2BuilderConfig starTreeV2BuilderConfig =
        new StarTreeV2BuilderConfig.Builder().setDimensionsSplitOrder(Arrays.asList(DIMENSION_D1, DIMENSION_D2))
            .setFunctionColumnPairs(
                Collections.singleton(new AggregationFunctionColumnPair(_valueAggregator.getAggregationType(), METRIC)))
            .setMaxLeafRecords(MAX_LEAF_RECORDS).build();
    File indexDir = new File(TEMP_DIR, SEGMENT_NAME);

    // Randomly build star-tree using on-heap or off-heap mode
    BuildMode buildMode = RANDOM.nextBoolean() ? BuildMode.ON_HEAP : BuildMode.OFF_HEAP;
    new MultipleTreesBuilder(Collections.singletonList(starTreeV2BuilderConfig), indexDir, buildMode).build();

    _indexSegment = ImmutableSegmentLoader.load(indexDir, ReadMode.mmap);
    _starTreeV2 = _indexSegment.getStarTrees().get(0);
  }

  @Test
  public void testQueries() {
    AggregationFunctionType aggregationType = _valueAggregator.getAggregationType();
    String aggregation;
    if (aggregationType == AggregationFunctionType.COUNT) {
      aggregation = "COUNT(*)";
    } else if (aggregationType == AggregationFunctionType.PERCENTILEEST
        || aggregationType == AggregationFunctionType.PERCENTILETDIGEST) {
      // Append a percentile number for percentile functions
      aggregation = String.format("%s50(%s)", aggregationType.getName(), METRIC);
    } else {
      aggregation = String.format("%s(%s)", aggregationType.getName(), METRIC);
    }

    String baseQuery = String.format("SELECT %s FROM %s", aggregation, TABLE_NAME);
    testQuery(baseQuery);
    testQuery(baseQuery + QUERY_FILTER);
    testQuery(baseQuery + QUERY_GROUP_BY);
    testQuery(baseQuery + QUERY_FILTER + QUERY_GROUP_BY);
  }

  @AfterClass
  public void tearDown()
      throws IOException {
    _indexSegment.destroy();
    FileUtils.deleteDirectory(TEMP_DIR);
  }

  @SuppressWarnings("unchecked")
  void testQuery(String query) {
    BrokerRequest brokerRequest = COMPILER.compileToBrokerRequest(query);

    // Aggregations
    List<AggregationInfo> aggregationInfos = brokerRequest.getAggregationsInfo();
    int numAggregations = aggregationInfos.size();
    List<AggregationFunctionColumnPair> functionColumnPairs = new ArrayList<>(numAggregations);
    for (AggregationInfo aggregationInfo : aggregationInfos) {
      functionColumnPairs.add(AggregationFunctionUtils.getFunctionColumnPair(aggregationInfo));
    }

    // Group-by columns
    Set<String> groupByColumnSet = new HashSet<>();
    GroupBy groupBy = brokerRequest.getGroupBy();
    if (groupBy != null) {
      for (String expression : groupBy.getExpressions()) {
        TransformExpressionTree.compileToExpressionTree(expression).getColumns(groupByColumnSet);
      }
    }
    int numGroupByColumns = groupByColumnSet.size();
    List<String> groupByColumns = new ArrayList<>(groupByColumnSet);

    // Filters
    FilterQueryTree rootFilterNode = RequestUtils.generateFilterQueryTree(brokerRequest);

    // Extract values with star-tree
    PlanNode starTreeFilterPlanNode;
    if (groupByColumns.isEmpty()) {
      starTreeFilterPlanNode = new StarTreeFilterPlanNode(_starTreeV2, rootFilterNode, null, null);
    } else {
      starTreeFilterPlanNode = new StarTreeFilterPlanNode(_starTreeV2, rootFilterNode, groupByColumnSet, null);
    }
    List<BlockSingleValIterator> starTreeAggregationColumnValueIterators = new ArrayList<>(numAggregations);
    for (AggregationFunctionColumnPair aggregationFunctionColumnPair : functionColumnPairs) {
      starTreeAggregationColumnValueIterators.add(
          (BlockSingleValIterator) _starTreeV2.getDataSource(aggregationFunctionColumnPair.toColumnName()).nextBlock()
              .getBlockValueSet().iterator());
    }
    List<BlockSingleValIterator> starTreeGroupByColumnValueIterators = new ArrayList<>(numGroupByColumns);
    for (String groupByColumn : groupByColumns) {
      starTreeGroupByColumnValueIterators.add(
          (BlockSingleValIterator) _starTreeV2.getDataSource(groupByColumn).nextBlock().getBlockValueSet().iterator());
    }
    Map<List<Integer>, List<Object>> starTreeResult =
        computeStarTreeResult(starTreeFilterPlanNode, starTreeAggregationColumnValueIterators,
            starTreeGroupByColumnValueIterators);

    // Extract values without star-tree
    PlanNode nonStarTreeFilterPlanNode = new FilterPlanNode(_indexSegment, brokerRequest);
    List<BlockSingleValIterator> nonStarTreeAggregationColumnValueIterators = new ArrayList<>(numAggregations);
    List<Dictionary> nonStarTreeAggregationColumnDictionaries = new ArrayList<>(numAggregations);
    for (AggregationFunctionColumnPair aggregationFunctionColumnPair : functionColumnPairs) {
      if (aggregationFunctionColumnPair.getFunctionType() == AggregationFunctionType.COUNT) {
        nonStarTreeAggregationColumnValueIterators.add(null);
        nonStarTreeAggregationColumnDictionaries.add(null);
      } else {
        DataSource dataSource = _indexSegment.getDataSource(aggregationFunctionColumnPair.getColumn());
        nonStarTreeAggregationColumnValueIterators
            .add((BlockSingleValIterator) dataSource.nextBlock().getBlockValueSet().iterator());
        nonStarTreeAggregationColumnDictionaries.add(dataSource.getDictionary());
      }
    }
    List<BlockSingleValIterator> nonStarTreeGroupByColumnValueIterators = new ArrayList<>(numGroupByColumns);
    for (String groupByColumn : groupByColumns) {
      nonStarTreeGroupByColumnValueIterators.add(
          (BlockSingleValIterator) _indexSegment.getDataSource(groupByColumn).nextBlock().getBlockValueSet()
              .iterator());
    }
    Map<List<Integer>, List<Object>> nonStarTreeResult =
        computeNonStarTreeResult(nonStarTreeFilterPlanNode, nonStarTreeAggregationColumnValueIterators,
            nonStarTreeAggregationColumnDictionaries, nonStarTreeGroupByColumnValueIterators);

    // Assert results
    assertEquals(starTreeResult.size(), nonStarTreeResult.size());
    for (Map.Entry<List<Integer>, List<Object>> entry : starTreeResult.entrySet()) {
      List<Integer> starTreeGroup = entry.getKey();
      Assert.assertTrue(nonStarTreeResult.containsKey(starTreeGroup));
      List<Object> starTreeValues = entry.getValue();
      List<Object> nonStarTreeValues = nonStarTreeResult.get(starTreeGroup);
      for (int i = 0; i < numAggregations; i++) {
        assertAggregatedValue((A) starTreeValues.get(i), (A) nonStarTreeValues.get(i));
      }
    }
  }

  @SuppressWarnings("unchecked")
  private Map<List<Integer>, List<Object>> computeStarTreeResult(PlanNode starTreeFilterPlanNode,
      List<BlockSingleValIterator> aggregationColumnValueIterators,
      List<BlockSingleValIterator> groupByColumnValueIterators) {
    Map<List<Integer>, List<Object>> result = new HashMap<>();
    int numAggregations = aggregationColumnValueIterators.size();
    int numGroupByColumns = groupByColumnValueIterators.size();
    BlockDocIdIterator docIdIterator = starTreeFilterPlanNode.run().nextBlock().getBlockDocIdSet().iterator();
    int docId;
    while ((docId = docIdIterator.next()) != Constants.EOF) {
      // Array of dictionary Ids (zero-length array for non-group-by queries)
      List<Integer> group = new ArrayList<>(numGroupByColumns);
      for (BlockSingleValIterator valueIterator : groupByColumnValueIterators) {
        valueIterator.skipTo(docId);
        group.add(valueIterator.nextIntVal());
      }
      List<Object> values = result.computeIfAbsent(group, k -> new ArrayList<>(numAggregations));
      if (values.isEmpty()) {
        for (BlockSingleValIterator valueIterator : aggregationColumnValueIterators) {
          valueIterator.skipTo(docId);
          values.add(getNextAggregatedValue(valueIterator));
        }
      } else {
        for (int i = 0; i < numAggregations; i++) {
          Object value = values.get(i);
          BlockSingleValIterator valueIterator = aggregationColumnValueIterators.get(i);
          valueIterator.skipTo(docId);
          values.set(i, _valueAggregator.applyAggregatedValue(value, getNextAggregatedValue(valueIterator)));
        }
      }
    }
    return result;
  }

  private Object getNextAggregatedValue(BlockSingleValIterator valueIterator) {
    switch (_aggregatedValueType) {
      case LONG:
        return valueIterator.nextLongVal();
      case DOUBLE:
        return valueIterator.nextDoubleVal();
      case BYTES:
        return _valueAggregator.deserializeAggregatedValue(valueIterator.nextBytesVal());
      default:
        throw new IllegalStateException();
    }
  }

  @SuppressWarnings("unchecked")
  private Map<List<Integer>, List<Object>> computeNonStarTreeResult(PlanNode nonStarTreeFilterPlanNode,
      List<BlockSingleValIterator> aggregationColumnValueIterators, List<Dictionary> aggregationColumnDictionaries,
      List<BlockSingleValIterator> groupByColumnValueIterators) {
    Map<List<Integer>, List<Object>> result = new HashMap<>();
    int numAggregations = aggregationColumnValueIterators.size();
    int numGroupByColumns = groupByColumnValueIterators.size();
    BlockDocIdIterator docIdIterator = nonStarTreeFilterPlanNode.run().nextBlock().getBlockDocIdSet().iterator();
    int docId;
    while ((docId = docIdIterator.next()) != Constants.EOF) {
      // Array of dictionary Ids (zero-length array for non-group-by queries)
      List<Integer> group = new ArrayList<>(numGroupByColumns);
      for (BlockSingleValIterator valueIterator : groupByColumnValueIterators) {
        valueIterator.skipTo(docId);
        group.add(valueIterator.nextIntVal());
      }
      List<Object> values = result.computeIfAbsent(group, k -> new ArrayList<>(numAggregations));
      if (values.isEmpty()) {
        for (int i = 0; i < numAggregations; i++) {
          BlockSingleValIterator valueIterator = aggregationColumnValueIterators.get(i);
          if (valueIterator == null) {
            // COUNT aggregation function
            values.add(1L);
          } else {
            valueIterator.skipTo(docId);
            Object rawValue = getNextRawValue(valueIterator, aggregationColumnDictionaries.get(i));
            values.add(_valueAggregator.getInitialAggregatedValue(rawValue));
          }
        }
      } else {
        for (int i = 0; i < numAggregations; i++) {
          Object value = values.get(i);
          BlockSingleValIterator valueIterator = aggregationColumnValueIterators.get(i);
          if (valueIterator == null) {
            // COUNT aggregation function
            value = (Long) value + 1;
          } else {
            valueIterator.skipTo(docId);
            Object rawValue = getNextRawValue(valueIterator, aggregationColumnDictionaries.get(i));
            value = _valueAggregator.applyRawValue(value, rawValue);
          }
          values.set(i, value);
        }
      }
    }
    return result;
  }

  private Object getNextRawValue(BlockSingleValIterator valueIterator, Dictionary dictionary) {
    int dictId = valueIterator.nextIntVal();
    return dictionary.get(dictId);
  }

  abstract ValueAggregator<R, A> getValueAggregator();

  abstract DataType getRawValueType();

  abstract R getRandomRawValue(Random random);

  abstract void assertAggregatedValue(A starTreeResult, A nonStarTreeResult);
}
