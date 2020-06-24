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
import org.apache.pinot.common.function.AggregationFunctionType;
import org.apache.pinot.common.request.BrokerRequest;
import org.apache.pinot.common.request.GroupBy;
import org.apache.pinot.common.request.transform.TransformExpressionTree;
import org.apache.pinot.common.segment.ReadMode;
import org.apache.pinot.core.common.BlockDocIdIterator;
import org.apache.pinot.core.common.Constants;
import org.apache.pinot.core.common.DataSource;
import org.apache.pinot.core.data.aggregator.ValueAggregator;
import org.apache.pinot.core.data.readers.GenericRowRecordReader;
import org.apache.pinot.core.indexsegment.IndexSegment;
import org.apache.pinot.core.indexsegment.generator.SegmentGeneratorConfig;
import org.apache.pinot.core.indexsegment.immutable.ImmutableSegmentLoader;
import org.apache.pinot.core.operator.docvalsets.SingleValueSet;
import org.apache.pinot.core.plan.FilterPlanNode;
import org.apache.pinot.core.plan.PlanNode;
import org.apache.pinot.core.query.aggregation.function.AggregationFunction;
import org.apache.pinot.core.query.aggregation.function.AggregationFunctionUtils;
import org.apache.pinot.core.query.request.context.FilterContext;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.core.query.request.context.utils.BrokerRequestToQueryContextConverter;
import org.apache.pinot.core.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.core.segment.index.readers.Dictionary;
import org.apache.pinot.core.startree.plan.StarTreeFilterPlanNode;
import org.apache.pinot.core.startree.v2.builder.MultipleTreesBuilder;
import org.apache.pinot.core.startree.v2.builder.MultipleTreesBuilder.BuildMode;
import org.apache.pinot.core.startree.v2.builder.StarTreeV2BuilderConfig;
import org.apache.pinot.pql.parsers.Pql2Compiler;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;


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
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME).build();

    List<GenericRow> segmentRecords = new ArrayList<>(NUM_SEGMENT_RECORDS);
    for (int i = 0; i < NUM_SEGMENT_RECORDS; i++) {
      GenericRow segmentRecord = new GenericRow();
      segmentRecord.putValue(DIMENSION_D1, RANDOM.nextInt(DIMENSION_CARDINALITY));
      segmentRecord.putValue(DIMENSION_D2, RANDOM.nextInt(DIMENSION_CARDINALITY));
      if (rawValueType != null) {
        segmentRecord.putValue(METRIC, getRandomRawValue(RANDOM));
      }
      segmentRecords.add(segmentRecord);
    }

    SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
    SegmentGeneratorConfig segmentGeneratorConfig = new SegmentGeneratorConfig(tableConfig, schema);
    segmentGeneratorConfig.setOutDir(TEMP_DIR.getPath());
    segmentGeneratorConfig.setSegmentName(SEGMENT_NAME);
    driver.init(segmentGeneratorConfig, new GenericRowRecordReader(segmentRecords));
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
    QueryContext queryContext = BrokerRequestToQueryContextConverter.convert(brokerRequest);

    // Aggregations
    AggregationFunction[] aggregationFunctions = AggregationFunctionUtils.getAggregationFunctions(brokerRequest);
    int numAggregations = aggregationFunctions.length;
    List<AggregationFunctionColumnPair> functionColumnPairs = new ArrayList<>(numAggregations);
    for (AggregationFunction aggregationFunction : aggregationFunctions) {
      AggregationFunctionColumnPair aggregationFunctionColumnPair =
          AggregationFunctionUtils.getAggregationFunctionColumnPair(aggregationFunction);
      assertNotNull(aggregationFunctionColumnPair);
      functionColumnPairs.add(aggregationFunctionColumnPair);
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

    // Filter
    FilterContext filter = queryContext.getFilter();

    // Extract values with star-tree
    PlanNode starTreeFilterPlanNode;
    if (groupByColumns.isEmpty()) {
      starTreeFilterPlanNode = new StarTreeFilterPlanNode(_starTreeV2, filter, null, null);
    } else {
      starTreeFilterPlanNode = new StarTreeFilterPlanNode(_starTreeV2, filter, groupByColumnSet, null);
    }
    List<SingleValueSet> starTreeAggregationColumnValueSets = new ArrayList<>(numAggregations);
    for (AggregationFunctionColumnPair aggregationFunctionColumnPair : functionColumnPairs) {
      starTreeAggregationColumnValueSets.add(
          (SingleValueSet) _starTreeV2.getDataSource(aggregationFunctionColumnPair.toColumnName()).nextBlock()
              .getBlockValueSet());
    }
    List<SingleValueSet> starTreeGroupByColumnValueSets = new ArrayList<>(numGroupByColumns);
    for (String groupByColumn : groupByColumns) {
      starTreeGroupByColumnValueSets
          .add((SingleValueSet) _starTreeV2.getDataSource(groupByColumn).nextBlock().getBlockValueSet());
    }
    Map<List<Integer>, List<Object>> starTreeResult =
        computeStarTreeResult(starTreeFilterPlanNode, starTreeAggregationColumnValueSets,
            starTreeGroupByColumnValueSets);

    // Extract values without star-tree
    PlanNode nonStarTreeFilterPlanNode = new FilterPlanNode(_indexSegment, queryContext);
    List<SingleValueSet> nonStarTreeAggregationColumnValueSets = new ArrayList<>(numAggregations);
    List<Dictionary> nonStarTreeAggregationColumnDictionaries = new ArrayList<>(numAggregations);
    for (AggregationFunctionColumnPair aggregationFunctionColumnPair : functionColumnPairs) {
      if (aggregationFunctionColumnPair.getFunctionType() == AggregationFunctionType.COUNT) {
        nonStarTreeAggregationColumnValueSets.add(null);
        nonStarTreeAggregationColumnDictionaries.add(null);
      } else {
        DataSource dataSource = _indexSegment.getDataSource(aggregationFunctionColumnPair.getColumn());
        nonStarTreeAggregationColumnValueSets.add((SingleValueSet) dataSource.nextBlock().getBlockValueSet());
        nonStarTreeAggregationColumnDictionaries.add(dataSource.getDictionary());
      }
    }
    List<SingleValueSet> nonStarTreeGroupByColumnValueSets = new ArrayList<>(numGroupByColumns);
    for (String groupByColumn : groupByColumns) {
      nonStarTreeGroupByColumnValueSets
          .add((SingleValueSet) _indexSegment.getDataSource(groupByColumn).nextBlock().getBlockValueSet());
    }
    Map<List<Integer>, List<Object>> nonStarTreeResult =
        computeNonStarTreeResult(nonStarTreeFilterPlanNode, nonStarTreeAggregationColumnValueSets,
            nonStarTreeAggregationColumnDictionaries, nonStarTreeGroupByColumnValueSets);

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
      List<SingleValueSet> aggregationColumnValueSets, List<SingleValueSet> groupByColumnValueSets) {
    Map<List<Integer>, List<Object>> result = new HashMap<>();
    int numAggregations = aggregationColumnValueSets.size();
    int numGroupByColumns = groupByColumnValueSets.size();
    BlockDocIdIterator docIdIterator = starTreeFilterPlanNode.run().nextBlock().getBlockDocIdSet().iterator();
    int docId;
    while ((docId = docIdIterator.next()) != Constants.EOF) {
      // Array of dictionary Ids (zero-length array for non-group-by queries)
      List<Integer> group = new ArrayList<>(numGroupByColumns);
      for (SingleValueSet valueSet : groupByColumnValueSets) {
        group.add(valueSet.getIntValue(docId));
      }
      List<Object> values = result.computeIfAbsent(group, k -> new ArrayList<>(numAggregations));
      if (values.isEmpty()) {
        for (SingleValueSet valueSet : aggregationColumnValueSets) {
          values.add(getAggregatedValue(valueSet, docId));
        }
      } else {
        for (int i = 0; i < numAggregations; i++) {
          Object value = values.get(i);
          SingleValueSet valueSet = aggregationColumnValueSets.get(i);
          values.set(i, _valueAggregator.applyAggregatedValue(value, getAggregatedValue(valueSet, docId)));
        }
      }
    }
    return result;
  }

  private Object getAggregatedValue(SingleValueSet valueSet, int docId) {
    switch (_aggregatedValueType) {
      case LONG:
        return valueSet.getLongValue(docId);
      case DOUBLE:
        return valueSet.getDoubleValue(docId);
      case BYTES:
        return _valueAggregator.deserializeAggregatedValue(valueSet.getBytesValue(docId));
      default:
        throw new IllegalStateException();
    }
  }

  @SuppressWarnings("unchecked")
  private Map<List<Integer>, List<Object>> computeNonStarTreeResult(PlanNode nonStarTreeFilterPlanNode,
      List<SingleValueSet> aggregationColumnValueSets, List<Dictionary> aggregationColumnDictionaries,
      List<SingleValueSet> groupByColumnValueSets) {
    Map<List<Integer>, List<Object>> result = new HashMap<>();
    int numAggregations = aggregationColumnValueSets.size();
    int numGroupByColumns = groupByColumnValueSets.size();
    BlockDocIdIterator docIdIterator = nonStarTreeFilterPlanNode.run().nextBlock().getBlockDocIdSet().iterator();
    int docId;
    while ((docId = docIdIterator.next()) != Constants.EOF) {
      // Array of dictionary Ids (zero-length array for non-group-by queries)
      List<Integer> group = new ArrayList<>(numGroupByColumns);
      for (SingleValueSet valueSet : groupByColumnValueSets) {
        group.add(valueSet.getIntValue(docId));
      }
      List<Object> values = result.computeIfAbsent(group, k -> new ArrayList<>(numAggregations));
      if (values.isEmpty()) {
        for (int i = 0; i < numAggregations; i++) {
          SingleValueSet valueSet = aggregationColumnValueSets.get(i);
          if (valueSet == null) {
            // COUNT aggregation function
            values.add(1L);
          } else {
            Object rawValue = getNextRawValue(valueSet, docId, aggregationColumnDictionaries.get(i));
            values.add(_valueAggregator.getInitialAggregatedValue(rawValue));
          }
        }
      } else {
        for (int i = 0; i < numAggregations; i++) {
          Object value = values.get(i);
          SingleValueSet valueSet = aggregationColumnValueSets.get(i);
          if (valueSet == null) {
            // COUNT aggregation function
            value = (Long) value + 1;
          } else {
            Object rawValue = getNextRawValue(valueSet, docId, aggregationColumnDictionaries.get(i));
            value = _valueAggregator.applyRawValue(value, rawValue);
          }
          values.set(i, value);
        }
      }
    }
    return result;
  }

  private Object getNextRawValue(SingleValueSet valueSet, int docId, Dictionary dictionary) {
    return dictionary.get(valueSet.getIntValue(docId));
  }

  abstract ValueAggregator<R, A> getValueAggregator();

  abstract DataType getRawValueType();

  abstract R getRandomRawValue(Random random);

  abstract void assertAggregatedValue(A starTreeResult, A nonStarTreeResult);
}
