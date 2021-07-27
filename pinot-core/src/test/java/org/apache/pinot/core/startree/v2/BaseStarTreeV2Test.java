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
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.request.context.FilterContext;
import org.apache.pinot.core.common.BlockDocIdIterator;
import org.apache.pinot.core.plan.FilterPlanNode;
import org.apache.pinot.core.plan.PlanNode;
import org.apache.pinot.core.query.aggregation.function.AggregationFunction;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.core.query.request.context.utils.QueryContextConverterUtils;
import org.apache.pinot.core.startree.CompositePredicateEvaluator;
import org.apache.pinot.core.startree.StarTreeUtils;
import org.apache.pinot.core.startree.plan.StarTreeFilterPlanNode;
import org.apache.pinot.segment.local.aggregator.ValueAggregator;
import org.apache.pinot.segment.local.indexsegment.immutable.ImmutableSegmentLoader;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.segment.local.segment.readers.GenericRowRecordReader;
import org.apache.pinot.segment.local.startree.v2.builder.MultipleTreesBuilder;
import org.apache.pinot.segment.spi.AggregationFunctionType;
import org.apache.pinot.segment.spi.Constants;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
import org.apache.pinot.segment.spi.datasource.DataSource;
import org.apache.pinot.segment.spi.index.reader.Dictionary;
import org.apache.pinot.segment.spi.index.reader.ForwardIndexReader;
import org.apache.pinot.segment.spi.index.reader.ForwardIndexReaderContext;
import org.apache.pinot.segment.spi.index.startree.AggregationFunctionColumnPair;
import org.apache.pinot.segment.spi.index.startree.StarTreeV2;
import org.apache.pinot.spi.config.table.StarTreeIndexConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.utils.ReadMode;
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
@SuppressWarnings({"rawtypes", "unchecked"})
abstract class BaseStarTreeV2Test<R, A> {
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
  private static final String QUERY_FILTER_AND = " WHERE d1 = 0 AND d2 < 10";
  // StarTree supports OR predicates only on a single dimension
  private static final String QUERY_FILTER_OR = " WHERE d1 > 10 OR d1 < 50";
  private static final String QUERY_FILTER_COMPLEX_OR_MULTIPLE_DIMENSIONS = " WHERE d2 < 95 AND (d1 > 10 OR d1 < 50)";
  private static final String QUERY_FILTER_COMPLEX_AND_MULTIPLE_DIMENSIONS_THREE_PREDICATES =
      " WHERE d2 < 95 AND d2 > 25 AND (d1 > 10 OR d1 < 50)";
  private static final String QUERY_FILTER_COMPLEX_OR_MULTIPLE_DIMENSIONS_THREE_PREDICATES =
      " WHERE (d2 > 95 OR d2 < 25) AND (d1 > 10 OR d1 < 50)";
  private static final String QUERY_FILTER_COMPLEX_OR_SINGLE_DIMENSION = " WHERE d1 = 95 AND (d1 > 90 OR d1 < 100)";
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

    StarTreeIndexConfig starTreeIndexConfig = new StarTreeIndexConfig(Arrays.asList(DIMENSION_D1, DIMENSION_D2), null,
        Collections.singletonList(
            new AggregationFunctionColumnPair(_valueAggregator.getAggregationType(), METRIC).toColumnName()),
        MAX_LEAF_RECORDS);
    File indexDir = new File(TEMP_DIR, SEGMENT_NAME);
    // Randomly build star-tree using on-heap or off-heap mode
    MultipleTreesBuilder.BuildMode buildMode =
        RANDOM.nextBoolean() ? MultipleTreesBuilder.BuildMode.ON_HEAP : MultipleTreesBuilder.BuildMode.OFF_HEAP;
    try (MultipleTreesBuilder builder = new MultipleTreesBuilder(Collections.singletonList(starTreeIndexConfig), false,
        indexDir, buildMode)) {
      builder.build();
    }

    _indexSegment = ImmutableSegmentLoader.load(indexDir, ReadMode.mmap);
    _starTreeV2 = _indexSegment.getStarTrees().get(0);
  }

  @Test
  public void testQueries()
      throws IOException {
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
    testQuery(baseQuery + QUERY_FILTER_AND);
    testQuery(baseQuery + QUERY_FILTER_OR);
    testQuery(baseQuery + QUERY_FILTER_COMPLEX_OR_MULTIPLE_DIMENSIONS);
    testQuery(baseQuery + QUERY_FILTER_COMPLEX_AND_MULTIPLE_DIMENSIONS_THREE_PREDICATES);
    testQuery(baseQuery + QUERY_FILTER_COMPLEX_OR_MULTIPLE_DIMENSIONS_THREE_PREDICATES);
    testQuery(baseQuery + QUERY_FILTER_COMPLEX_OR_SINGLE_DIMENSION);
    testQuery(baseQuery + QUERY_GROUP_BY);
    testQuery(baseQuery + QUERY_FILTER_AND + QUERY_GROUP_BY);
    testQuery(baseQuery + QUERY_FILTER_OR + QUERY_GROUP_BY);
    testQuery(baseQuery + QUERY_FILTER_COMPLEX_OR_MULTIPLE_DIMENSIONS + QUERY_GROUP_BY);
    testQuery(baseQuery + QUERY_FILTER_COMPLEX_OR_MULTIPLE_DIMENSIONS_THREE_PREDICATES + QUERY_GROUP_BY);
    testQuery(baseQuery + QUERY_FILTER_COMPLEX_AND_MULTIPLE_DIMENSIONS_THREE_PREDICATES + QUERY_GROUP_BY);
    testQuery(baseQuery + QUERY_FILTER_COMPLEX_OR_SINGLE_DIMENSION + QUERY_GROUP_BY);
  }

  @AfterClass
  public void tearDown()
      throws IOException {
    _indexSegment.destroy();
    FileUtils.deleteDirectory(TEMP_DIR);
  }

  void testQuery(String query)
      throws IOException {
    QueryContext queryContext = QueryContextConverterUtils.getQueryContextFromSQL(query);

    // Aggregations
    AggregationFunction[] aggregationFunctions = queryContext.getAggregationFunctions();
    assertNotNull(aggregationFunctions);
    int numAggregations = aggregationFunctions.length;
    AggregationFunctionColumnPair[] aggregationFunctionColumnPairs =
        StarTreeUtils.extractAggregationFunctionPairs(aggregationFunctions);
    assertNotNull(aggregationFunctionColumnPairs);

    // Group-by columns
    Set<String> groupByColumnSet = new HashSet<>();
    List<ExpressionContext> groupByExpressions = queryContext.getGroupByExpressions();
    if (groupByExpressions != null) {
      for (ExpressionContext groupByExpression : groupByExpressions) {
        groupByExpression.getColumns(groupByColumnSet);
      }
    }
    int numGroupByColumns = groupByColumnSet.size();
    List<String> groupByColumns = new ArrayList<>(groupByColumnSet);

    // Filter
    FilterContext filter = queryContext.getFilter();
    Map<String, List<CompositePredicateEvaluator>> predicateEvaluatorsMap =
        StarTreeUtils.extractPredicateEvaluatorsMap(_indexSegment, filter);
    assertNotNull(predicateEvaluatorsMap);

    // Extract values with star-tree
    PlanNode starTreeFilterPlanNode =
        new StarTreeFilterPlanNode(_starTreeV2, predicateEvaluatorsMap, groupByColumnSet, null);
    List<ForwardIndexReader> starTreeAggregationColumnReaders = new ArrayList<>(numAggregations);
    for (AggregationFunctionColumnPair aggregationFunctionColumnPair : aggregationFunctionColumnPairs) {
      starTreeAggregationColumnReaders
          .add(_starTreeV2.getDataSource(aggregationFunctionColumnPair.toColumnName()).getForwardIndex());
    }
    List<ForwardIndexReader> starTreeGroupByColumnReaders = new ArrayList<>(numGroupByColumns);
    for (String groupByColumn : groupByColumns) {
      starTreeGroupByColumnReaders.add(_starTreeV2.getDataSource(groupByColumn).getForwardIndex());
    }
    Map<List<Integer>, List<Object>> starTreeResult =
        computeStarTreeResult(starTreeFilterPlanNode, starTreeAggregationColumnReaders, starTreeGroupByColumnReaders);

    // Extract values without star-tree
    PlanNode nonStarTreeFilterPlanNode = new FilterPlanNode(_indexSegment, queryContext);
    List<ForwardIndexReader> nonStarTreeAggregationColumnReaders = new ArrayList<>(numAggregations);
    List<Dictionary> nonStarTreeAggregationColumnDictionaries = new ArrayList<>(numAggregations);
    for (AggregationFunctionColumnPair aggregationFunctionColumnPair : aggregationFunctionColumnPairs) {
      if (aggregationFunctionColumnPair.getFunctionType() == AggregationFunctionType.COUNT) {
        nonStarTreeAggregationColumnReaders.add(null);
        nonStarTreeAggregationColumnDictionaries.add(null);
      } else {
        DataSource dataSource = _indexSegment.getDataSource(aggregationFunctionColumnPair.getColumn());
        nonStarTreeAggregationColumnReaders.add(dataSource.getForwardIndex());
        nonStarTreeAggregationColumnDictionaries.add(dataSource.getDictionary());
      }
    }
    List<ForwardIndexReader> nonStarTreeGroupByColumnReaders = new ArrayList<>(numGroupByColumns);
    for (String groupByColumn : groupByColumns) {
      nonStarTreeGroupByColumnReaders.add(_indexSegment.getDataSource(groupByColumn).getForwardIndex());
    }
    Map<List<Integer>, List<Object>> nonStarTreeResult =
        computeNonStarTreeResult(nonStarTreeFilterPlanNode, nonStarTreeAggregationColumnReaders,
            nonStarTreeAggregationColumnDictionaries, nonStarTreeGroupByColumnReaders);

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

  private Map<List<Integer>, List<Object>> computeStarTreeResult(PlanNode starTreeFilterPlanNode,
      List<ForwardIndexReader> aggregationColumnReaders, List<ForwardIndexReader> groupByColumnReaders)
      throws IOException {
    Map<List<Integer>, List<Object>> result = new HashMap<>();
    int numAggregations = aggregationColumnReaders.size();
    int numGroupByColumns = groupByColumnReaders.size();

    List<ForwardIndexReaderContext> aggregationColumnReaderContexts = new ArrayList<>(numAggregations);
    List<ForwardIndexReaderContext> groupByColumnReaderContexts = new ArrayList<>(numGroupByColumns);
    try {
      for (ForwardIndexReader aggregationColumnReader : aggregationColumnReaders) {
        aggregationColumnReaderContexts.add(aggregationColumnReader.createContext());
      }
      for (ForwardIndexReader groupByColumnReader : groupByColumnReaders) {
        groupByColumnReaderContexts.add(groupByColumnReader.createContext());
      }

      BlockDocIdIterator docIdIterator = starTreeFilterPlanNode.run().nextBlock().getBlockDocIdSet().iterator();
      int docId;
      while ((docId = docIdIterator.next()) != Constants.EOF) {
        // Array of dictionary ids (zero-length array for non-group-by queries)
        List<Integer> group = new ArrayList<>(numGroupByColumns);
        for (int i = 0; i < numGroupByColumns; i++) {
          group.add(groupByColumnReaders.get(i).getDictId(docId, groupByColumnReaderContexts.get(i)));
        }
        List<Object> values = result.computeIfAbsent(group, k -> new ArrayList<>(numAggregations));
        if (values.isEmpty()) {
          for (int i = 0; i < numAggregations; i++) {
            values.add(
                getAggregatedValue(docId, aggregationColumnReaders.get(i), aggregationColumnReaderContexts.get(i)));
          }
        } else {
          for (int i = 0; i < numAggregations; i++) {
            Object value = values.get(i);
            values.set(i, _valueAggregator.applyAggregatedValue(value,
                getAggregatedValue(docId, aggregationColumnReaders.get(i), aggregationColumnReaderContexts.get(i))));
          }
        }
      }
      return result;
    } finally {
      for (ForwardIndexReaderContext readerContext : aggregationColumnReaderContexts) {
        if (readerContext != null) {
          readerContext.close();
        }
      }
      for (ForwardIndexReaderContext readerContext : groupByColumnReaderContexts) {
        if (readerContext != null) {
          readerContext.close();
        }
      }
    }
  }

  private Object getAggregatedValue(int docId, ForwardIndexReader reader, ForwardIndexReaderContext readerContext) {
    switch (_aggregatedValueType) {
      case LONG:
        return reader.getLong(docId, readerContext);
      case DOUBLE:
        return reader.getDouble(docId, readerContext);
      case BYTES:
        return _valueAggregator.deserializeAggregatedValue(reader.getBytes(docId, readerContext));
      default:
        throw new IllegalStateException();
    }
  }

  private Map<List<Integer>, List<Object>> computeNonStarTreeResult(PlanNode nonStarTreeFilterPlanNode,
      List<ForwardIndexReader> aggregationColumnReaders, List<Dictionary> aggregationColumnDictionaries,
      List<ForwardIndexReader> groupByColumnReaders)
      throws IOException {
    Map<List<Integer>, List<Object>> result = new HashMap<>();
    int numAggregations = aggregationColumnReaders.size();
    int numGroupByColumns = groupByColumnReaders.size();

    List<ForwardIndexReaderContext> aggregationColumnReaderContexts = new ArrayList<>(numAggregations);
    List<ForwardIndexReaderContext> groupByColumnReaderContexts = new ArrayList<>(numGroupByColumns);
    try {
      for (ForwardIndexReader aggregationColumnReader : aggregationColumnReaders) {
        if (aggregationColumnReader != null) {
          aggregationColumnReaderContexts.add(aggregationColumnReader.createContext());
        } else {
          aggregationColumnReaderContexts.add(null);
        }
      }
      for (ForwardIndexReader groupByColumnReader : groupByColumnReaders) {
        groupByColumnReaderContexts.add(groupByColumnReader.createContext());
      }

      BlockDocIdIterator docIdIterator = nonStarTreeFilterPlanNode.run().nextBlock().getBlockDocIdSet().iterator();
      int docId;
      while ((docId = docIdIterator.next()) != Constants.EOF) {
        // Array of dictionary ids (zero-length array for non-group-by queries)
        List<Integer> group = new ArrayList<>(numGroupByColumns);
        for (int i = 0; i < numGroupByColumns; i++) {
          group.add(groupByColumnReaders.get(i).getDictId(docId, groupByColumnReaderContexts.get(i)));
        }
        List<Object> values = result.computeIfAbsent(group, k -> new ArrayList<>(numAggregations));
        if (values.isEmpty()) {
          for (int i = 0; i < numAggregations; i++) {
            ForwardIndexReader aggregationColumnReader = aggregationColumnReaders.get(i);
            if (aggregationColumnReader == null) {
              // COUNT aggregation function
              values.add(1L);
            } else {
              Object rawValue = getNextRawValue(docId, aggregationColumnReader, aggregationColumnReaderContexts.get(i),
                  aggregationColumnDictionaries.get(i));
              values.add(_valueAggregator.getInitialAggregatedValue(rawValue));
            }
          }
        } else {
          for (int i = 0; i < numAggregations; i++) {
            Object value = values.get(i);
            ForwardIndexReader aggregationColumnReader = aggregationColumnReaders.get(i);
            if (aggregationColumnReader == null) {
              // COUNT aggregation function
              value = (Long) value + 1;
            } else {
              Object rawValue = getNextRawValue(docId, aggregationColumnReader, aggregationColumnReaderContexts.get(i),
                  aggregationColumnDictionaries.get(i));
              value = _valueAggregator.applyRawValue(value, rawValue);
            }
            values.set(i, value);
          }
        }
      }
      return result;
    } finally {
      for (ForwardIndexReaderContext readerContext : aggregationColumnReaderContexts) {
        if (readerContext != null) {
          readerContext.close();
        }
      }
      for (ForwardIndexReaderContext readerContext : groupByColumnReaderContexts) {
        if (readerContext != null) {
          readerContext.close();
        }
      }
    }
  }

  private Object getNextRawValue(int docId, ForwardIndexReader reader, ForwardIndexReaderContext readerContext,
      Dictionary dictionary) {
    return dictionary.get(reader.getDictId(docId, readerContext));
  }

  abstract ValueAggregator<R, A> getValueAggregator();

  abstract DataType getRawValueType();

  abstract R getRandomRawValue(Random random);

  abstract void assertAggregatedValue(A starTreeResult, A nonStarTreeResult);
}
