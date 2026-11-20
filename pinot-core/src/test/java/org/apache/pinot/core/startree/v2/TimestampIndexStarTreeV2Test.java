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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.Set;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.core.query.aggregation.function.AggregationFunction;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.core.query.request.context.utils.QueryContextConverterUtils;
import org.apache.pinot.core.startree.StarTreeUtils;
import org.apache.pinot.segment.local.aggregator.SumValueAggregator;
import org.apache.pinot.segment.local.aggregator.ValueAggregator;
import org.apache.pinot.segment.spi.index.startree.AggregationFunctionColumnPair;
import org.apache.pinot.spi.config.table.FieldConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.config.table.TimestampConfig;
import org.apache.pinot.spi.config.table.TimestampIndexGranularity;
import org.apache.pinot.spi.data.DateTimeFieldSpec;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.utils.TimestampIndexUtils;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;


public class TimestampIndexStarTreeV2Test extends BaseStarTreeV2Test<Object, Double> {
  private static final String TIMESTAMP_COLUMN = "tsCol";
  private static final String TIMESTAMP_COLUMN_WITH_GRANULARITY =
      TimestampIndexUtils.getColumnWithGranularity(TIMESTAMP_COLUMN, TimestampIndexGranularity.MILLISECOND);

  @Override
  ValueAggregator<Object, Double> getValueAggregator() {
    return new SumValueAggregator();
  }

  @Override
  DataType getRawValueType() {
    return DataType.INT;
  }

  @Override
  Object getRandomRawValue(Random random) {
    return random.nextInt();
  }

  @Override
  void assertAggregatedValue(Double starTreeResult, Double nonStarTreeResult) {
    assertEquals(starTreeResult, nonStarTreeResult, 1e-5);
  }

  @Override
  protected void addDimensionFields(Schema.SchemaBuilder schemaBuilder) {
    super.addDimensionFields(schemaBuilder);
    schemaBuilder.addField(
        new DateTimeFieldSpec(TIMESTAMP_COLUMN, DataType.TIMESTAMP, DateTimeFieldSpec.TimeFormat.TIMESTAMP.name(),
            "1:MILLISECONDS"));
  }

  @Override
  protected void addDimensionValues(GenericRow segmentRecord, Random random) {
    super.addDimensionValues(segmentRecord, random);
    segmentRecord.putValue(TIMESTAMP_COLUMN, (long) random.nextInt(DIMENSION_CARDINALITY));
  }

  @Override
  protected TableConfig createTableConfig() {
    return new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME)
        .setFieldConfigList(List.of(new FieldConfig.Builder(TIMESTAMP_COLUMN).withTimestampConfig(
            new TimestampConfig(List.of(TimestampIndexGranularity.MILLISECOND))).build()))
        .build();
  }

  @Override
  protected List<String> getDimensionsSplitOrder() {
    return List.of(DIMENSION1, DIMENSION2, TIMESTAMP_COLUMN_WITH_GRANULARITY);
  }

  @Test
  public void testStarTreeBuiltOnTimestampIndexColumn() {
    assertTrue(getStarTreeV2().getMetadata().getDimensionsSplitOrder().contains(TIMESTAMP_COLUMN_WITH_GRANULARITY));
    assertEquals(getStarTreeV2().getDataSource(TIMESTAMP_COLUMN_WITH_GRANULARITY).getDictionary().length(),
        DIMENSION_CARDINALITY);
  }

  @Test
  public void testStarTreeFitForQueryOnTimestampIndexColumn() {
    QueryContext queryContext = QueryContextConverterUtils.getQueryContext(
        String.format("SELECT %s FROM %s WHERE %s < 10 GROUP BY %s", getAggregation(), TABLE_NAME,
            TIMESTAMP_COLUMN_WITH_GRANULARITY, TIMESTAMP_COLUMN_WITH_GRANULARITY));
    AggregationFunction[] aggregationFunctions = queryContext.getAggregationFunctions();
    assertNotNull(aggregationFunctions);
    AggregationFunctionColumnPair[] aggregationFunctionColumnPairs =
        StarTreeUtils.extractAggregationFunctionPairs(aggregationFunctions);
    assertNotNull(aggregationFunctionColumnPairs);
    List<Pair<AggregationFunction, AggregationFunctionColumnPair>> aggregations =
        new ArrayList<>(aggregationFunctions.length);
    for (int i = 0; i < aggregationFunctions.length; i++) {
      aggregations.add(Pair.of(aggregationFunctions[i], aggregationFunctionColumnPairs[i]));
    }
    List<ExpressionContext> groupByExpressions = queryContext.getGroupByExpressions();
    assertNotNull(groupByExpressions);
    assertTrue(StarTreeUtils.isFitForStarTree(getStarTreeV2().getMetadata(), aggregations,
        groupByExpressions.toArray(new ExpressionContext[0]), Set.of(TIMESTAMP_COLUMN_WITH_GRANULARITY)));
    assertFalse(StarTreeUtils.isFitForStarTree(getStarTreeV2().getMetadata(), aggregations,
        groupByExpressions.toArray(new ExpressionContext[0]), Set.of(TIMESTAMP_COLUMN)));
  }

  @Test
  public void testQueriesOnTimestampIndexColumn()
      throws IOException {
    String query = String.format("SELECT %s FROM %s", getAggregation(), TABLE_NAME);
    testQuery(query + String.format(" WHERE %s = 0", TIMESTAMP_COLUMN_WITH_GRANULARITY));
    testQuery(query + String.format(" WHERE %s < 10", TIMESTAMP_COLUMN_WITH_GRANULARITY));
    testQuery(query + String.format(" WHERE %1$s > 10 OR %1$s < 50", TIMESTAMP_COLUMN_WITH_GRANULARITY));
    testQuery(query + String.format(" WHERE NOT %s > 10", TIMESTAMP_COLUMN_WITH_GRANULARITY));
    testQuery(
        query + String.format(" WHERE %s < 10 AND %s > 50", TIMESTAMP_COLUMN_WITH_GRANULARITY, DIMENSION1));
    testQuery(query + String.format(" WHERE %s > 90 AND NOT %s < 25", TIMESTAMP_COLUMN_WITH_GRANULARITY, DIMENSION2));
    testQuery(query + " GROUP BY " + TIMESTAMP_COLUMN_WITH_GRANULARITY);
  }
}
