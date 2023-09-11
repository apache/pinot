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
package org.apache.pinot.core.query.aggregation.function;

import com.google.common.base.Preconditions;
import java.util.List;
import java.util.Map;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.core.common.BlockValSet;
import org.apache.pinot.core.query.aggregation.AggregationResultHolder;
import org.apache.pinot.core.query.aggregation.ObjectAggregationResultHolder;
import org.apache.pinot.core.query.aggregation.groupby.GroupByResultHolder;
import org.apache.pinot.core.query.aggregation.groupby.ObjectGroupByResultHolder;
import org.apache.pinot.segment.local.utils.GeometrySerializer;
import org.apache.pinot.segment.local.utils.GeometryUtils;
import org.apache.pinot.segment.spi.AggregationFunctionType;
import org.apache.pinot.spi.utils.ByteArray;
import org.locationtech.jts.geom.Geometry;


public class StUnionAggregationFunction extends BaseSingleInputAggregationFunction<Geometry, ByteArray> {

  public StUnionAggregationFunction(List<ExpressionContext> arguments) {
    super(verifyArguments(arguments));
  }

  private static ExpressionContext verifyArguments(List<ExpressionContext> arguments) {
    Preconditions.checkArgument(arguments.size() == 1, "ST_UNION expects 1 argument, got: %s", arguments.size());
    return arguments.get(0);
  }

  @Override
  public AggregationFunctionType getType() {
    return AggregationFunctionType.STUNION;
  }

  @Override
  public AggregationResultHolder createAggregationResultHolder() {
    return new ObjectAggregationResultHolder();
  }

  @Override
  public GroupByResultHolder createGroupByResultHolder(int initialCapacity, int maxCapacity) {
    return new ObjectGroupByResultHolder(initialCapacity, maxCapacity);
  }

  @Override
  public void aggregate(int length, AggregationResultHolder aggregationResultHolder,
      Map<ExpressionContext, BlockValSet> blockValSetMap) {
    byte[][] bytesArray = blockValSetMap.get(_expression).getBytesValuesSV();
    Geometry geometry = aggregationResultHolder.getResult();
    for (int i = 0; i < length; i++) {
      Geometry value = GeometrySerializer.deserialize(bytesArray[i]);
      geometry = geometry == null ? value : geometry.union(value);
    }
    aggregationResultHolder.setValue(geometry);
  }

  @Override
  public void aggregateGroupBySV(int length, int[] groupKeyArray, GroupByResultHolder groupByResultHolder,
      Map<ExpressionContext, BlockValSet> blockValSetMap) {
    byte[][] bytesArray = blockValSetMap.get(_expression).getBytesValuesSV();
    for (int i = 0; i < length; i++) {
      int groupKey = groupKeyArray[i];
      Geometry value = GeometrySerializer.deserialize(bytesArray[i]);
      Geometry geometry = groupByResultHolder.getResult(groupKey);
      groupByResultHolder.setValueForKey(groupKey, geometry == null ? value : geometry.union(value));
    }
  }

  @Override
  public void aggregateGroupByMV(int length, int[][] groupKeysArray, GroupByResultHolder groupByResultHolder,
      Map<ExpressionContext, BlockValSet> blockValSetMap) {
    byte[][] bytesArray = blockValSetMap.get(_expression).getBytesValuesSV();
    for (int i = 0; i < length; i++) {
      Geometry value = GeometrySerializer.deserialize(bytesArray[i]);
      for (int groupKey : groupKeysArray[i]) {
        Geometry geometry = groupByResultHolder.getResult(groupKey);
        groupByResultHolder.setValueForKey(groupKey, geometry == null ? value : geometry.union(value));
      }
    }
  }

  @Override
  public Geometry extractAggregationResult(AggregationResultHolder aggregationResultHolder) {
    Geometry geometry = aggregationResultHolder.getResult();
    return geometry == null ? GeometryUtils.EMPTY_POINT : geometry;
  }

  @Override
  public Geometry extractGroupByResult(GroupByResultHolder groupByResultHolder, int groupKey) {
    Geometry geometry = groupByResultHolder.getResult(groupKey);
    return geometry == null ? GeometryUtils.EMPTY_POINT : geometry;
  }

  @Override
  public Geometry merge(Geometry intermediateResult1, Geometry intermediateResult2) {
    return intermediateResult1.union(intermediateResult2);
  }

  @Override
  public DataSchema.ColumnDataType getIntermediateResultColumnType() {
    return DataSchema.ColumnDataType.OBJECT;
  }

  @Override
  public DataSchema.ColumnDataType getFinalResultColumnType() {
    return DataSchema.ColumnDataType.BYTES;
  }

  @Override
  public ByteArray extractFinalResult(Geometry geometry) {
    return new ByteArray(GeometrySerializer.serialize(geometry));
  }
}
