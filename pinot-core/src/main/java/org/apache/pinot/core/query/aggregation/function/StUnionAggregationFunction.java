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

import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.pinot.common.CustomObject;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.core.common.BlockValSet;
import org.apache.pinot.core.common.ObjectSerDeUtils;
import org.apache.pinot.core.query.aggregation.AggregationResultHolder;
import org.apache.pinot.core.query.aggregation.ObjectAggregationResultHolder;
import org.apache.pinot.core.query.aggregation.groupby.GroupByResultHolder;
import org.apache.pinot.core.query.aggregation.groupby.ObjectGroupByResultHolder;
import org.apache.pinot.segment.local.utils.GeometrySerializer;
import org.apache.pinot.segment.local.utils.GeometryUtils;
import org.apache.pinot.segment.spi.AggregationFunctionType;
import org.apache.pinot.spi.utils.ByteArray;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.util.GeometryCombiner;
import org.locationtech.jts.operation.union.UnaryUnionOp;


public class StUnionAggregationFunction extends BaseSingleInputAggregationFunction<Geometry, ByteArray> {

  public StUnionAggregationFunction(List<ExpressionContext> arguments, boolean nullHandlingEnabled) {
    super(verifySingleArgument(arguments, "ST_UNION"), nullHandlingEnabled);
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
    BlockValSet blockValSet = blockValSetMap.get(_expression);
    if (blockValSet.isSingleValue()) {
      aggregateSV(length, aggregationResultHolder, blockValSet);
    } else {
      aggregateMV(length, aggregationResultHolder, blockValSet);
    }
  }

  private void aggregateSV(int length, AggregationResultHolder aggregationResultHolder, BlockValSet blockValSet) {
    byte[][] bytesArray = blockValSet.getBytesValuesSV();
    // The holder is written only from inside the range, so a block with no non-null row leaves it untouched and
    // extractFinalResult sees the null that means nothing was aggregated
    forEachNotNull(length, blockValSet, (from, to) -> {
      Geometry geometry = aggregationResultHolder.getResult();
      for (int i = from; i < to; i++) {
        geometry = union(geometry, GeometrySerializer.deserialize(bytesArray[i]));
      }
      aggregationResultHolder.setValue(geometry);
    });
  }

  /// Every geometry of a multi-value row is folded into the same union, so a row contributes once per value.
  private void aggregateMV(int length, AggregationResultHolder aggregationResultHolder, BlockValSet blockValSet) {
    byte[][][] bytesArrays = blockValSet.getBytesValuesMV();
    forEachNotNull(length, blockValSet, (from, to) -> {
      Geometry geometry = aggregationResultHolder.getResult();
      for (int i = from; i < to; i++) {
        for (byte[] bytes : bytesArrays[i]) {
          geometry = union(geometry, GeometrySerializer.deserialize(bytes));
        }
      }
      aggregationResultHolder.setValue(geometry);
    });
  }

  @Override
  public void aggregateGroupBySV(int length, int[] groupKeyArray, GroupByResultHolder groupByResultHolder,
      Map<ExpressionContext, BlockValSet> blockValSetMap) {
    BlockValSet blockValSet = blockValSetMap.get(_expression);
    if (blockValSet.isSingleValue()) {
      aggregateSVGroupBySV(length, groupKeyArray, groupByResultHolder, blockValSet);
    } else {
      aggregateMVGroupBySV(length, groupKeyArray, groupByResultHolder, blockValSet);
    }
  }

  private void aggregateSVGroupBySV(int length, int[] groupKeyArray, GroupByResultHolder groupByResultHolder,
      BlockValSet blockValSet) {
    byte[][] bytesArray = blockValSet.getBytesValuesSV();
    forEachNotNull(length, blockValSet, (from, to) -> {
      for (int i = from; i < to; i++) {
        int groupKey = groupKeyArray[i];
        Geometry value = GeometrySerializer.deserialize(bytesArray[i]);
        groupByResultHolder.setValueForKey(groupKey, union(groupByResultHolder.getResult(groupKey), value));
      }
    });
  }

  private void aggregateMVGroupBySV(int length, int[] groupKeyArray, GroupByResultHolder groupByResultHolder,
      BlockValSet blockValSet) {
    byte[][][] bytesArrays = blockValSet.getBytesValuesMV();
    forEachNotNull(length, blockValSet, (from, to) -> {
      for (int i = from; i < to; i++) {
        int groupKey = groupKeyArray[i];
        for (byte[] bytes : bytesArrays[i]) {
          Geometry value = GeometrySerializer.deserialize(bytes);
          groupByResultHolder.setValueForKey(groupKey, union(groupByResultHolder.getResult(groupKey), value));
        }
      }
    });
  }

  @Override
  public void aggregateGroupByMV(int length, int[][] groupKeysArray, GroupByResultHolder groupByResultHolder,
      Map<ExpressionContext, BlockValSet> blockValSetMap) {
    BlockValSet blockValSet = blockValSetMap.get(_expression);
    if (blockValSet.isSingleValue()) {
      aggregateSVGroupByMV(length, groupKeysArray, groupByResultHolder, blockValSet);
    } else {
      aggregateMVGroupByMV(length, groupKeysArray, groupByResultHolder, blockValSet);
    }
  }

  private void aggregateSVGroupByMV(int length, int[][] groupKeysArray, GroupByResultHolder groupByResultHolder,
      BlockValSet blockValSet) {
    byte[][] bytesArray = blockValSet.getBytesValuesSV();
    forEachNotNull(length, blockValSet, (from, to) -> {
      for (int i = from; i < to; i++) {
        // Deserialized once per row, not once per group key the row belongs to
        Geometry value = GeometrySerializer.deserialize(bytesArray[i]);
        for (int groupKey : groupKeysArray[i]) {
          groupByResultHolder.setValueForKey(groupKey, union(groupByResultHolder.getResult(groupKey), value));
        }
      }
    });
  }

  private void aggregateMVGroupByMV(int length, int[][] groupKeysArray, GroupByResultHolder groupByResultHolder,
      BlockValSet blockValSet) {
    byte[][][] bytesArrays = blockValSet.getBytesValuesMV();
    forEachNotNull(length, blockValSet, (from, to) -> {
      for (int i = from; i < to; i++) {
        for (byte[] bytes : bytesArrays[i]) {
          Geometry value = GeometrySerializer.deserialize(bytes);
          for (int groupKey : groupKeysArray[i]) {
            groupByResultHolder.setValueForKey(groupKey, union(groupByResultHolder.getResult(groupKey), value));
          }
        }
      }
    });
  }

  @Nullable
  @Override
  public Geometry extractAggregationResult(AggregationResultHolder aggregationResultHolder) {
    Geometry geometry = aggregationResultHolder.getResult();
    if (geometry != null) {
      return geometry;
    }
    // With the option disabled an untouched holder still renders the empty accumulator, which is the
    // intermediate this mode has always emitted; with it enabled the null is the signal that nothing was
    // aggregated.
    return _nullHandlingEnabled ? null : GeometryUtils.EMPTY_POINT;
  }

  @Nullable
  @Override
  public Geometry extractGroupByResult(GroupByResultHolder groupByResultHolder, int groupKey) {
    Geometry geometry = groupByResultHolder.getResult(groupKey);
    return geometry != null ? geometry : (_nullHandlingEnabled ? null : GeometryUtils.EMPTY_POINT);
  }

  @Override
  public Geometry merge(Geometry intermediateResult1, Geometry intermediateResult2) {
    return union(intermediateResult1, intermediateResult2);
  }

  @Override
  public DataSchema.ColumnDataType getIntermediateResultColumnType() {
    return DataSchema.ColumnDataType.OBJECT;
  }

  @Override
  public SerializedIntermediateResult serializeIntermediateResult(Geometry geometry) {
    return new SerializedIntermediateResult(ObjectSerDeUtils.ObjectType.Geometry.getValue(),
        ObjectSerDeUtils.GEOMETRY_SER_DE.serialize(geometry));
  }

  @Override
  public Geometry deserializeIntermediateResult(CustomObject customObject) {
    return ObjectSerDeUtils.GEOMETRY_SER_DE.deserialize(customObject.getBuffer());
  }

  @Override
  public DataSchema.ColumnDataType getFinalResultColumnType() {
    return DataSchema.ColumnDataType.BYTES;
  }

  @Nullable
  @Override
  public ByteArray extractFinalResult(@Nullable Geometry geometry) {
    if (geometry == null) {
      // A null intermediate result means nothing was aggregated. With null handling enabled the union of no
      // geometries is NULL, matching ST_Union; with it disabled it is the empty point, which is the answer this mode
      // has always given.
      return _nullHandlingEnabled ? null : new ByteArray(GeometrySerializer.serialize(GeometryUtils.EMPTY_POINT));
    }
    return new ByteArray(GeometrySerializer.serialize(geometry));
  }

  /// Returns the union of the supplied geometries.
  ///
  /// When either operand is a `GeometryCollection`, [Geometry#union(Geometry)] can produce invalid
  /// topologies or drop components because it expects homogeneous inputs.  The [UnaryUnionOp] implementation is
  /// purpose-built for arbitrary collections, so we first combine the components and delegate to it to ensure a valid
  /// and deterministic result.
  @Nullable
  private static Geometry union(@Nullable Geometry left, @Nullable Geometry right) {
    if (left == null) {
      return right;
    }
    if (right == null) {
      return left;
    }
    if (Geometry.TYPENAME_GEOMETRYCOLLECTION.equals(left.getGeometryType())
        || Geometry.TYPENAME_GEOMETRYCOLLECTION.equals(right.getGeometryType())) {
      return UnaryUnionOp.union(GeometryCombiner.combine(left, right));
    }
    return left.union(right);
  }
}
