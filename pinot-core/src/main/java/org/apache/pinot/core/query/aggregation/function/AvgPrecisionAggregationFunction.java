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

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.List;
import java.util.Map;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.core.common.BlockValSet;
import org.apache.pinot.core.common.ObjectSerDeUtils;
import org.apache.pinot.core.query.aggregation.AggregationResultHolder;
import org.apache.pinot.core.query.aggregation.ObjectAggregationResultHolder;
import org.apache.pinot.core.query.aggregation.groupby.GroupByResultHolder;
import org.apache.pinot.core.query.aggregation.groupby.ObjectGroupByResultHolder;
import org.apache.pinot.segment.local.customobject.AvgBigPair;
import org.apache.pinot.segment.spi.AggregationFunctionType;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.roaringbitmap.RoaringBitmap;

public class AvgPrecisionAggregationFunction extends BaseSingleInputAggregationFunction<AvgBigPair, BigDecimal> {
    private static final BigDecimal DEFAULT_FINAL_RESULT = new BigDecimal("-1E+1000");
    private final boolean _nullHandlingEnabled;

    public AvgPrecisionAggregationFunction(List<ExpressionContext> arguments, boolean nullHandlingEnabled) {
        this(verifySingleArgument(arguments, "AVG"), nullHandlingEnabled);
    }

    protected AvgPrecisionAggregationFunction(ExpressionContext expression, boolean nullHandlingEnabled) {
        super(expression);
        _nullHandlingEnabled = nullHandlingEnabled;
    }

    @Override
    public AggregationFunctionType getType() {
        return AggregationFunctionType.AVGPRECISION;
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
        if (_nullHandlingEnabled) {
            RoaringBitmap nullBitmap = blockValSet.getNullBitmap();
            if (nullBitmap != null && !nullBitmap.isEmpty()) {
                aggregateNullHandlingEnabled(length, aggregationResultHolder, blockValSet, nullBitmap);
                return;
            }
        }

        if (blockValSet.getValueType() != DataType.BYTES) {
            double[] doubleValues = blockValSet.getDoubleValuesSV();
            BigDecimal sum = BigDecimal.ZERO;
            for (int i = 0; i < length; i++) {
                sum = sum.add(BigDecimal.valueOf(doubleValues[i]));
            }
            setAggregationResult(aggregationResultHolder, sum, length);
        } else {
            // Serialized AvgBigPair
            byte[][] bytesValues = blockValSet.getBytesValuesSV();
            BigDecimal sum = BigDecimal.ZERO;
            long count = 0L;
            for (int i = 0; i < length; i++) {
                AvgBigPair value = ObjectSerDeUtils.AVG_BIG_PAIR_SER_DE.deserialize(bytesValues[i]);
                sum = sum.add(value.getSum());
                count += value.getCount();
            }
            setAggregationResult(aggregationResultHolder, sum, count);
        }
    }

    private void aggregateNullHandlingEnabled(int length, AggregationResultHolder aggregationResultHolder,
                                              BlockValSet blockValSet, RoaringBitmap nullBitmap) {
        if (blockValSet.getValueType() != DataType.BYTES) {
            double[] doubleValues = blockValSet.getDoubleValuesSV();
            if (nullBitmap.getCardinality() < length) {
                BigDecimal sum = BigDecimal.ZERO;
                long count = 0L;
                for (int i = 0; i < length; i++) {
                    if (!nullBitmap.contains(i)) {
                        sum = sum.add(BigDecimal.valueOf(doubleValues[i]));
                        count++;
                    }
                }
                setAggregationResult(aggregationResultHolder, sum, count);
            }
        } else {
            // Serialized AvgBigPair
            byte[][] bytesValues = blockValSet.getBytesValuesSV();
            if (nullBitmap.getCardinality() < length) {
                BigDecimal sum = BigDecimal.ZERO;
                long count = 0L;
                for (int i = 0; i < length; i++) {
                    if (!nullBitmap.contains(i)) {
                        AvgBigPair value = ObjectSerDeUtils.AVG_BIG_PAIR_SER_DE.deserialize(bytesValues[i]);
                        sum = sum.add(value.getSum());
                        count += value.getCount();
                    }
                }
                setAggregationResult(aggregationResultHolder, sum, count);
            }
        }
    }

    protected void setAggregationResult(AggregationResultHolder aggregationResultHolder, BigDecimal sum, long count) {
        AvgBigPair avgPair = aggregationResultHolder.getResult();
        if (avgPair == null) {
            aggregationResultHolder.setValue(new AvgBigPair(sum, count));
        } else {
            avgPair.apply(sum, count);
        }
    }

    @Override
    public void aggregateGroupBySV(int length, int[] groupKeyArray, GroupByResultHolder groupByResultHolder,
                                   Map<ExpressionContext, BlockValSet> blockValSetMap) {
        BlockValSet blockValSet = blockValSetMap.get(_expression);
        if (_nullHandlingEnabled) {
            RoaringBitmap nullBitmap = blockValSet.getNullBitmap();
            if (nullBitmap != null && !nullBitmap.isEmpty()) {
                aggregateGroupBySVNullHandlingEnabled(length, groupKeyArray,
                        groupByResultHolder, blockValSet, nullBitmap);
                return;
            }
        }

        if (blockValSet.getValueType() != DataType.BYTES) {
            double[] doubleValues = blockValSet.getDoubleValuesSV();
            for (int i = 0; i < length; i++) {
                setGroupByResult(groupKeyArray[i], groupByResultHolder, BigDecimal.valueOf(doubleValues[i]), 1L);
            }
        } else {
            // Serialized AvgBigPair
            byte[][] bytesValues = blockValSet.getBytesValuesSV();
            for (int i = 0; i < length; i++) {
                AvgBigPair avgPair = ObjectSerDeUtils.AVG_BIG_PAIR_SER_DE.deserialize(bytesValues[i]);
                setGroupByResult(groupKeyArray[i], groupByResultHolder, avgPair.getSum(), avgPair.getCount());
            }
        }
    }

    private void aggregateGroupBySVNullHandlingEnabled(int length, int[] groupKeyArray,
                                                       GroupByResultHolder groupByResultHolder,
                                                       BlockValSet blockValSet, RoaringBitmap nullBitmap) {
        if (blockValSet.getValueType() != DataType.BYTES) {
            double[] doubleValues = blockValSet.getDoubleValuesSV();
            if (nullBitmap.getCardinality() < length) {
                for (int i = 0; i < length; i++) {
                    if (!nullBitmap.contains(i)) {
                        int groupKey = groupKeyArray[i];
                        setGroupByResult(groupKey, groupByResultHolder, BigDecimal.valueOf(doubleValues[i]), 1L);
                    }
                }
            }
        } else {
            // Serialized AvgBigPair
            byte[][] bytesValues = blockValSet.getBytesValuesSV();
            if (nullBitmap.getCardinality() < length) {
                for (int i = 0; i < length; i++) {
                    if (!nullBitmap.contains(i)) {
                        int groupKey = groupKeyArray[i];
                        AvgBigPair avgPair = ObjectSerDeUtils.AVG_BIG_PAIR_SER_DE.deserialize(bytesValues[i]);
                        setGroupByResult(groupKey, groupByResultHolder, avgPair.getSum(), avgPair.getCount());
                    }
                }
            }
        }
    }

    @Override
    public void aggregateGroupByMV(int length, int[][] groupKeysArray, GroupByResultHolder groupByResultHolder,
                                   Map<ExpressionContext, BlockValSet> blockValSetMap) {
        BlockValSet blockValSet = blockValSetMap.get(_expression);

        if (blockValSet.getValueType() != DataType.BYTES) {
            double[] doubleValues = blockValSet.getDoubleValuesSV();
            for (int i = 0; i < length; i++) {
                BigDecimal value = BigDecimal.valueOf(doubleValues[i]);
                for (int groupKey : groupKeysArray[i]) {
                    setGroupByResult(groupKey, groupByResultHolder, value, 1L);
                }
            }
        } else {
            // Serialized AvgBigPair
            byte[][] bytesValues = blockValSet.getBytesValuesSV();
            for (int i = 0; i < length; i++) {
                AvgBigPair avgPair = ObjectSerDeUtils.AVG_BIG_PAIR_SER_DE.deserialize(bytesValues[i]);
                BigDecimal sum = avgPair.getSum();
                long count = avgPair.getCount();
                for (int groupKey : groupKeysArray[i]) {
                    setGroupByResult(groupKey, groupByResultHolder, sum, count);
                }
            }
        }
    }

    protected void setGroupByResult(int groupKey, GroupByResultHolder groupByResultHolder, BigDecimal sum, long count) {
        AvgBigPair avgPair = groupByResultHolder.getResult(groupKey);
        if (avgPair == null) {
            groupByResultHolder.setValueForKey(groupKey, new AvgBigPair(sum, count));
        } else {
            avgPair.apply(sum, count);
        }
    }

    @Override
    public AvgBigPair extractAggregationResult(AggregationResultHolder aggregationResultHolder) {
        AvgBigPair avgPair = aggregationResultHolder.getResult();
        if (avgPair == null) {
            return _nullHandlingEnabled ? null : new AvgBigPair(BigDecimal.ZERO, 0L);
        }
        return avgPair;
    }

    @Override
    public AvgBigPair extractGroupByResult(GroupByResultHolder groupByResultHolder, int groupKey) {
        AvgBigPair avgPair = groupByResultHolder.getResult(groupKey);
        if (avgPair == null) {
            return _nullHandlingEnabled ? null : new AvgBigPair(BigDecimal.ZERO, 0L);
        }
        return avgPair;
    }

    @Override
    public AvgBigPair merge(AvgBigPair intermediateResult1, AvgBigPair intermediateResult2) {
        if (_nullHandlingEnabled) {
            if (intermediateResult1 == null) {
                return intermediateResult2;
            }
            if (intermediateResult2 == null) {
                return intermediateResult1;
            }
        }
        intermediateResult1.apply(intermediateResult2);
        return intermediateResult1;
    }

    @Override
    public ColumnDataType getIntermediateResultColumnType() {
        return ColumnDataType.OBJECT;
    }

    @Override
    public ColumnDataType getFinalResultColumnType() {
        return ColumnDataType.BIG_DECIMAL;
    }

    @Override
    public BigDecimal extractFinalResult(AvgBigPair intermediateResult) {
        if (intermediateResult == null) {
            return null;
        }
        long count = intermediateResult.getCount();
        if (count == 0L) {
            return DEFAULT_FINAL_RESULT;
        } else {
            return intermediateResult.getSum().divide(BigDecimal.valueOf(count), RoundingMode.HALF_UP);
        }
    }
}
