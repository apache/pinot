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

import com.clearspring.analytics.stream.cardinality.HyperLogLog;
import com.clearspring.analytics.stream.cardinality.HyperLogLogPlus;
import com.dynatrace.hash4j.distinctcount.UltraLogLog;
import it.unimi.dsi.fastutil.doubles.Double2LongOpenHashMap;
import it.unimi.dsi.fastutil.doubles.DoubleArrayList;
import it.unimi.dsi.fastutil.doubles.DoubleOpenHashSet;
import it.unimi.dsi.fastutil.floats.Float2LongOpenHashMap;
import it.unimi.dsi.fastutil.floats.FloatArrayList;
import it.unimi.dsi.fastutil.floats.FloatOpenHashSet;
import it.unimi.dsi.fastutil.ints.Int2LongOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.longs.Long2LongOpenHashMap;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import it.unimi.dsi.fastutil.objects.Object2LongOpenHashMap;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import it.unimi.dsi.fastutil.objects.ObjectLinkedOpenHashSet;
import it.unimi.dsi.fastutil.objects.ObjectOpenHashSet;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.PriorityQueue;
import org.apache.datasketches.cpc.CpcSketch;
import org.apache.datasketches.frequencies.FrequentItemsSketch;
import org.apache.datasketches.frequencies.FrequentLongsSketch;
import org.apache.datasketches.kll.KllDoublesSketch;
import org.apache.datasketches.theta.ThetaSketch;
import org.apache.datasketches.theta.UpdatableThetaSketch;
import org.apache.datasketches.tuple.aninteger.IntegerSummary;
import org.apache.datasketches.tuple.aninteger.IntegerTupleSketch;
import org.apache.pinot.common.CustomObject;
import org.apache.pinot.common.request.Literal;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.request.context.FunctionContext;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.core.operator.docvalsets.RowBasedBlockValSet;
import org.apache.pinot.core.query.aggregation.function.AggregationFunction.SerializedResult;
import org.apache.pinot.core.query.aggregation.function.array.ArrayAggBigDecimalFunction;
import org.apache.pinot.core.query.aggregation.function.array.ArrayAggBytesFunction;
import org.apache.pinot.core.query.aggregation.function.array.ArrayAggDistinctBigDecimalFunction;
import org.apache.pinot.core.query.aggregation.function.array.ArrayAggDistinctBytesFunction;
import org.apache.pinot.core.query.aggregation.function.array.ArrayAggDistinctDoubleFunction;
import org.apache.pinot.core.query.aggregation.function.array.ArrayAggDistinctFloatFunction;
import org.apache.pinot.core.query.aggregation.function.array.ArrayAggDistinctIntFunction;
import org.apache.pinot.core.query.aggregation.function.array.ArrayAggDistinctLongFunction;
import org.apache.pinot.core.query.aggregation.function.array.ArrayAggDistinctStringFunction;
import org.apache.pinot.core.query.aggregation.function.array.ArrayAggDoubleFunction;
import org.apache.pinot.core.query.aggregation.function.array.ArrayAggFloatFunction;
import org.apache.pinot.core.query.aggregation.function.array.ArrayAggIntFunction;
import org.apache.pinot.core.query.aggregation.function.array.ArrayAggLongFunction;
import org.apache.pinot.core.query.aggregation.function.array.ArrayAggStringFunction;
import org.apache.pinot.core.query.aggregation.function.array.ListAggDistinctFunction;
import org.apache.pinot.core.query.aggregation.function.array.ListAggFunction;
import org.apache.pinot.core.query.aggregation.function.array.SumArrayDoubleAggregationFunction;
import org.apache.pinot.core.query.aggregation.function.array.SumArrayLongAggregationFunction;
import org.apache.pinot.core.query.aggregation.function.funnel.FunnelCountAggregationFunctionFactory;
import org.apache.pinot.core.query.aggregation.function.funnel.FunnelStepEvent;
import org.apache.pinot.core.query.aggregation.function.funnel.window.FunnelMaxStepAggregationFunction;
import org.apache.pinot.core.query.aggregation.utils.exprminmax.ExprMinMaxMeasuringValSetWrapper;
import org.apache.pinot.core.query.aggregation.utils.exprminmax.ExprMinMaxObject;
import org.apache.pinot.core.query.aggregation.utils.exprminmax.ExprMinMaxProjectionValSetWrapper;
import org.apache.pinot.core.query.utils.idset.IdSet;
import org.apache.pinot.core.query.utils.idset.IdSets;
import org.apache.pinot.segment.local.customobject.AvgPair;
import org.apache.pinot.segment.local.customobject.BigDecimalLongPair;
import org.apache.pinot.segment.local.customobject.BytesLongPair;
import org.apache.pinot.segment.local.customobject.CovarianceTuple;
import org.apache.pinot.segment.local.customobject.CpcSketchAccumulator;
import org.apache.pinot.segment.local.customobject.DoubleLongPair;
import org.apache.pinot.segment.local.customobject.FloatLongPair;
import org.apache.pinot.segment.local.customobject.IntLongPair;
import org.apache.pinot.segment.local.customobject.LongLongPair;
import org.apache.pinot.segment.local.customobject.MinMaxRangePair;
import org.apache.pinot.segment.local.customobject.PinotFourthMoment;
import org.apache.pinot.segment.local.customobject.QuantileDigest;
import org.apache.pinot.segment.local.customobject.SerializedFrequentLongsSketch;
import org.apache.pinot.segment.local.customobject.SerializedFrequentStringsSketch;
import org.apache.pinot.segment.local.customobject.StringLongPair;
import org.apache.pinot.segment.local.customobject.ThetaSketchAccumulator;
import org.apache.pinot.segment.local.customobject.TupleIntSketchAccumulator;
import org.apache.pinot.segment.local.customobject.VarianceTuple;
import org.apache.pinot.segment.local.customobject.tdigest.NonFiniteAwareTDigest;
import org.apache.pinot.segment.local.customobject.tdigest.SerializedTDigest;
import org.apache.pinot.segment.local.customobject.tdigest.TDigest;
import org.apache.pinot.segment.local.utils.GeometryUtils;
import org.apache.pinot.segment.spi.memory.PagedPinotOutputStream;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.utils.ByteArray;
import org.locationtech.jts.geom.Coordinate;
import org.roaringbitmap.RoaringBitmap;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertSame;


/// Checks that the serialized intermediate result of an aggregation function has the wire type value mixed-version
/// servers and brokers exchange, writes the payload of its [SerializedResult#getBytes] into a heap or direct buffer
/// and into a paged output stream, filling exactly [SerializedResult#getSize] bytes, and that the payload deserializes
/// to a result with the same final result, which keeps no reference to the buffer it was read from.
@SuppressWarnings({"rawtypes", "unchecked"})
public class IntermediateResultByteBufferSerDeTest {
  private static final ExpressionContext COLUMN = ExpressionContext.forIdentifier("column");
  private static final ExpressionContext COLUMN_2 = ExpressionContext.forIdentifier("column2");
  private static final ExpressionContext TIME_COLUMN = ExpressionContext.forIdentifier("time");
  private static final ExpressionContext THRESHOLD_ONE =
      ExpressionContext.forLiteral(Literal.stringValue("threshold=1"));
  private static final int PADDING = 4;
  private static final byte PADDING_BYTE = 0x5a;

  private static final HistogramAggregationFunction HISTOGRAM = new HistogramAggregationFunction(
      List.of(COLUMN, ExpressionContext.forLiteral(Literal.doubleValue(0)),
          ExpressionContext.forLiteral(Literal.doubleValue(10)), ExpressionContext.forLiteral(Literal.intValue(2))),
      false);
  private static final PercentileTDigestAggregationFunction PERCENTILE_TDIGEST =
      new PercentileTDigestAggregationFunction(COLUMN, 50, false);
  private static final PercentileRawTDigestAggregationFunction PERCENTILE_RAW_TDIGEST =
      new PercentileRawTDigestAggregationFunction(COLUMN, 50, false);
  private static final PercentileSmartTDigestAggregationFunction PERCENTILE_SMART_TDIGEST =
      new PercentileSmartTDigestAggregationFunction(
          List.of(COLUMN, ExpressionContext.forLiteral(Literal.doubleValue(50)), THRESHOLD_ONE), false);
  private static final DistinctCountAggregationFunction DISTINCT_COUNT =
      new DistinctCountAggregationFunction(List.of(COLUMN), false);
  private static final DistinctCountBitmapAggregationFunction DISTINCT_COUNT_BITMAP =
      new DistinctCountBitmapAggregationFunction(List.of(COLUMN), false);
  private static final DistinctCountSmartHLLAggregationFunction DISTINCT_COUNT_SMART_HLL =
      new DistinctCountSmartHLLAggregationFunction(List.of(COLUMN, THRESHOLD_ONE), false);
  private static final DistinctCountSmartHLLPlusAggregationFunction DISTINCT_COUNT_SMART_HLL_PLUS =
      new DistinctCountSmartHLLPlusAggregationFunction(List.of(COLUMN, THRESHOLD_ONE), false);
  private static final DistinctCountSmartULLAggregationFunction DISTINCT_COUNT_SMART_ULL =
      new DistinctCountSmartULLAggregationFunction(List.of(COLUMN, THRESHOLD_ONE), false);
  private static final ModeAggregationFunction MODE = new ModeAggregationFunction(List.of(COLUMN), false);
  private static final ModeAggregationFunction MODE_STRING = new ModeAggregationFunction(
      List.of(COLUMN, ExpressionContext.forLiteral(Literal.stringValue("MIN")),
          ExpressionContext.forLiteral(Literal.stringValue("STRING"))), false);
  private static final AnyValueAggregationFunction ANY_VALUE = new AnyValueAggregationFunction(List.of(COLUMN), false);
  private static final FunnelMaxStepAggregationFunction FUNNEL_MAX_STEP = new FunnelMaxStepAggregationFunction(
      List.of(TIME_COLUMN, ExpressionContext.forLiteral(Literal.longValue(1000L)),
          ExpressionContext.forLiteral(Literal.intValue(2)), ExpressionContext.forIdentifier("step0"),
          ExpressionContext.forIdentifier("step1")), false);
  private static final AggregationFunction FUNNEL_COUNT = funnelCount();
  private static final AggregationFunction FUNNEL_COUNT_SET = funnelCount("set");

  private static AggregationFunction funnelCount(String... settings) {
    List<ExpressionContext> arguments = new ArrayList<>(List.of(
        ExpressionContext.forFunction(new FunctionContext(FunctionContext.Type.TRANSFORM, "steps",
            List.of(ExpressionContext.forIdentifier("step0"), ExpressionContext.forIdentifier("step1")))),
        ExpressionContext.forFunction(new FunctionContext(FunctionContext.Type.TRANSFORM, "correlateby",
            List.of(ExpressionContext.forIdentifier("userId"))))));
    if (settings.length > 0) {
      List<ExpressionContext> literals = new ArrayList<>(settings.length);
      for (String setting : settings) {
        literals.add(ExpressionContext.forLiteral(Literal.stringValue(setting)));
      }
      arguments.add(ExpressionContext.forFunction(
          new FunctionContext(FunctionContext.Type.TRANSFORM, "settings", literals)));
    }
    return new FunnelCountAggregationFunctionFactory(arguments, false).get();
  }

  private static PinotFourthMoment fourthMoment() {
    PinotFourthMoment moment = new PinotFourthMoment();
    moment.combine(4, 1.0, 2.0, 3.0, 4.0);
    return moment;
  }

  private static QuantileDigest quantileDigest() {
    QuantileDigest digest = new QuantileDigest(0.05);
    for (long value = 1; value <= 10; value++) {
      digest.add(value);
    }
    return digest;
  }

  private static KllDoublesSketch kllSketch() {
    KllDoublesSketch sketch = KllDoublesSketch.newHeapInstance();
    for (int value = 1; value <= 10; value++) {
      sketch.update(value);
    }
    return sketch;
  }

  private static FrequentLongsSketch frequentLongsSketch() {
    FrequentLongsSketch sketch = new FrequentLongsSketch(64);
    sketch.update(1L);
    sketch.update(1L);
    sketch.update(2L);
    return sketch;
  }

  private static FrequentItemsSketch<String> frequentStringsSketch() {
    FrequentItemsSketch<String> sketch = new FrequentItemsSketch<>(64);
    sketch.update("a");
    sketch.update("a");
    sketch.update("b");
    return sketch;
  }

  private static CpcSketchAccumulator cpcSketchAccumulator() {
    CpcSketch sketch = new CpcSketch(12);
    sketch.update(1);
    sketch.update(2);
    CpcSketchAccumulator accumulator = new CpcSketchAccumulator();
    accumulator.apply(sketch);
    return accumulator;
  }

  private static TupleIntSketchAccumulator tupleIntSketchAccumulator() {
    IntegerTupleSketch sketch = new IntegerTupleSketch(12, IntegerSummary.Mode.Sum);
    sketch.update(1L, 1);
    sketch.update(2L, 1);
    TupleIntSketchAccumulator accumulator = new TupleIntSketchAccumulator();
    accumulator.apply(sketch.compact());
    return accumulator;
  }

  private static ThetaSketchAccumulator thetaSketchAccumulator() {
    ThetaSketchAccumulator accumulator = new ThetaSketchAccumulator();
    accumulator.apply(thetaSketch(1, 2));
    return accumulator;
  }

  private static ThetaSketch thetaSketch(long... values) {
    UpdatableThetaSketch sketch = UpdatableThetaSketch.builder().build();
    for (long value : values) {
      sketch.update(value);
    }
    return sketch.compact();
  }

  @DataProvider
  public static Object[][] intermediateResults() {
    // Three one-value containers: serialized through a heap array when the destination is direct.
    RoaringBitmap sparseBitmap = RoaringBitmap.bitmapOf(1, 1 << 16, 3 << 16);
    RoaringBitmap denseBitmap = new RoaringBitmap();
    denseBitmap.add(0L, 1L << 17);
    HyperLogLog hyperLogLog = new HyperLogLog(8);
    HyperLogLogPlus hyperLogLogPlus = new HyperLogLogPlus(14, 25);
    UltraLogLog ultraLogLog = UltraLogLog.create(12);
    for (int i = 0; i < 100; i++) {
      hyperLogLog.offer(i);
      hyperLogLogPlus.offer(i);
      ultraLogLog.add(i);
    }
    TDigest tDigest = NonFiniteAwareTDigest.forLegacyAggregation(100);
    tDigest.add(1.5, 1);
    tDigest.add(2.5, 2);
    Int2LongOpenHashMap int2LongMap = new Int2LongOpenHashMap();
    int2LongMap.put(1, 2L);
    int2LongMap.put(2, 1L);
    Long2LongOpenHashMap long2LongMap = new Long2LongOpenHashMap();
    long2LongMap.put(1L, 2L);
    long2LongMap.put(2L, 1L);
    Float2LongOpenHashMap float2LongMap = new Float2LongOpenHashMap();
    float2LongMap.put(1.5f, 2L);
    float2LongMap.put(2.5f, 1L);
    Double2LongOpenHashMap double2LongMap = new Double2LongOpenHashMap();
    double2LongMap.put(1.5, 2L);
    double2LongMap.put(2.5, 1L);
    Object2LongOpenHashMap<String> string2LongMap = new Object2LongOpenHashMap<>();
    string2LongMap.put("a", 2L);
    string2LongMap.put("b", 1L);
    IdSet idSet = IdSets.create(DataType.INT);
    idSet.add(1);
    idSet.add(2);
    PriorityQueue<FunnelStepEvent> funnelStepEvents =
        new PriorityQueue<>(List.of(new FunnelStepEvent(1L, 0), new FunnelStepEvent(2L, 1)));
    return new Object[][]{
        {new ArrayAggIntFunction(COLUMN, DataType.INT, false), new IntArrayList(new int[]{1, -2, 7}), 42},
        {new ArrayAggBigDecimalFunction(COLUMN, false),
            new ObjectArrayList<>(new BigDecimal[]{new BigDecimal("1.5"), new BigDecimal("-2")}), 52},
        {new ArrayAggStringFunction(COLUMN, false), new ObjectArrayList<>(new String[]{"a", "bc"}), 45},
        {new ArrayAggBytesFunction(COLUMN, DataType.BYTES, false),
            new ObjectArrayList<>(new ByteArray[]{new ByteArray(new byte[]{1, 2}), new ByteArray(new byte[0])}), 54},
        {new ListAggFunction(COLUMN, ",", false), new ObjectArrayList<>(new String[]{"a", "bc"}), 45},
        {new ListAggDistinctFunction(COLUMN, ",", false), new ObjectLinkedOpenHashSet<>(new String[]{"b", "a"}), 50},
        {new ArrayAggLongFunction(COLUMN, DataType.LONG, false), new LongArrayList(new long[]{1, Long.MIN_VALUE}), 43},
        {new ArrayAggFloatFunction(COLUMN, false), new FloatArrayList(new float[]{1.5f, -0.0f}), 44},
        {new ArrayAggDoubleFunction(COLUMN, false), new DoubleArrayList(new double[]{-0.0, 1e300}), 3},
        {new ArrayAggDistinctIntFunction(COLUMN, DataType.INT, false), new IntOpenHashSet(new int[]{7}), 9},
        {new ArrayAggDistinctLongFunction(COLUMN, DataType.LONG, false), new LongOpenHashSet(new long[]{7}), 15},
        {new ArrayAggDistinctFloatFunction(COLUMN, false), new FloatOpenHashSet(new float[]{1.5f}), 16},
        {new ArrayAggDistinctDoubleFunction(COLUMN, false), new DoubleOpenHashSet(new double[]{2.5}), 17},
        {new ArrayAggDistinctBigDecimalFunction(COLUMN, false),
            new ObjectOpenHashSet<>(new BigDecimal[]{new BigDecimal("1.5")}), 53},
        {new ArrayAggDistinctStringFunction(COLUMN, false), new ObjectOpenHashSet<>(new String[]{"a"}), 18},
        {new ArrayAggDistinctBytesFunction(COLUMN, DataType.BYTES, false),
            new ObjectOpenHashSet<>(new ByteArray[]{new ByteArray(new byte[]{1})}), 19},
        {new SumArrayLongAggregationFunction(List.of(COLUMN), false), new LongArrayList(new long[]{3, 4}), 43},
        {new SumArrayDoubleAggregationFunction(List.of(COLUMN), false), new DoubleArrayList(new double[]{0.25, 8}), 3},
        {HISTOGRAM, new DoubleArrayList(new double[]{1, 2}), 3},
        {new PercentileAggregationFunction(COLUMN, 50, false), new DoubleArrayList(new double[]{-3.5, 0, 7}), 3},
        {PERCENTILE_TDIGEST, tDigest, 10},
        {PERCENTILE_RAW_TDIGEST, tDigest, 10},
        {PERCENTILE_SMART_TDIGEST, new DoubleArrayList(new double[]{1, 2}), 3},
        {PERCENTILE_SMART_TDIGEST, tDigest, 10},
        {DISTINCT_COUNT, new IntOpenHashSet(new int[]{1, 2}), 9},
        {DISTINCT_COUNT, new LongOpenHashSet(new long[]{1, 2}), 15},
        {DISTINCT_COUNT, new FloatOpenHashSet(new float[]{1.5f}), 16},
        {DISTINCT_COUNT, new DoubleOpenHashSet(new double[]{2.5}), 17},
        {DISTINCT_COUNT, new ObjectOpenHashSet<>(new BigDecimal[]{new BigDecimal("1.5")}), 53},
        {DISTINCT_COUNT, new ObjectOpenHashSet<>(new String[]{"a", "b"}), 18},
        {DISTINCT_COUNT, new ObjectOpenHashSet<>(new ByteArray[]{new ByteArray(new byte[]{1})}), 19},
        {DISTINCT_COUNT, new IntOpenHashSet(), 9},
        {DISTINCT_COUNT_BITMAP, sparseBitmap, 14},
        {DISTINCT_COUNT_BITMAP, denseBitmap, 14},
        {new DistinctCountHLLAggregationFunction(List.of(COLUMN), false), hyperLogLog, 6},
        {new FastHLLAggregationFunction(List.of(COLUMN), false), hyperLogLog, 6},
        {new DistinctCountHLLPlusAggregationFunction(List.of(COLUMN), false), hyperLogLogPlus, 40},
        {DISTINCT_COUNT_SMART_HLL, hyperLogLog, 6},
        {DISTINCT_COUNT_SMART_HLL, new IntOpenHashSet(new int[]{1, 2}), 9},
        {DISTINCT_COUNT_SMART_HLL_PLUS, hyperLogLogPlus, 40},
        {DISTINCT_COUNT_SMART_HLL_PLUS, new LongOpenHashSet(new long[]{1, 2}), 15},
        {new DistinctCountULLAggregationFunction(List.of(COLUMN), false), ultraLogLog, 46},
        {DISTINCT_COUNT_SMART_ULL, ultraLogLog, 46},
        {DISTINCT_COUNT_SMART_ULL, new IntOpenHashSet(new int[]{1, 2}), 9},
        {MODE, int2LongMap, 23},
        {MODE, long2LongMap, 24},
        {MODE, float2LongMap, 25},
        {MODE, double2LongMap, 26},
        {MODE_STRING, string2LongMap, 57},
        {FUNNEL_COUNT, List.of(RoaringBitmap.bitmapOf(1, 2), RoaringBitmap.bitmapOf(2)), 21},
        {FUNNEL_COUNT_SET, List.of(new IntOpenHashSet(new int[]{1, 2}), new IntOpenHashSet(new int[]{2})), 21},
        {FUNNEL_COUNT_SET, List.of(new LongOpenHashSet(new long[]{1, 2}), new LongOpenHashSet(new long[]{2})), 21},
        {FUNNEL_COUNT_SET, List.of(new FloatOpenHashSet(new float[]{1.5f}), new FloatOpenHashSet()), 21},
        {FUNNEL_COUNT_SET, List.of(new DoubleOpenHashSet(new double[]{2.5}), new DoubleOpenHashSet()), 21},
        {FUNNEL_COUNT_SET,
            List.of(new ObjectOpenHashSet<>(new String[]{"a", "b"}), new ObjectOpenHashSet<String>()), 21},
        {FUNNEL_COUNT_SET, List.of(new ObjectOpenHashSet<String>(), new ObjectOpenHashSet<String>()), 21},
        {funnelCount("theta_sketch"), List.of(thetaSketch(1, 2), thetaSketch(2)), 21},
        {funnelCount("partitioned"), LongArrayList.wrap(new long[]{2, 1}), 43},
        {funnelCount("partitioned"), List.of(0L, 0L), 21},
        {funnelCount("partitioned", "sorted"), LongArrayList.wrap(new long[]{2, 1}), 43},
        {funnelCount("partitioned", "sorted"), List.of(0L, 0L), 21},
        {FUNNEL_MAX_STEP, funnelStepEvents, 51},
        // Serialized eagerly into its payload bytes
        {new AvgAggregationFunction(List.of(COLUMN), false), new AvgPair(3.0, 2L), 4},
        {new MinMaxRangeAggregationFunction(List.of(COLUMN), false), new MinMaxRangePair(1.5, 2.5), 5},
        {new SumPrecisionAggregationFunction(List.of(COLUMN), false), new BigDecimal("1.5"), 22},
        {new StUnionAggregationFunction(List.of(COLUMN), false),
            GeometryUtils.GEOMETRY_FACTORY.createPoint(new Coordinate(1, 2)), 13},
        {new IdSetAggregationFunction(List.of(COLUMN), false), idSet, 20},
        {ANY_VALUE, null, 0},
        {ANY_VALUE, 5, 1},
        {ANY_VALUE, "a", 1},
        {new FirstIntValueWithTimeAggregationFunction(COLUMN, TIME_COLUMN, false, false), new IntLongPair(1, 2L), 27},
        {new FirstLongValueWithTimeAggregationFunction(COLUMN, TIME_COLUMN, false), new LongLongPair(1L, 2L), 28},
        {new FirstFloatValueWithTimeAggregationFunction(COLUMN, TIME_COLUMN, false), new FloatLongPair(1.5f, 2L), 29},
        {new FirstDoubleValueWithTimeAggregationFunction(COLUMN, TIME_COLUMN, false), new DoubleLongPair(1.5, 2L), 30},
        {new FirstBigDecimalValueWithTimeAggregationFunction(COLUMN, TIME_COLUMN, false),
            new BigDecimalLongPair(new BigDecimal("1.5"), 2L), 55},
        {new FirstStringValueWithTimeAggregationFunction(COLUMN, TIME_COLUMN, false), new StringLongPair("a", 2L), 31},
        {new FirstBytesValueWithTimeAggregationFunction(COLUMN, TIME_COLUMN, false, ColumnDataType.BYTES),
            new BytesLongPair(new ByteArray(new byte[]{1, 2}), 2L), 56},
        {new LastIntValueWithTimeAggregationFunction(COLUMN, TIME_COLUMN, false, false), new IntLongPair(1, 2L), 27},
        {new LastLongValueWithTimeAggregationFunction(COLUMN, TIME_COLUMN, false), new LongLongPair(1L, 2L), 28},
        {new LastFloatValueWithTimeAggregationFunction(COLUMN, TIME_COLUMN, false), new FloatLongPair(1.5f, 2L), 29},
        {new LastDoubleValueWithTimeAggregationFunction(COLUMN, TIME_COLUMN, false), new DoubleLongPair(1.5, 2L), 30},
        {new LastBigDecimalValueWithTimeAggregationFunction(COLUMN, TIME_COLUMN, false),
            new BigDecimalLongPair(new BigDecimal("1.5"), 2L), 55},
        {new LastStringValueWithTimeAggregationFunction(COLUMN, TIME_COLUMN, false), new StringLongPair("a", 2L), 31},
        {new LastBytesValueWithTimeAggregationFunction(COLUMN, TIME_COLUMN, false, ColumnDataType.BYTES),
            new BytesLongPair(new ByteArray(new byte[]{1, 2}), 2L), 56},
        {new CovarianceAggregationFunction(List.of(COLUMN, COLUMN_2), false, false),
            new CovarianceTuple(1.0, 2.0, 3.0, 4L), 32},
        {new VarianceAggregationFunction(List.of(COLUMN), false, false, false), new VarianceTuple(4L, 2.0, 1.0), 33},
        {new FourthMomentAggregationFunction(List.of(COLUMN), FourthMomentAggregationFunction.Type.KURTOSIS, false),
            fourthMoment(), 34},
        {new PercentileEstAggregationFunction(COLUMN, 50, false), quantileDigest(), 7},
        {new PercentileKLLAggregationFunction(List.of(COLUMN, ExpressionContext.forLiteral(Literal.doubleValue(50))),
            false), kllSketch(), 36},
        {new FrequentLongsSketchAggregationFunction(List.of(COLUMN), false), frequentLongsSketch(), 39},
        {new FrequentStringsSketchAggregationFunction(List.of(COLUMN), false), frequentStringsSketch(), 38},
        {new DistinctCountCPCSketchAggregationFunction(List.of(COLUMN), false), cpcSketchAccumulator(), 49},
        {new IntegerTupleSketchAggregationFunction(List.of(COLUMN), IntegerSummary.Mode.Sum, false),
            tupleIntSketchAccumulator(), 48},
        {new DistinctCountThetaSketchAggregationFunction(List.of(COLUMN), false), List.of(thetaSketchAccumulator()), 21}
    };
  }

  /// Each row is an aggregation function, an intermediate result of it, and the wire type value it serializes with.
  /// The type values are part of the wire format and never change.
  @Test(dataProvider = "intermediateResults")
  public void testSerializeIntoByteBuffer(AggregationFunction function, Object intermediateResult, int expectedType)
      throws IOException {
    SerializedResult expected = function.serializeIntermediateResult(intermediateResult);
    assertEquals(expected.getType(), expectedType);
    byte[] expectedBytes = expected.getBytes();
    byte[] padding = new byte[PADDING];
    Arrays.fill(padding, PADDING_BYTE);
    for (boolean direct : new boolean[]{false, true}) {
      SerializedResult serialized = function.serializeIntermediateResult(intermediateResult);
      assertEquals(serialized.getType(), expected.getType());
      assertEquals(serialized.getSize(), expectedBytes.length);
      ByteBuffer buffer = paddedBuffer(expectedBytes.length, direct);
      serialized.writeTo(buffer);
      int end = PADDING + expectedBytes.length;
      assertEquals(buffer.position(), end);
      assertEquals(buffer.limit(), end);
      assertSame(buffer.order(), ByteOrder.BIG_ENDIAN);
      byte[] contents = new byte[buffer.capacity()];
      buffer.clear();
      buffer.get(contents);
      assertEquals(Arrays.copyOfRange(contents, 0, PADDING), padding);
      assertEquals(Arrays.copyOfRange(contents, PADDING, end), expectedBytes);
      assertEquals(Arrays.copyOfRange(contents, end, contents.length), padding);
    }
    // Small pages, so the payload spans page boundaries
    try (PagedPinotOutputStream output =
        new PagedPinotOutputStream(new PagedPinotOutputStream.HeapPageAllocator(100))) {
      function.serializeIntermediateResult(intermediateResult).writeTo(output);
      assertEquals(output.getCurrentOffset(), expectedBytes.length);
      assertEquals(toBytes(output), expectedBytes);
    }
    assertEquals(function.serializeIntermediateResult(intermediateResult).getBytes(), expectedBytes);
    Object expectedFinalResult = comparableForm(function.extractFinalResult(intermediateResult));
    // Overwriting the source buffer after deserializing leaves the final result unchanged
    byte[] source = expectedBytes.clone();
    Object deserialized =
        function.deserializeIntermediateResult(new CustomObject(expectedType, ByteBuffer.wrap(source)));
    Arrays.fill(source, PADDING_BYTE);
    assertEquals(comparableForm(function.extractFinalResult(deserialized)), expectedFinalResult);
    ByteBuffer offsetBuffer = paddedBuffer(expectedBytes.length, false);
    offsetBuffer.put(expectedBytes).position(PADDING);
    Object deserializedFromOffset =
        function.deserializeIntermediateResult(new CustomObject(expectedType, offsetBuffer));
    assertEquals(comparableForm(function.extractFinalResult(deserializedFromOffset)), expectedFinalResult);
  }

  /// The parent `EXPRMIN` / `EXPRMAX` function, checked on its bytes since [ExprMinMaxObject] has no value equality:
  /// its wire type value, and a deserialized object that serializes to the same bytes after its source buffer is
  /// overwritten.
  @Test
  public void testParentExprMinMax()
      throws IOException {
    ParentExprMinMaxAggregationFunction function = new ParentExprMinMaxAggregationFunction(
        List.of(ExpressionContext.forLiteral(Literal.stringValue("0")),
            ExpressionContext.forLiteral(Literal.intValue(1)), COLUMN, COLUMN_2), false, false);
    List<Object[]> rows = List.of(new Object[]{5, "a1"}, new Object[]{5, "a2"}, new Object[]{3, "b1"});
    List<ExprMinMaxMeasuringValSetWrapper> measuring =
        List.of(new ExprMinMaxMeasuringValSetWrapper(new RowBasedBlockValSet(ColumnDataType.INT, rows, 0, false)));
    List<ExprMinMaxProjectionValSetWrapper> projection =
        List.of(new ExprMinMaxProjectionValSetWrapper(new RowBasedBlockValSet(ColumnDataType.STRING, rows, 1, false)));
    ExprMinMaxObject object =
        new ExprMinMaxObject(new DataSchema(new String[]{"measure"}, new ColumnDataType[]{ColumnDataType.INT}),
            new DataSchema(new String[]{"projection"}, new ColumnDataType[]{ColumnDataType.STRING}));
    for (int i = 0; i < rows.size(); i++) {
      int result = object.compareAndSetKey(measuring, i, true);
      if (result > 0) {
        object.setToNewVal(projection, i);
      } else if (result == 0) {
        object.addVal(projection, i);
      }
    }
    SerializedResult serialized = function.serializeIntermediateResult(object);
    assertEquals(serialized.getType(), 35);
    byte[] expectedBytes = serialized.getBytes();
    byte[] source = expectedBytes.clone();
    ExprMinMaxObject deserialized =
        function.deserializeIntermediateResult(new CustomObject(35, ByteBuffer.wrap(source)));
    Arrays.fill(source, PADDING_BYTE);
    assertEquals(function.serializeIntermediateResult(deserialized).getBytes(), expectedBytes);
  }

  /// Returns a final result in a form that compares by value. The serialized sketches have no `equals`, so they
  /// compare by their string form, which is what the broker returns.
  private static Object comparableForm(Object finalResult) {
    if (finalResult instanceof SerializedTDigest
        || finalResult instanceof SerializedFrequentLongsSketch
        || finalResult instanceof SerializedFrequentStringsSketch) {
      return finalResult.toString();
    }
    return finalResult;
  }

  private static byte[] toBytes(PagedPinotOutputStream output) {
    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    for (ByteBuffer page : output.getPages()) {
      byte[] pageBytes = new byte[page.remaining()];
      page.get(pageBytes);
      bytes.writeBytes(pageBytes);
    }
    return bytes.toByteArray();
  }

  /// Returns a heap or direct buffer positioned for `size` bytes, with [#PADDING] bytes of [#PADDING_BYTE] on either
  /// side of them.
  private static ByteBuffer paddedBuffer(int size, boolean direct) {
    int capacity = size + 2 * PADDING;
    ByteBuffer buffer = direct ? ByteBuffer.allocateDirect(capacity) : ByteBuffer.allocate(capacity);
    while (buffer.hasRemaining()) {
      buffer.put(PADDING_BYTE);
    }
    buffer.clear().position(PADDING).limit(PADDING + size);
    return buffer;
  }
}
