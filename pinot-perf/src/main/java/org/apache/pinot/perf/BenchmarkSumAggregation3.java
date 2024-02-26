package org.apache.pinot.perf;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.core.common.BlockValSet;
import org.apache.pinot.core.plan.DocIdSetPlanNode;
import org.apache.pinot.core.query.aggregation.AggregationResultHolder;
import org.apache.pinot.core.query.aggregation.DoubleAggregationResultHolder;
import org.apache.pinot.core.query.aggregation.function.AggregationFunction;
import org.apache.pinot.core.query.aggregation.function.SumAggregationFunction;
import org.apache.pinot.core.query.aggregation.function.SumAggregationFunctionFoldDouble;
import org.apache.pinot.core.query.aggregation.function.SumAggregationFunctionFoldHolder;
import org.apache.pinot.core.query.aggregation.function.SumAggregationFunctionFoldPrimitive;
import org.apache.pinot.segment.spi.index.reader.Dictionary;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.jetbrains.annotations.Nullable;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.profile.LinuxPerfAsmProfiler;
import org.openjdk.jmh.profile.LinuxPerfNormProfiler;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.roaringbitmap.RoaringBitmap;


@Fork(1)
@BenchmarkMode(Mode.Throughput)
@Warmup(iterations = 50, time = 100, timeUnit = TimeUnit.MILLISECONDS)
@Measurement(iterations = 50, time = 100, timeUnit = TimeUnit.MILLISECONDS)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
public class BenchmarkSumAggregation3 {
  public static void main(String[] args)
      throws RunnerException {
    Options opt = new OptionsBuilder().include(BenchmarkSumAggregation3.class.getSimpleName())
//                .addProfiler(LinuxPerfAsmProfiler.class)
//                .addProfiler(LinuxPerfNormProfiler.class)
        .build();

    new Runner(opt).run();
  }

  private static final ExpressionContext EXPR = ExpressionContext.forIdentifier("col");

  @Param({"100", "50", "0"})
  public int _nullHandlingEnabledPerCent;
  @Param({"2", "4", "8", "16", "32", "64", "128"})
  public int _aNullInterval;
  /**
   * While with {@link #_aNullInterval} we control how often null values are going to be stored, with
   * {@link #_randomNull} whether the null value is the first value in the interval (when the attribute is false) or
   * whether it is in a random position inside the interval (when the attribute is true).
   *
   * Using a false value may produce incorrect results, given the periodic nature of the null distribution may affect
   * the CPU branch algorithm.
   */
  public boolean _randomNull = true;
  @Param({"normal", "foldDouble"})
//  @Param({"normal", "foldDouble", "foldHolder", "foldPrimitive"})
  public String _zImpl;

  private AggregationFunction _aggregationFunction;
  private AggregationResultHolder _resultHolder;
  private RoaringBitmap _nullBitmap;
  private long[] _values;
  private Map<ExpressionContext, BlockValSet> _blockValSetMap;
  private double _expectedNotNullSum;
  private double _expectedNullSum;
  private double _nextExpectedSum;
  private Random _segmentNullRandomGenerator = new Random(42);
  private Random _callRandomGenerator = new Random(42);

  @Setup(Level.Iteration)
  public void setupFunction() {
    boolean nullHandlingEnabled = _callRandomGenerator.nextInt(100) < _nullHandlingEnabledPerCent;

    if (nullHandlingEnabled) {
      _nextExpectedSum = _expectedNullSum;
    } else {
      _nextExpectedSum = _expectedNotNullSum;
    }

    switch (_zImpl) {
      case "normal":
        _aggregationFunction = new SumAggregationFunction(List.of(EXPR), nullHandlingEnabled);
        break;
      case "foldDouble":
        _aggregationFunction = new SumAggregationFunctionFoldDouble(List.of(EXPR), nullHandlingEnabled);
        break;
      case "foldPrimitive":
        _aggregationFunction = new SumAggregationFunctionFoldPrimitive(List.of(EXPR), nullHandlingEnabled);
        break;
      case "foldHolder":
        _aggregationFunction = new SumAggregationFunctionFoldHolder(List.of(EXPR), nullHandlingEnabled);
        break;
      default:
        throw new IllegalArgumentException("Unknown impl: " + _zImpl);
    }
    _resultHolder = _aggregationFunction.createAggregationResultHolder();
  }

  @Setup(Level.Trial)
  public void setup()
      throws IOException {
    _nullBitmap = createNullBitmap();
    _values = new long[DocIdSetPlanNode.MAX_DOC_PER_CALL];
    LongSupplier longSupplier = Distribution.createLongSupplier(42, "EXP(0.5)");
    for (int i = 0; i < DocIdSetPlanNode.MAX_DOC_PER_CALL; i++) {
      _values[i] = longSupplier.getAsLong();
    }
    _expectedNullSum = 0;
    _expectedNotNullSum = 0;
    for (int i = 0; i < DocIdSetPlanNode.MAX_DOC_PER_CALL; i++) {
      _expectedNotNullSum += _values[i];
      if (_nullBitmap == null || !_nullBitmap.contains(i)) {
        _expectedNullSum += _values[i];
      }
    }
    System.out.println("Expecting not null sum " + _expectedNotNullSum + " and null sum " + _expectedNullSum);
    _blockValSetMap = Map.of(EXPR, new BenchmarkBlockValSet(_nullBitmap, _values));
  }

  private RoaringBitmap createNullBitmap() {
    if (_nullHandlingEnabledPerCent > 0) {
      RoaringBitmap nullBitmap = new RoaringBitmap();

      if (_randomNull) {
        for (int i = 0; i < DocIdSetPlanNode.MAX_DOC_PER_CALL; i += _aNullInterval) {
          int randomValue = _segmentNullRandomGenerator.nextInt(_aNullInterval);
          nullBitmap.add(Math.min(i + randomValue, DocIdSetPlanNode.MAX_DOC_PER_CALL - 1));
        }
      } else {
        for (int i = 0; i < DocIdSetPlanNode.MAX_DOC_PER_CALL; i += _aNullInterval) {
          nullBitmap.add(i);
        }
      }
      return nullBitmap;
    } else {
      return null;
    }
  }

  private static class BenchmarkBlockValSet implements BlockValSet {

    final RoaringBitmap _nullBitmap;
    final long[] _values;

    private BenchmarkBlockValSet(@Nullable RoaringBitmap nullBitmap, long[] values) {
      _nullBitmap = nullBitmap;
      _values = values;
    }

    @Nullable
    @Override
    public RoaringBitmap getNullBitmap() {
      return _nullBitmap;
    }

    @Override
    public DataType getValueType() {
      return DataType.LONG;
    }

    @Override
    public boolean isSingleValue() {
      return true;
    }

    @Nullable
    @Override
    public Dictionary getDictionary() {
      throw new UnsupportedOperationException();
    }

    @Override
    public int[] getDictionaryIdsSV() {
      throw new UnsupportedOperationException();
    }

    @Override
    public int[] getIntValuesSV() {
      throw new UnsupportedOperationException();
    }

    @Override
    public long[] getLongValuesSV() {
      return _values;
    }

    @Override
    public float[] getFloatValuesSV() {
      throw new UnsupportedOperationException();
    }

    @Override
    public double[] getDoubleValuesSV() {
      throw new UnsupportedOperationException();
    }

    @Override
    public BigDecimal[] getBigDecimalValuesSV() {
      throw new UnsupportedOperationException();
    }

    @Override
    public String[] getStringValuesSV() {
      throw new UnsupportedOperationException();
    }

    @Override
    public byte[][] getBytesValuesSV() {
      throw new UnsupportedOperationException();
    }

    @Override
    public int[][] getDictionaryIdsMV() {
      throw new UnsupportedOperationException();
    }

    @Override
    public int[][] getIntValuesMV() {
      throw new UnsupportedOperationException();
    }

    @Override
    public long[][] getLongValuesMV() {
      throw new UnsupportedOperationException();
    }

    @Override
    public float[][] getFloatValuesMV() {
      throw new UnsupportedOperationException();
    }

    @Override
    public double[][] getDoubleValuesMV() {
      throw new UnsupportedOperationException();
    }

    @Override
    public String[][] getStringValuesMV() {
      throw new UnsupportedOperationException();
    }

    @Override
    public byte[][][] getBytesValuesMV() {
      throw new UnsupportedOperationException();
    }

    @Override
    public int[] getNumMVEntries() {
      throw new UnsupportedOperationException();
    }
  }

  @Benchmark
  public void test(Blackhole bh) {
    _resultHolder.setValue(0d);
    _aggregationFunction.aggregate(DocIdSetPlanNode.MAX_DOC_PER_CALL, _resultHolder, _blockValSetMap);
    if (_resultHolder instanceof DoubleAggregationResultHolder) {
      double result = _resultHolder.getDoubleResult();
      if (result != _nextExpectedSum) {
        throw new IllegalStateException("Expected: " + _nextExpectedSum + ", got: " + result);
      }
      bh.consume(result);
    } else {
      Double result = _resultHolder.getResult();
      if (result != _nextExpectedSum) {
        throw new IllegalStateException("Expected: " + _nextExpectedSum + ", got: " + _resultHolder.getResult());
      }
      bh.consume(result);
    }
  }
}