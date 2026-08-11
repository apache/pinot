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
package org.apache.pinot.perf.aggregation;

import com.tdunning.math.stats.Centroid;
import com.tdunning.math.stats.MergingDigest;
import com.tdunning.math.stats.TDigest;
import java.nio.ByteBuffer;
import java.security.CodeSource;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.SplittableRandom;
import java.util.StringJoiner;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.pinot.common.CustomObject;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.core.common.ObjectSerDeUtils;
import org.apache.pinot.core.data.table.ConcurrentIndexedTable;
import org.apache.pinot.core.data.table.IndexedTable;
import org.apache.pinot.core.data.table.Key;
import org.apache.pinot.core.data.table.Record;
import org.apache.pinot.core.query.aggregation.function.AggregationFunction;
import org.apache.pinot.core.query.aggregation.function.AggregationFunction.SerializedIntermediateResult;
import org.apache.pinot.core.query.aggregation.function.PercentileTDigestAggregationFunction;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.core.query.request.context.utils.QueryContextConverterUtils;
import org.openjdk.jmh.annotations.AuxCounters;
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
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.BenchmarkParams;

/// Measures the TDigest reducer kernel and the complete [ConcurrentIndexedTable] group-by combine path.
///
/// Source digests and the exact raw-value oracle are built once per trial. Each invocation receives fresh merge
/// targets so a previous invocation cannot change its input. Accuracy and centroid statistics come from an untimed
/// quality sweep: 32 deterministic independent source orders for `RANDOMIZED`, and one order for fixed-order modes.
/// Run with JMH's GC profiler to report allocation and GC metrics, for example `-prof gc`. Error counters report
/// absolute error in billionths because all generated distributions are bounded to `[0, 1]`.
///
/// The TDigest dependency version is deliberately a build-level dimension. Compare identical `PAIRWISE` runs from a
/// 3.2 build and a 3.3 build. Use `_sourceLayout=FIXED_VERBOSE` to hold the serialized centroid shape constant for the
/// attributable pairwise experiment; the default `NATIVE` mode includes version-specific source compression.
/// `SINGLETON_LIST` is accepted as a command-line JMH parameter for the 3.3 experiment, but is excluded from the
/// default matrix and rejected on TDigest 3.2 because that version corrupts non-empty targets in `add(List)`.
/// `PROMOTED_LOCAL` measures promotion of raw group-by [MergingDigest] results. `ACCUMULATOR_LOCAL` models
/// materialized accumulator results from non-grouped and StarTree segment execution. `ACCUMULATOR_WIRE` keeps
/// deserialized sources lazy to model broker reduction. Use `_sourceReuse=UNIQUE` for production-shaped runs so
/// every group and metric has distinct prebuilt source state; the default shared corpus keeps the full matrix small.
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 2, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(value = 2, jvmArgsAppend = {"-Xms4g", "-Xmx8g"})
@Threads(1)
@State(Scope.Thread)
public class BenchmarkPercentileTDigestCombine {
  private static final int VALUES_PER_DIGEST = 10_000;
  private static final int QUALITY_SAMPLES = 32;
  private static final long QUALITY_ORDER_SEED = 0x3C6EF372FE94F82BL;
  private static final double ERROR_SCALE = 1_000_000_000.0;
  private static final Pattern TDIGEST_JAR_VERSION = Pattern.compile("t-digest-(\\d+)\\.(\\d+)(?:\\.[^/]*)?\\.jar");

  public enum Distribution {
    UNIFORM,
    SKEWED,
    BIMODAL,
    DUPLICATE_HEAVY
  }

  public enum MergeOrder {
    ORIGINAL,
    REVERSED,
    RANDOMIZED
  }

  public enum SourceLayout {
    NATIVE,
    FIXED_VERBOSE
  }

  public enum SourceReuse {
    SHARED,
    UNIQUE
  }

  public enum Implementation {
    PAIRWISE(0),
    PROMOTED_LOCAL(0),
    ACCUMULATOR_LOCAL(0),
    ACCUMULATOR_WIRE(0),
    SINGLETON_LIST(1),
    BATCH_8(8),
    BATCH_16(16),
    BATCH_32(32);

    private final int _batchSize;

    Implementation(int batchSize) {
      _batchSize = batchSize;
    }

    private boolean isBatching() {
      return _batchSize >= 8;
    }
  }

  @Param({"1", "1440"})
  private int _numGroups;

  @Param({"1", "7"})
  private int _numMetrics;

  @Param({"8", "32", "128"})
  private int _fanIn;

  @Param({"50", "100", "200"})
  private int _compression;

  @Param
  private Distribution _distribution;

  @Param
  private MergeOrder _mergeOrder;

  @Param({"NATIVE"})
  private SourceLayout _sourceLayout;

  @Param({"SHARED"})
  private SourceReuse _sourceReuse;

  @Param({"PAIRWISE", "PROMOTED_LOCAL", "ACCUMULATOR_LOCAL", "ACCUMULATOR_WIRE"})
  private Implementation _implementation;

  private ReducerAggregationFunction[] _functions;
  private TDigest[] _sources;
  private byte[][] _serializedSources;
  private TDigest[] _firstSources;
  private int[] _sourceOrder;
  private double _averageInputCentroidCount;
  private double _expectedP75;
  private double _expectedP95;
  private double _expectedP99;
  private QueryContext _queryContext;
  private DataSchema _dataSchema;
  private ExecutorService _executorService;
  private QualityStats _qualityStats;

  @Setup(Level.Trial)
  public void setUpTrial() {
    rejectUnsafeSingletonListMerge();
    buildSourcesAndOracle();
    buildMergeOrder();
    buildQuery();
    _executorService = Executors.newSingleThreadExecutor();
    buildQualityStats();
  }

  @Setup(Level.Invocation)
  public void setUpInvocation() {
    prepareFirstSources();
  }

  @TearDown(Level.Trial)
  public void tearDownTrial() {
    if (_executorService != null) {
      _executorService.shutdownNow();
    }
  }

  @Benchmark
  public TDigest[] mergeKernel(QualityCounters counters) {
    resetPending();
    TDigest[] targets = new TDigest[_numGroups * _numMetrics];
    for (int targetIndex = 0; targetIndex < targets.length; targetIndex++) {
      targets[targetIndex] = _firstSources[targetIndex];
    }
    for (int position = 1; position < _fanIn; position++) {
      for (int groupId = 0; groupId < _numGroups; groupId++) {
        int targetOffset = groupId * _numMetrics;
        for (int metricId = 0; metricId < _numMetrics; metricId++) {
          int targetIndex = targetOffset + metricId;
          targets[targetIndex] = _functions[metricId].merge(targets[targetIndex],
              _sources[sourceSlot(position, targetIndex)]);
        }
      }
    }
    flushPending();
    counters.record(_qualityStats);
    return targets;
  }

  @Benchmark
  public IndexedTable combineIndexedTable(QualityCounters counters) {
    IndexedTable table = buildIndexedTable(false);
    counters.record(_qualityStats);
    return table;
  }

  @Benchmark
  public IndexedTable combineIndexedTableAndExtract(QualityCounters counters) {
    IndexedTable table = buildIndexedTable(true);
    counters.record(_qualityStats);
    return table;
  }

  private IndexedTable buildIndexedTable(boolean extractFinalResult) {
    resetPending();
    DataSchema dataSchema = extractFinalResult
        ? new DataSchema(_dataSchema.getColumnNames().clone(), _dataSchema.getColumnDataTypes().clone()) : _dataSchema;
    IndexedTable table = new ConcurrentIndexedTable(dataSchema, false, _queryContext, _numGroups,
        Integer.MAX_VALUE, Integer.MAX_VALUE, _numGroups, _executorService);
    for (int groupId = 0; groupId < _numGroups; groupId++) {
      Object[] values = new Object[_numMetrics + 1];
      values[0] = groupId;
      for (int metricId = 0; metricId < _numMetrics; metricId++) {
        int targetIndex = groupId * _numMetrics + metricId;
        values[metricId + 1] = _firstSources[targetIndex];
      }
      table.upsert(new Key(new Object[]{groupId}), new Record(values));
    }
    for (int position = 1; position < _fanIn; position++) {
      for (int groupId = 0; groupId < _numGroups; groupId++) {
        Object[] values = new Object[_numMetrics + 1];
        values[0] = groupId;
        for (int metricId = 0; metricId < _numMetrics; metricId++) {
          int targetIndex = groupId * _numMetrics + metricId;
          values[metricId + 1] = _sources[sourceSlot(position, targetIndex)];
        }
        table.upsert(new Key(new Object[]{groupId}), new Record(values));
      }
    }
    flushPending();
    table.finish(false, extractFinalResult);
    return table;
  }

  private void buildSourcesAndOracle() {
    _sources = new TDigest[_fanIn];
    _serializedSources = new byte[_fanIn][];
    double[] rawValues = new double[_fanIn * VALUES_PER_DIGEST];
    int totalInputCentroids = 0;
    for (int sourceId = 0; sourceId < _fanIn; sourceId++) {
      ImmutableMergingDigest nativeDigest = null;
      if (_sourceLayout == SourceLayout.NATIVE) {
        nativeDigest = new ImmutableMergingDigest(_compression);
        TDigestBenchmarkUtils.usePinotScaleFunction(nativeDigest);
      }
      double[] sourceValues = new double[VALUES_PER_DIGEST];
      SplittableRandom random = new SplittableRandom(0x6A09E667F3BCC909L + sourceId);
      int valueOffset = sourceId * VALUES_PER_DIGEST;
      for (int valueId = 0; valueId < VALUES_PER_DIGEST; valueId++) {
        double value = nextValue(random, valueId);
        if (nativeDigest != null) {
          nativeDigest.add(value);
        }
        sourceValues[valueId] = value;
        rawValues[valueOffset + valueId] = value;
      }
      TDigest digest;
      byte[] fixedBytes = null;
      if (nativeDigest != null) {
        nativeDigest.freeze();
        digest = nativeDigest;
      } else {
        Arrays.sort(sourceValues);
        FixedTDigest fixedDigest = new FixedTDigest(sourceValues, _compression);
        digest = fixedDigest;
        fixedBytes = fixedDigest.bytes();
      }
      if (isAccumulatorSource()) {
        PercentileTDigestAggregationFunction function = new PercentileTDigestAggregationFunction(
            ExpressionContext.forIdentifier("metric"), 75.0, _compression, false);
        SerializedIntermediateResult serialized = function.serializeIntermediateResult(digest);
        byte[] bytes = serialized.getBytes();
        _serializedSources[sourceId] = bytes;
        digest = function.deserializeIntermediateResult(
            new CustomObject(serialized.getType(), ByteBuffer.wrap(bytes)));
        if (_implementation == Implementation.ACCUMULATOR_LOCAL) {
          digest.compress();
        }
      } else {
        _serializedSources[sourceId] = fixedBytes != null ? fixedBytes
            : ObjectSerDeUtils.TDIGEST_SER_DE.serialize(digest);
      }
      _sources[sourceId] = digest;
      totalInputCentroids += digest.centroidCount();
    }
    _averageInputCentroidCount = (double) totalInputCentroids / _fanIn;
    Arrays.sort(rawValues);
    _expectedP75 = exactQuantile(rawValues, 0.75);
    _expectedP95 = exactQuantile(rawValues, 0.95);
    _expectedP99 = exactQuantile(rawValues, 0.99);
    expandSources();
  }

  private void expandSources() {
    if (_sourceReuse == SourceReuse.SHARED) {
      return;
    }
    int numTargets = _numGroups * _numMetrics;
    byte[][] baseSerializedSources = _serializedSources;
    _sources = new TDigest[_fanIn * numTargets];
    _serializedSources = new byte[_sources.length][];
    PercentileTDigestAggregationFunction function = new PercentileTDigestAggregationFunction(
        ExpressionContext.forIdentifier("metric"), 75.0, _compression, false);
    for (int sourceId = 0; sourceId < _fanIn; sourceId++) {
      byte[] sourceBytes = baseSerializedSources[sourceId];
      for (int targetIndex = 0; targetIndex < numTargets; targetIndex++) {
        int expandedIndex = sourceId * numTargets + targetIndex;
        byte[] bytes = sourceBytes.clone();
        if (isAccumulatorSource()) {
          _serializedSources[expandedIndex] = bytes;
          TDigest source = function.deserializeIntermediateResult(
              new CustomObject(ObjectSerDeUtils.ObjectType.TDigest.getValue(), ByteBuffer.wrap(bytes)));
          if (_implementation == Implementation.ACCUMULATOR_LOCAL) {
            source.compress();
          }
          _sources[expandedIndex] = source;
        } else {
          _serializedSources[expandedIndex] = bytes;
          _sources[expandedIndex] = _sourceLayout == SourceLayout.FIXED_VERBOSE ? new FixedTDigest(bytes)
              : ObjectSerDeUtils.TDIGEST_SER_DE.deserialize(bytes);
        }
      }
    }
  }

  private void buildMergeOrder() {
    _sourceOrder = new int[_fanIn];
    for (int i = 0; i < _fanIn; i++) {
      _sourceOrder[i] = i;
    }
    if (_mergeOrder == MergeOrder.REVERSED) {
      for (int left = 0, right = _fanIn - 1; left < right; left++, right--) {
        int value = _sourceOrder[left];
        _sourceOrder[left] = _sourceOrder[right];
        _sourceOrder[right] = value;
      }
    } else if (_mergeOrder == MergeOrder.RANDOMIZED) {
      SplittableRandom random = new SplittableRandom(0xBB67AE8584CAA73BL);
      for (int i = _fanIn - 1; i > 0; i--) {
        int other = random.nextInt(i + 1);
        int value = _sourceOrder[i];
        _sourceOrder[i] = _sourceOrder[other];
        _sourceOrder[other] = value;
      }
    }
  }

  private void buildQuery() {
    StringJoiner selectExpressions = new StringJoiner(", ");
    String[] columnNames = new String[_numMetrics + 1];
    ColumnDataType[] columnDataTypes = new ColumnDataType[_numMetrics + 1];
    columnNames[0] = "groupKey";
    columnDataTypes[0] = ColumnDataType.INT;
    for (int metricId = 0; metricId < _numMetrics; metricId++) {
      String metricName = "metric" + metricId;
      selectExpressions.add("PERCENTILETDIGEST(" + metricName + ", 75, " + _compression + ")");
      columnNames[metricId + 1] = metricName;
      columnDataTypes[metricId + 1] = ColumnDataType.OBJECT;
    }
    _queryContext = QueryContextConverterUtils.getQueryContext(
        "SELECT " + selectExpressions + " FROM testTable GROUP BY groupKey");
    AggregationFunction[] aggregationFunctions = _queryContext.getAggregationFunctions();
    if (aggregationFunctions == null || aggregationFunctions.length != _numMetrics) {
      throw new IllegalStateException("Unexpected aggregation functions in benchmark query");
    }
    _functions = new ReducerAggregationFunction[_numMetrics];
    for (int metricId = 0; metricId < _numMetrics; metricId++) {
      ReducerAggregationFunction function = new ReducerAggregationFunction(
          ExpressionContext.forIdentifier("metric" + metricId), _compression, _implementation);
      _functions[metricId] = function;
      aggregationFunctions[metricId] = function;
    }
    _dataSchema = new DataSchema(columnNames, columnDataTypes);
  }

  private void buildQualityStats() {
    double totalResultingCentroids = 0.0;
    double maxResultingCentroids = 0.0;
    double totalP75Error = 0.0;
    double maxP75Error = 0.0;
    double totalP95Error = 0.0;
    double maxP95Error = 0.0;
    double totalP99Error = 0.0;
    double maxP99Error = 0.0;
    int qualitySamples = _mergeOrder == MergeOrder.RANDOMIZED ? QUALITY_SAMPLES : 1;
    SplittableRandom qualityOrderRandom = new SplittableRandom(QUALITY_ORDER_SEED);
    for (int sampleId = 0; sampleId < qualitySamples; sampleId++) {
      int[] sourceOrder =
          _mergeOrder == MergeOrder.RANDOMIZED ? buildRandomizedSourceOrder(qualityOrderRandom.split()) : _sourceOrder;
      resetPending();
      TDigest digest = newFirstSource(sourceSlot(sourceOrder, 0, 0), 0);
      for (int position = 1; position < _fanIn; position++) {
        digest = _functions[0].merge(digest, _sources[sourceSlot(sourceOrder, position, 0)]);
      }
      flushPending();
      digest.compress();
      validateDigest(digest);
      double p75Error = Math.abs(digest.quantile(0.75) - _expectedP75) * ERROR_SCALE;
      double p95Error = Math.abs(digest.quantile(0.95) - _expectedP95) * ERROR_SCALE;
      double p99Error = Math.abs(digest.quantile(0.99) - _expectedP99) * ERROR_SCALE;
      double resultingCentroids = digest.centroidCount();
      totalResultingCentroids += resultingCentroids;
      maxResultingCentroids = Math.max(maxResultingCentroids, resultingCentroids);
      totalP75Error += p75Error;
      maxP75Error = Math.max(maxP75Error, p75Error);
      totalP95Error += p95Error;
      maxP95Error = Math.max(maxP95Error, p95Error);
      totalP99Error += p99Error;
      maxP99Error = Math.max(maxP99Error, p99Error);
    }
    _qualityStats = new QualityStats(_averageInputCentroidCount, totalResultingCentroids / qualitySamples,
        maxResultingCentroids, totalP75Error / qualitySamples, maxP75Error, totalP95Error / qualitySamples,
        maxP95Error, totalP99Error / qualitySamples, maxP99Error);
    prepareFirstSources();
    validateIndexedTable(buildIndexedTable(false));
  }

  private int[] buildRandomizedSourceOrder(SplittableRandom random) {
    int[] sourceOrder = new int[_fanIn];
    for (int i = 0; i < _fanIn; i++) {
      sourceOrder[i] = i;
    }
    for (int i = _fanIn - 1; i > 0; i--) {
      int other = random.nextInt(i + 1);
      int value = sourceOrder[i];
      sourceOrder[i] = sourceOrder[other];
      sourceOrder[other] = value;
    }
    return sourceOrder;
  }

  private void validateIndexedTable(IndexedTable table) {
    if (table.size() != _numGroups) {
      throw new IllegalStateException("Unexpected IndexedTable size: " + table.size() + ", expected: " + _numGroups);
    }
    Iterator<Record> iterator = table.iterator();
    while (iterator.hasNext()) {
      Record record = iterator.next();
      Object[] values = record.getValues();
      for (int metricId = 0; metricId < _numMetrics; metricId++) {
        validateDigest((TDigest) values[metricId + 1]);
      }
    }
  }

  private void prepareFirstSources() {
    int numTargets = _numGroups * _numMetrics;
    _firstSources = new TDigest[numTargets];
    for (int targetIndex = 0; targetIndex < numTargets; targetIndex++) {
      _firstSources[targetIndex] = newFirstSource(sourceSlot(0, targetIndex), targetIndex % _numMetrics);
    }
  }

  private TDigest newFirstSource(int sourceIndex, int metricId) {
    if (isAccumulatorSource()) {
      byte[] bytes = _serializedSources[sourceIndex];
      TDigest target = _functions[metricId].deserializeIntermediateResult(
          new CustomObject(ObjectSerDeUtils.ObjectType.TDigest.getValue(), ByteBuffer.wrap(bytes)));
      if (_implementation == Implementation.ACCUMULATOR_LOCAL) {
        target.compress();
      }
      return target;
    }
    return ObjectSerDeUtils.TDIGEST_SER_DE.deserialize(_serializedSources[sourceIndex]);
  }

  private boolean isAccumulatorSource() {
    return _implementation == Implementation.ACCUMULATOR_LOCAL
        || _implementation == Implementation.ACCUMULATOR_WIRE;
  }

  private int sourceSlot(int position, int targetIndex) {
    return sourceSlot(_sourceOrder, position, targetIndex);
  }

  private int sourceSlot(int[] sourceOrder, int position, int targetIndex) {
    int sourceId = sourceOrder[position];
    return _sourceReuse == SourceReuse.SHARED ? sourceId : sourceId * _numGroups * _numMetrics + targetIndex;
  }

  private void validateDigest(TDigest digest) {
    long expectedWeight = expectedWeight();
    if (digest.size() != expectedWeight) {
      throw new IllegalStateException(
          "Unexpected TDigest weight: " + digest.size() + ", expected: " + expectedWeight);
    }
    long centroidWeight = 0L;
    double previousMean = Double.NEGATIVE_INFINITY;
    for (Centroid centroid : digest.centroids()) {
      if (!Double.isFinite(centroid.mean()) || centroid.count() <= 0 || centroid.mean() + 1e-12 < previousMean) {
        throw new IllegalStateException("Invalid centroid: previousMean=" + previousMean + ", mean=" + centroid.mean()
            + ", weight=" + centroid.count());
      }
      centroidWeight += centroid.count();
      previousMean = centroid.mean();
    }
    if (centroidWeight != expectedWeight) {
      throw new IllegalStateException(
          "Unexpected centroid weight: " + centroidWeight + ", expected: " + expectedWeight);
    }
    double[] quantiles = {digest.quantile(0.0), digest.quantile(0.5), digest.quantile(0.75),
        digest.quantile(0.95), digest.quantile(0.99), digest.quantile(1.0)};
    for (int i = 0; i < quantiles.length; i++) {
      if (!Double.isFinite(quantiles[i]) || (i > 0 && quantiles[i] + 1e-12 < quantiles[i - 1])) {
        throw new IllegalStateException("Invalid quantiles: " + Arrays.toString(quantiles));
      }
    }
  }

  private void flushPending() {
    for (ReducerAggregationFunction function : _functions) {
      function.flushAll();
    }
  }

  private void resetPending() {
    for (ReducerAggregationFunction function : _functions) {
      function.resetPending();
    }
  }

  private long expectedWeight() {
    return (long) _fanIn * VALUES_PER_DIGEST;
  }

  private double nextValue(SplittableRandom random, int valueId) {
    switch (_distribution) {
      case UNIFORM:
        return random.nextDouble();
      case SKEWED:
        return Math.pow(random.nextDouble(), 8.0);
      case BIMODAL:
        double mode = (valueId & 1) == 0 ? 0.2 : 0.8;
        double radius = Math.sqrt(-2.0 * Math.log(Math.max(Double.MIN_NORMAL, random.nextDouble())));
        double normal = radius * Math.cos(2.0 * Math.PI * random.nextDouble());
        return Math.max(0.0, Math.min(1.0, mode + normal * 0.04));
      case DUPLICATE_HEAVY:
        int bucket = random.nextInt(100);
        if (bucket < 80) {
          return 0.5;
        } else if (bucket < 90) {
          return 0.1;
        } else if (bucket < 98) {
          return 0.9;
        }
        return random.nextDouble();
      default:
        throw new IllegalStateException("Unsupported distribution: " + _distribution);
    }
  }

  private static double exactQuantile(double[] sortedValues, double quantile) {
    double index = quantile * (sortedValues.length - 1);
    int lower = (int) index;
    int upper = Math.min(lower + 1, sortedValues.length - 1);
    double fraction = index - lower;
    return sortedValues[lower] + fraction * (sortedValues[upper] - sortedValues[lower]);
  }

  private void rejectUnsafeSingletonListMerge() {
    if (_sourceLayout == SourceLayout.FIXED_VERBOSE
        && (_implementation == Implementation.SINGLETON_LIST || _implementation.isBatching())) {
      throw new IllegalArgumentException("FIXED_VERBOSE sources do not support list or batch merging");
    }
    if (_sourceReuse == SourceReuse.UNIQUE
        && (_implementation == Implementation.SINGLETON_LIST || _implementation.isBatching())) {
      throw new IllegalArgumentException("UNIQUE sources do not support list or batch merging");
    }
    if (_implementation != Implementation.SINGLETON_LIST) {
      return;
    }
    CodeSource codeSource = TDigest.class.getProtectionDomain().getCodeSource();
    String location = codeSource != null ? codeSource.getLocation().toString() : "";
    Matcher matcher = TDIGEST_JAR_VERSION.matcher(location);
    if (!matcher.find()) {
      throw new IllegalStateException("Cannot determine TDigest version from: " + location);
    }
    int major = Integer.parseInt(matcher.group(1));
    int minor = Integer.parseInt(matcher.group(2));
    if (major < 3 || (major == 3 && minor < 3)) {
      throw new IllegalArgumentException(
          "SINGLETON_LIST is unsafe on TDigest " + major + "." + minor + "; use TDigest 3.3 or newer");
    }
  }

  private static final class QualityStats {
    private final double _inputCentroidCount;
    private final double _resultingCentroidCount;
    private final double _maxResultingCentroidCount;
    private final double _p75ErrorPpb;
    private final double _maxP75ErrorPpb;
    private final double _p95ErrorPpb;
    private final double _maxP95ErrorPpb;
    private final double _p99ErrorPpb;
    private final double _maxP99ErrorPpb;

    private QualityStats(double inputCentroidCount, double resultingCentroidCount, double maxResultingCentroidCount,
        double p75ErrorPpb, double maxP75ErrorPpb, double p95ErrorPpb, double maxP95ErrorPpb, double p99ErrorPpb,
        double maxP99ErrorPpb) {
      _inputCentroidCount = inputCentroidCount;
      _resultingCentroidCount = resultingCentroidCount;
      _maxResultingCentroidCount = maxResultingCentroidCount;
      _p75ErrorPpb = p75ErrorPpb;
      _maxP75ErrorPpb = maxP75ErrorPpb;
      _p95ErrorPpb = p95ErrorPpb;
      _maxP95ErrorPpb = maxP95ErrorPpb;
      _p99ErrorPpb = p99ErrorPpb;
      _maxP99ErrorPpb = maxP99ErrorPpb;
    }
  }

  @State(Scope.Thread)
  @AuxCounters(AuxCounters.Type.EVENTS)
  public static class QualityCounters {
    private double _aggregationDivisor;
    private long _samples;
    private double _inputCentroidCount;
    private double _resultingCentroidCount;
    private double _maxResultingCentroidCount;
    private double _p75ErrorPpb;
    private double _maxP75ErrorPpb;
    private double _p95ErrorPpb;
    private double _maxP95ErrorPpb;
    private double _p99ErrorPpb;
    private double _maxP99ErrorPpb;

    @Setup(Level.Trial)
    public void setUp(BenchmarkParams benchmarkParams) {
      // Event counters are summed across measurement iterations and forks in the final JMH result.
      _aggregationDivisor = (double) benchmarkParams.getMeasurement().getCount()
          * Math.max(1, benchmarkParams.getForks());
    }

    @Setup(Level.Iteration)
    public void reset() {
      _samples = 0L;
      _inputCentroidCount = 0.0;
      _resultingCentroidCount = 0.0;
      _maxResultingCentroidCount = 0.0;
      _p75ErrorPpb = 0.0;
      _maxP75ErrorPpb = 0.0;
      _p95ErrorPpb = 0.0;
      _maxP95ErrorPpb = 0.0;
      _p99ErrorPpb = 0.0;
      _maxP99ErrorPpb = 0.0;
    }

    public double inputCentroidCount() {
      return perResult(_inputCentroidCount);
    }

    public double resultingCentroidCount() {
      return perResult(_resultingCentroidCount);
    }

    public double maxResultingCentroidCount() {
      return perResult(_maxResultingCentroidCount);
    }

    public double p75ErrorPpb() {
      return perResult(_p75ErrorPpb);
    }

    public double maxP75ErrorPpb() {
      return perResult(_maxP75ErrorPpb);
    }

    public double p95ErrorPpb() {
      return perResult(_p95ErrorPpb);
    }

    public double maxP95ErrorPpb() {
      return perResult(_maxP95ErrorPpb);
    }

    public double p99ErrorPpb() {
      return perResult(_p99ErrorPpb);
    }

    public double maxP99ErrorPpb() {
      return perResult(_maxP99ErrorPpb);
    }

    private void record(QualityStats stats) {
      _samples++;
      _inputCentroidCount += stats._inputCentroidCount;
      _resultingCentroidCount += stats._resultingCentroidCount;
      _maxResultingCentroidCount += stats._maxResultingCentroidCount;
      _p75ErrorPpb += stats._p75ErrorPpb;
      _maxP75ErrorPpb += stats._maxP75ErrorPpb;
      _p95ErrorPpb += stats._p95ErrorPpb;
      _maxP95ErrorPpb += stats._maxP95ErrorPpb;
      _p99ErrorPpb += stats._p99ErrorPpb;
      _maxP99ErrorPpb += stats._maxP99ErrorPpb;
    }

    private double perResult(double sum) {
      return _samples == 0L ? 0.0 : sum / _samples / _aggregationDivisor;
    }
  }

  /// Immutable verbose TDigest input used to keep the exact source centroid layout identical across dependency
  /// versions. TDigest 3.3 force-compresses a [MergingDigest] when its centroids are read, so a library digest cannot
  /// represent this control input without changing the state under test.
  private static final class FixedTDigest extends TDigest {
    private final byte[] _bytes;
    private final double _compression;
    private final double _min;
    private final double _max;
    private final long _size;
    private final List<Centroid> _centroids;

    private FixedTDigest(double[] sortedValues, double compression) {
      this(TDigestBenchmarkUtils.createFixedVerboseBytes(sortedValues, compression));
    }

    private FixedTDigest(byte[] bytes) {
      _bytes = bytes;
      ByteBuffer input = ByteBuffer.wrap(bytes);
      if (input.getInt() != 1) {
        throw new IllegalArgumentException("Expected verbose TDigest encoding");
      }
      _min = input.getDouble();
      _max = input.getDouble();
      _compression = input.getDouble();
      int numCentroids = input.getInt();
      List<Centroid> centroids = new ArrayList<>(numCentroids);
      long size = 0L;
      for (int i = 0; i < numCentroids; i++) {
        double weight = input.getDouble();
        double mean = input.getDouble();
        int count = Math.toIntExact((long) weight);
        if (count != weight) {
          throw new IllegalArgumentException("Non-integral fixed TDigest weight: " + weight);
        }
        centroids.add(new Centroid(mean, count));
        size += count;
      }
      _centroids = List.copyOf(centroids);
      _size = size;
    }

    private byte[] bytes() {
      return _bytes.clone();
    }

    @Override
    public void add(double value) {
      throw new UnsupportedOperationException("Fixed TDigest is immutable");
    }

    @Override
    public void add(double value, int weight) {
      throw new UnsupportedOperationException("Fixed TDigest is immutable");
    }

    @Override
    public void add(List<? extends TDigest> others) {
      throw new UnsupportedOperationException("Fixed TDigest is immutable");
    }

    @Override
    public void add(TDigest other) {
      throw new UnsupportedOperationException("Fixed TDigest is immutable");
    }

    @Override
    public void compress() {
    }

    @Override
    public long size() {
      return _size;
    }

    @Override
    public double cdf(double value) {
      return toTDigest().cdf(value);
    }

    @Override
    public double quantile(double quantile) {
      return toTDigest().quantile(quantile);
    }

    @Override
    public int centroidCount() {
      return _centroids.size();
    }

    @Override
    public List<Centroid> centroids() {
      return _centroids;
    }

    @Override
    public double compression() {
      return _compression;
    }

    @Override
    public int byteSize() {
      return _bytes.length;
    }

    @Override
    public int smallByteSize() {
      return toTDigest().smallByteSize();
    }

    @Override
    public void asBytes(ByteBuffer buffer) {
      buffer.put(_bytes);
    }

    @Override
    public void asSmallBytes(ByteBuffer buffer) {
      toTDigest().asSmallBytes(buffer);
    }

    @Override
    public TDigest recordAllData() {
      return toTDigest().recordAllData();
    }

    @Override
    public boolean isRecording() {
      return false;
    }

    @Override
    public double getMin() {
      return _min;
    }

    @Override
    public double getMax() {
      return _max;
    }

    private TDigest toTDigest() {
      return MergingDigest.fromBytes(ByteBuffer.wrap(_bytes));
    }
  }

  private static final class ImmutableMergingDigest extends MergingDigest {
    private boolean _frozen;

    private ImmutableMergingDigest(double compression) {
      super(compression);
    }

    private void freeze() {
      super.compress();
      _frozen = true;
    }

    @Override
    public void compress() {
      if (!_frozen) {
        super.compress();
      }
    }
  }

  private static final class ReducerAggregationFunction extends PercentileTDigestAggregationFunction {
    private final Implementation _implementation;
    private final Map<TDigest, List<TDigest>> _pending = new IdentityHashMap<>();

    private ReducerAggregationFunction(ExpressionContext expression, int compression,
        Implementation implementation) {
      super(expression, 75.0, compression, false);
      _implementation = implementation;
    }

    @Override
    public TDigest merge(TDigest intermediateResult1, TDigest intermediateResult2) {
      if (!_implementation.isBatching()) {
        if (_implementation == Implementation.PAIRWISE) {
          if (intermediateResult1.size() == 0L) {
            return intermediateResult2;
          }
          if (intermediateResult2.size() == 0L) {
            return intermediateResult1;
          }
          intermediateResult1.add(intermediateResult2);
          return intermediateResult1;
        }
        if (_implementation == Implementation.SINGLETON_LIST && intermediateResult1.size() != 0L
            && intermediateResult2.size() != 0L) {
          intermediateResult1.add(List.of(intermediateResult2));
          return intermediateResult1;
        }
        return super.merge(intermediateResult1, intermediateResult2);
      }
      if (intermediateResult1.size() == 0L) {
        return intermediateResult2;
      }
      if (intermediateResult2.size() == 0L) {
        return intermediateResult1;
      }
      List<TDigest> pending = _pending.computeIfAbsent(intermediateResult1,
          ignored -> new ArrayList<>(_implementation._batchSize));
      pending.add(intermediateResult2);
      if (pending.size() == _implementation._batchSize) {
        flush(intermediateResult1, pending);
      }
      return intermediateResult1;
    }

    private void flushAll() {
      if (_pending.isEmpty()) {
        return;
      }
      for (Map.Entry<TDigest, List<TDigest>> entry : _pending.entrySet()) {
        flush(entry.getKey(), entry.getValue());
      }
      _pending.clear();
    }

    private void resetPending() {
      if (!_pending.isEmpty()) {
        throw new IllegalStateException("Pending TDigest batches were not flushed");
      }
    }

    private void flush(TDigest target, List<TDigest> pending) {
      if (pending.isEmpty()) {
        return;
      }
      TDigest batch = TDigestBenchmarkUtils.usePinotScaleFunction(TDigest.createMergingDigest(_compressionFactor));
      batch.add(pending);
      pending.clear();
      target.add(batch);
    }
  }
}
