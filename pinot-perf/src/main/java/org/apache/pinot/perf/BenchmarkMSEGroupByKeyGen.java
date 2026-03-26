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
package org.apache.pinot.perf;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.query.planner.plannode.AggregateNode.AggType;
import org.apache.pinot.query.runtime.blocks.RowHeapDataBlock;
import org.apache.pinot.query.runtime.operator.MultistageGroupByExecutor;
import org.apache.pinot.query.runtime.operator.groupby.GroupIdGenerator;
import org.apache.pinot.query.runtime.operator.groupby.GroupIdGeneratorFactory;
import org.apache.pinot.query.runtime.operator.utils.SortUtils;
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
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.ChainedOptionsBuilder;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.TimeValue;


/**
 * JMH benchmarks for the MSE group-by key generation and result materialisation hot paths.
 *
 * <h3>Scenarios covered</h3>
 * <ul>
 *   <li>{@code SINGLE_INT}    — single INT key → {@code OneIntKeyGroupIdGenerator} (Int2IntOpenHashMap)</li>
 *   <li>{@code SINGLE_STRING} — single STRING key → {@code OneObjectKeyGroupIdGenerator} (Object2IntOpenHashMap)</li>
 *   <li>{@code TWO_INT_KEYS}  — two fixed-width keys → {@code TwoKeysGroupIdGenerator} (Long2IntOpenHashMap)</li>
 *   <li>{@code FOUR_INT_KEYS} — four-key composite → {@code MultiKeysGroupIdGenerator}
 *       (Object2IntOpenHashMap&lt;FixedIntArray&gt;)</li>
 *   <li>{@code SKEWED}          — single INT key, very low cardinality (10 distinct groups in 100 K rows)</li>
 *   <li>{@code SORTED_COLUMN}   — single INT key, every row is a unique group (no collisions)</li>
 * </ul>
 *
 * <h3>Benchmark methods</h3>
 * <ul>
 *   <li>{@link #keyGenSteadyState}      — steady-state look-up: all groups are already in the map</li>
 *   <li>{@link #keyGenInsert}           — build phase: inserts {@code NUM_ROWS} rows into a fresh generator per
 *       iteration</li>
 *   <li>{@link #materializeTopK}        — result materialisation: top-K via priority queue
 *       ({@code ORDER BY sum(val) DESC LIMIT K})</li>
 *   <li>{@link #materializeAll}         — result materialisation without ordering (plain truncation)</li>
 * </ul>
 *
 * <h3>Running</h3>
 * <pre>
 *   mvn -pl pinot-perf -am package -DskipTests -Ppinot-fastdev
 *   java -cp pinot-perf/target/pinot-perf-*-jar-with-dependencies.jar \
 *        org.apache.pinot.perf.BenchmarkMSEGroupByKeyGen
 * </pre>
 *
 * Or via the main method:
 * <pre>
 *   mvn -pl pinot-perf -am exec:java -Dexec.mainClass=org.apache.pinot.perf.BenchmarkMSEGroupByKeyGen
 * </pre>
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Fork(value = 1, jvmArgs = {"-server", "-Xmx4G"})
@Warmup(iterations = 2, time = 10)
@Measurement(iterations = 3, time = 15)
@State(Scope.Benchmark)
public class BenchmarkMSEGroupByKeyGen {

  /** Number of input rows fed per benchmark invocation (key-gen benchmarks). */
  private static final int NUM_ROWS = 100_000;
  /** Number of groups for top-K materialisation benchmark. */
  private static final int NUM_GROUPS_FOR_MATERIALIZE = 50_000;
  /** K for the top-K result materialisation benchmark. */
  private static final int TOP_K = 100;
  /** Groups limit used when constructing generators. */
  private static final int NUM_GROUPS_LIMIT = 200_000;

  // ---- scenario selection ----
  @Param({"SINGLE_INT", "SINGLE_STRING", "TWO_INT_KEYS", "FOUR_INT_KEYS", "SKEWED", "SORTED_COLUMN"})
  public String _scenario;

  // ---- shared state ----

  /**
   * Pre-built single-value key array for {@link #keyGenSteadyState}.
   * Each element is the key object expected by the generator (Integer, String, or Object[] for multi-key).
   */
  private Object[] _keys;

  /**
   * Number of distinct groups that exist in {@link #_keys}.
   * Used to size the generator so it never hits the groups limit.
   */
  private int _distinctGroups;

  /**
   * Schema for a single-INT-key executor (used in top-K materialisation benchmarks).
   */
  private static final DataSchema SCHEMA_INT_SUM = new DataSchema(
      new String[]{"k0", "sum_v"},
      new ColumnDataType[]{ColumnDataType.INT, ColumnDataType.DOUBLE});

  /**
   * Pre-populated {@link MultistageGroupByExecutor} for materialisation benchmarks.
   * Populated once in {@link #setupMaterialize}.
   */
  private MultistageGroupByExecutor _populatedExecutor;

  /**
   * Reverse comparator used for {@code ORDER BY sum DESC} in top-K materialisation.
   */
  private java.util.Comparator<Object[]> _sumDescComparator;

  // ---- Setup methods ----

  /**
   * Builds the key array for steady-state look-up benchmarks.
   * Called once per trial — the generator is re-created inside the benchmark method
   * because the insert benchmark requires a fresh map per iteration.
   */
  @Setup(Level.Trial)
  public void setupKeys() {
    Random rng = new Random(42);

    switch (_scenario) {
      case "SINGLE_INT":
        _distinctGroups = 1_000;
        _keys = buildIntKeys(rng, NUM_ROWS, _distinctGroups);
        break;
      case "SINGLE_STRING":
        _distinctGroups = 1_000;
        _keys = buildStringKeys(rng, NUM_ROWS, _distinctGroups);
        break;
      case "TWO_INT_KEYS":
        _distinctGroups = 500 * 500;
        _keys = buildTwoIntKeys(rng, NUM_ROWS, 500, 500);
        break;
      case "FOUR_INT_KEYS":
        _distinctGroups = 10 * 10 * 10 * 10;
        _keys = buildFourIntKeys(rng, NUM_ROWS, 10, 10, 10, 10);
        break;
      case "SKEWED":
        // Very low cardinality: 10 distinct groups in 100 K rows (hot-key pattern)
        _distinctGroups = 10;
        _keys = buildIntKeys(rng, NUM_ROWS, _distinctGroups);
        break;
      case "SORTED_COLUMN":
        // Every row is a unique group (sorted column, no collisions)
        _distinctGroups = NUM_ROWS;
        _keys = buildUniqueIntKeys(NUM_ROWS);
        break;
      default:
        throw new IllegalArgumentException("Unknown scenario: " + _scenario);
    }
  }

  /**
   * Pre-populates the {@link MultistageGroupByExecutor} for result-materialisation benchmarks.
   * Called once per trial.
   */
  @Setup(Level.Trial)
  public void setupMaterialize() {
    // Build an executor with NUM_GROUPS_FOR_MATERIALIZE distinct INT groups, each with a random SUM.
    Random rng = new Random(99);
    int[] groupIds = new int[NUM_GROUPS_FOR_MATERIALIZE];
    for (int i = 0; i < NUM_GROUPS_FOR_MATERIALIZE; i++) {
      groupIds[i] = i;
    }

    _populatedExecutor = buildAndPopulateIntSumExecutor(rng, NUM_GROUPS_FOR_MATERIALIZE);

    // Comparator: sort by col[1] (sum) descending → reversed so peek() returns the smallest
    List<RelFieldCollation> collations = List.of(
        new RelFieldCollation(1, RelFieldCollation.Direction.DESCENDING, RelFieldCollation.NullDirection.LAST));
    _sumDescComparator = new SortUtils.SortComparator(collations, true);
  }

  // ---- Benchmark: key-gen steady state ----

  /**
   * Steady-state key generation: the generator is pre-populated with all distinct groups in
   * {@link #setupSteadyStateGenerator()}, so every {@code getGroupId()} call is a lookup (no insert).
   * This exercises the happy-path hash-map probe loop.
   */
  @State(Scope.Thread)
  public static class SteadyStateGenState {
    GroupIdGenerator _gen;
  }

  @Setup(Level.Trial)
  public void setupSteadyStateGenerator(SteadyStateGenState state) {
    // Create a generator for this scenario and pre-insert all distinct groups.
    GroupIdGenerator gen = makeGenerator(_scenario, _distinctGroups + 1, _distinctGroups);
    for (Object key : _keys) {
      gen.getGroupId(key);
    }
    state._gen = gen;
  }

  @Benchmark
  public void keyGenSteadyState(SteadyStateGenState state, Blackhole bh) {
    GroupIdGenerator gen = state._gen;
    for (Object key : _keys) {
      bh.consume(gen.getGroupId(key));
    }
  }

  // ---- Benchmark: key-gen insert (build phase) ----

  /**
   * Insert-phase key generation: a fresh generator is created before each iteration.
   * Measures the cost of building the map from scratch with {@code NUM_ROWS} rows and
   * {@code distinctGroups} distinct groups.
   */
  @State(Scope.Thread)
  public static class InsertGenState {
    GroupIdGenerator _gen;
  }

  @Setup(Level.Iteration)
  public void setupInsertGenerator(InsertGenState state) {
    state._gen = makeGenerator(_scenario, NUM_GROUPS_LIMIT, _distinctGroups);
  }

  @Benchmark
  public void keyGenInsert(InsertGenState state, Blackhole bh) {
    GroupIdGenerator gen = state._gen;
    for (Object key : _keys) {
      bh.consume(gen.getGroupId(key));
    }
  }

  // ---- Benchmark: result materialisation — top-K ----

  /**
   * Benchmarks {@link MultistageGroupByExecutor#getResult(java.util.Comparator, int)} — the ORDER BY agg DESC LIMIT K
   * path that uses a min-heap of size K to find the top-K rows.
   */
  @Benchmark
  public void materializeTopK(Blackhole bh) {
    List<Object[]> rows = _populatedExecutor.getResult(_sumDescComparator, TOP_K);
    bh.consume(rows);
  }

  // ---- Benchmark: result materialisation — all groups (plain truncation) ----

  /**
   * Benchmarks {@link MultistageGroupByExecutor#getResult(int)} — the no-ORDER-BY path that simply
   * iterates the hash-map and truncates at maxRows.  This is the lower bound for materialisation cost.
   */
  @Benchmark
  public void materializeAll(Blackhole bh) {
    List<Object[]> rows = _populatedExecutor.getResult(NUM_GROUPS_FOR_MATERIALIZE);
    bh.consume(rows);
  }

  // ---- helpers ----

  private static GroupIdGenerator makeGenerator(String scenario, int numGroupsLimit, int initialCapacity) {
    switch (scenario) {
      case "SINGLE_INT":
      case "SKEWED":
      case "SORTED_COLUMN":
        return GroupIdGeneratorFactory.getGroupIdGenerator(
            new ColumnDataType[]{ColumnDataType.INT}, 1, numGroupsLimit, initialCapacity);
      case "SINGLE_STRING":
        return GroupIdGeneratorFactory.getGroupIdGenerator(
            new ColumnDataType[]{ColumnDataType.STRING}, 1, numGroupsLimit, initialCapacity);
      case "TWO_INT_KEYS":
        return GroupIdGeneratorFactory.getGroupIdGenerator(
            new ColumnDataType[]{ColumnDataType.INT, ColumnDataType.INT}, 2, numGroupsLimit, initialCapacity);
      case "FOUR_INT_KEYS":
        return GroupIdGeneratorFactory.getGroupIdGenerator(
            new ColumnDataType[]{ColumnDataType.INT, ColumnDataType.INT, ColumnDataType.INT, ColumnDataType.INT},
            4, numGroupsLimit, initialCapacity);
      default:
        throw new IllegalArgumentException("Unknown scenario: " + scenario);
    }
  }

  /** Builds an array of NUM_ROWS Integer keys drawn from [0, distinctGroups). */
  private static Object[] buildIntKeys(Random rng, int numRows, int distinctGroups) {
    Object[] keys = new Object[numRows];
    for (int i = 0; i < numRows; i++) {
      keys[i] = rng.nextInt(distinctGroups);
    }
    return keys;
  }

  /** Builds an array of NUM_ROWS unique Integer keys (0, 1, 2, … numRows-1). */
  private static Object[] buildUniqueIntKeys(int numRows) {
    Object[] keys = new Object[numRows];
    for (int i = 0; i < numRows; i++) {
      keys[i] = i;
    }
    return keys;
  }

  /** Builds an array of NUM_ROWS String keys drawn from [0, distinctGroups) formatted as "k%05d". */
  private static Object[] buildStringKeys(Random rng, int numRows, int distinctGroups) {
    Object[] keys = new Object[numRows];
    for (int i = 0; i < numRows; i++) {
      keys[i] = String.format("k%05d", rng.nextInt(distinctGroups));
    }
    return keys;
  }

  /**
   * Builds two-key Object[] arrays for the {@code TWO_INT_KEYS} scenario.
   * Each element is an Object[]{Integer, Integer} matching the multi-key generator contract.
   */
  private static Object[] buildTwoIntKeys(Random rng, int numRows, int card0, int card1) {
    Object[] keys = new Object[numRows];
    for (int i = 0; i < numRows; i++) {
      keys[i] = new Object[]{rng.nextInt(card0), rng.nextInt(card1)};
    }
    return keys;
  }

  /**
   * Builds four-key Object[] arrays for the {@code FOUR_INT_KEYS} scenario.
   */
  private static Object[] buildFourIntKeys(Random rng, int numRows, int c0, int c1, int c2, int c3) {
    Object[] keys = new Object[numRows];
    for (int i = 0; i < numRows; i++) {
      keys[i] = new Object[]{rng.nextInt(c0), rng.nextInt(c1), rng.nextInt(c2), rng.nextInt(c3)};
    }
    return keys;
  }

  /**
   * Builds a {@link MultistageGroupByExecutor} pre-populated with {@code numGroups} distinct INT groups,
   * each having a random {@code SUM(val)} accumulated from a single row.
   *
   * <p>The executor is configured for {@code AggType.LEAF} (raw input rows, not intermediate format)
   * computing {@code SUM} on column index 1.
   */
  private static MultistageGroupByExecutor buildAndPopulateIntSumExecutor(Random rng, int numGroups) {
    // Build a block with one row per distinct group, val = random double.
    // Schema: [k0 INT, val DOUBLE]
    DataSchema inSchema = new DataSchema(new String[]{"k0", "val"},
        new ColumnDataType[]{ColumnDataType.INT, ColumnDataType.DOUBLE});

    List<Object[]> rows = new ArrayList<>(numGroups);
    for (int g = 0; g < numGroups; g++) {
      rows.add(new Object[]{g, (double) rng.nextInt(10_000)});
    }
    RowHeapDataBlock block = new RowHeapDataBlock(rows, inSchema);

    // Result schema: [k0 INT, sum(val) DOUBLE]
    // group key ids = [0], agg on col 1
    org.apache.pinot.core.query.aggregation.function.AggregationFunction<?, ?>[] aggFunctions =
        buildSumAggFunction();

    MultistageGroupByExecutor executor = new MultistageGroupByExecutor(
        new int[]{0},
        aggFunctions,
        new int[]{-1}, // no filter
        -1,
        AggType.LEAF,
        true, // leafReturnFinalResult
        SCHEMA_INT_SUM,
        Map.of(),
        null);

    executor.processBlock(block);
    return executor;
  }

  @SuppressWarnings({"rawtypes", "unchecked"})
  private static org.apache.pinot.core.query.aggregation.function.AggregationFunction<?, ?>[] buildSumAggFunction() {
    // SUM($1) — identifier "$1" means column index 1 in MSE convention
    org.apache.pinot.common.request.context.ExpressionContext arg =
        org.apache.pinot.common.request.context.ExpressionContext.forIdentifier("$1");
    org.apache.pinot.common.request.context.FunctionContext fn =
        new org.apache.pinot.common.request.context.FunctionContext(
            org.apache.pinot.common.request.context.FunctionContext.Type.AGGREGATION,
            "SUM",
            java.util.List.of(arg));
    return new org.apache.pinot.core.query.aggregation.function.AggregationFunction[]{
        org.apache.pinot.core.query.aggregation.function.AggregationFunctionFactory.getAggregationFunction(fn, true)
    };
  }

  // ---- main ----

  public static void main(String[] args)
      throws Exception {
    ChainedOptionsBuilder opt = new OptionsBuilder()
        .include(BenchmarkMSEGroupByKeyGen.class.getSimpleName())
        .warmupTime(TimeValue.seconds(10))
        .warmupIterations(2)
        .measurementTime(TimeValue.seconds(15))
        .measurementIterations(3)
        .forks(1);
    new Runner(opt.build()).run();
  }
}
