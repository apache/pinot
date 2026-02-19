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
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.pinot.common.function.FunctionInfo;
import org.apache.pinot.common.function.FunctionRegistry;
import org.apache.pinot.common.function.QueryFunctionInvoker;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.query.planner.logical.RexExpression;
import org.apache.pinot.query.runtime.operator.operands.FunctionOperand;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.ChainedOptionsBuilder;
import org.openjdk.jmh.runner.options.OptionsBuilder;


/**
 * Benchmark for {@link FunctionOperand#apply} to measure the impact of skipping
 * {@code convertTypes()} when operand types already match function parameter types.
 *
 * <p>The benchmark has two parts:
 * <ul>
 *   <li><b>End-to-end apply()</b>: Compares FunctionOperand.apply() when types match (convertTypes
 *       skipped) vs when types mismatch (convertTypes called per row).</li>
 *   <li><b>Isolated convertTypes()</b>: Directly measures the per-row cost of
 *       convertTypes() on type-matching arguments — the exact overhead eliminated by the
 *       optimization — against a no-op baseline.</li>
 * </ul>
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Fork(1)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@State(Scope.Benchmark)
public class BenchmarkFunctionOperand {

  private static final int NUM_ROWS = 1000;

  // End-to-end state
  private FunctionOperand _matchingTypesOperand;
  private FunctionOperand _mismatchedTypesOperand;
  private List<List<Object>> _longRows;
  private List<List<Object>> _intRows;

  // Isolated convertTypes state
  private QueryFunctionInvoker _invoker;
  private Object[][] _matchingArgs;

  @Setup(Level.Trial)
  public void setUp() {
    FunctionRegistry.init();

    // --- End-to-end setup ---

    // Scenario 1: plus(LONG, LONG) with LONG columns → types match, no conversion needed
    DataSchema longSchema = new DataSchema(
        new String[]{"col0", "col1"},
        new ColumnDataType[]{ColumnDataType.LONG, ColumnDataType.LONG}
    );
    RexExpression.FunctionCall plusLong = new RexExpression.FunctionCall(
        ColumnDataType.LONG, "plus",
        Arrays.asList(new RexExpression.InputRef(0), new RexExpression.InputRef(1))
    );
    _matchingTypesOperand = new FunctionOperand(plusLong, longSchema);

    // Scenario 2: plus(INT, INT) → resolves to longPlus(long, long), but argument types are INT
    //             → types mismatch, convertTypes() called per row
    DataSchema intSchema = new DataSchema(
        new String[]{"col0", "col1"},
        new ColumnDataType[]{ColumnDataType.INT, ColumnDataType.INT}
    );
    RexExpression.FunctionCall plusInt = new RexExpression.FunctionCall(
        ColumnDataType.LONG, "plus",
        Arrays.asList(new RexExpression.InputRef(0), new RexExpression.InputRef(1))
    );
    _mismatchedTypesOperand = new FunctionOperand(plusInt, intSchema);

    _longRows = new ArrayList<>(NUM_ROWS);
    _intRows = new ArrayList<>(NUM_ROWS);
    for (int i = 0; i < NUM_ROWS; i++) {
      _longRows.add(Arrays.asList((long) i, (long) (i + 1)));
      _intRows.add(Arrays.asList(i, i + 1));
    }

    // --- Isolated convertTypes setup ---

    // Get the longPlus(long, long) function invoker
    ColumnDataType[] longArgTypes = {ColumnDataType.LONG, ColumnDataType.LONG};
    FunctionInfo functionInfo = FunctionRegistry.lookupFunctionInfo(
        FunctionRegistry.canonicalize("plus"), longArgTypes);
    _invoker = new QueryFunctionInvoker(functionInfo);

    // Pre-allocate argument arrays with Long values (matching type for long parameters).
    // convertTypes() will still check isAssignableFrom + HashMap lookup per argument per row.
    _matchingArgs = new Object[NUM_ROWS][];
    for (int i = 0; i < NUM_ROWS; i++) {
      _matchingArgs[i] = new Object[]{(long) i, (long) (i + 1)};
    }
  }

  // --- End-to-end benchmarks ---

  @Benchmark
  public void applyTypesMatch(Blackhole bh) {
    for (List<Object> row : _longRows) {
      bh.consume(_matchingTypesOperand.apply(row));
    }
  }

  @Benchmark
  public void applyTypesNeedConversion(Blackhole bh) {
    for (List<Object> row : _intRows) {
      bh.consume(_mismatchedTypesOperand.apply(row));
    }
  }

  // --- Isolated convertTypes benchmarks ---
  // These directly measure the per-block overhead of convertTypes() on already-matching types,
  // which is the exact cost eliminated by the _needsConversion optimization.

  /**
   * Baseline: invoke the function on each row without calling convertTypes().
   * This represents the optimized path (types match, convertTypes skipped).
   */
  @Benchmark
  public void invokeWithoutConvertTypes(Blackhole bh) {
    for (Object[] args : _matchingArgs) {
      bh.consume(_invoker.invoke(args));
    }
  }

  /**
   * Old path: call convertTypes() then invoke the function on each row.
   * This represents the pre-optimization path (convertTypes always called).
   */
  @Benchmark
  public void invokeWithConvertTypes(Blackhole bh) {
    for (Object[] args : _matchingArgs) {
      _invoker.convertTypes(args);
      bh.consume(_invoker.invoke(args));
    }
  }

  public static void main(String[] args)
      throws Exception {
    ChainedOptionsBuilder opt = new OptionsBuilder()
        .include(BenchmarkFunctionOperand.class.getSimpleName());
    new Runner(opt.build()).run();
  }
}
