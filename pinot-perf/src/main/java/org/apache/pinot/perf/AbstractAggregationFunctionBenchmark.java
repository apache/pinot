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

import java.util.Map;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.core.common.BlockValSet;
import org.apache.pinot.core.plan.DocIdSetPlanNode;
import org.apache.pinot.core.query.aggregation.AggregationResultHolder;
import org.apache.pinot.core.query.aggregation.function.AggregationFunction;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.infra.Blackhole;
import org.testng.Assert;


public abstract class AbstractAggregationFunctionBenchmark {

  /**
   * Returns the aggregation function to benchmark.
   *
   * This method will be called in the benchmark method, so it must be fast.
   */
  protected abstract AggregationFunction<?, ?> getAggregationFunction();

  /**
   * Returns the result holder to use for the aggregation function.
   *
   * This method will be called in the benchmark method, so it must be fast.
   * @return
   */
  protected abstract AggregationResultHolder getResultHolder();

  /**
   * Resets the result holder to prepare for the next aggregation.
   *
   * This method will be called in the benchmark method, so it must be fast.
   */
  protected abstract void resetResultHolder(AggregationResultHolder resultHolder);

  /**
   * Returns the expected result of the aggregation function.
   *
   * This method will be called in the benchmark method, so it must be fast.
   */
  protected abstract Object getExpectedResult();

  /**
   * Returns the block value set map to use for the aggregation function.
   *
   * This method will be called in the benchmark method, so it must be fast.
   */
  protected abstract Map<ExpressionContext, BlockValSet> getBlockValSetMap();

  /**
   * Verifies the final result of the aggregation function.
   *
   * This method will be called in the benchmark method, so it must be fast.
   */
  protected void verifyResult(Blackhole bh, Comparable finalResult, Object expectedResult) {
    Assert.assertEquals(finalResult, expectedResult);
    bh.consume(finalResult);
  }

  /**
   * Base class for benchmarks that are stable on each {@link Level#Trial} or {@link Level#Iteration}.
   */
  public static abstract class Stable extends AbstractAggregationFunctionBenchmark {
    protected AggregationFunction<?, ?> _aggregationFunction;
    protected AggregationResultHolder _resultHolder;
    protected Object _expectedResult;
    protected Map<ExpressionContext, BlockValSet> _blockValSetMap;

    /**
     * Returns the level at which the aggregation function should be created.
     *
     * By default, the aggregation function is created at the {@link Level#Trial} level.
     */
    protected Level getAggregationFunctionLevel() {
      return Level.Trial;
    }

    /**
     * Creates the aggregation function to benchmark.
     *
     * This method will be called at the level returned by {@link #getAggregationFunctionLevel()}.
     * Therefore, time spent here is not counted towards the benchmark time.
     */
    protected abstract AggregationFunction<?, ?> createAggregationFunction();

    /**
     * Returns the level at which the result holder should be created.
     *
     * By default, the result holder is created at the {@link Level#Trial} level.
     */
    protected Level getResultHolderLevel() {
      return Level.Trial;
    }

    /**
     * Creates the result holder to use for the aggregation function.
     *
     * This method will be called at the level returned by {@link #getResultHolderLevel()}.
     * Therefore, time spent here is not counted towards the benchmark time.
     */
    protected abstract AggregationResultHolder createResultHolder();

    /**
     * Returns the level at which the block value set map should be created.
     *
     * By default, the block value set map is created at the {@link Level#Trial} level.
     */
    protected Level getBlockValSetMapLevel() {
      return Level.Trial;
    }

    /**
     * Creates the block value set map to use for the aggregation function.
     *
     * This method will be called at the level returned by {@link #getBlockValSetMapLevel()}.
     * Therefore, time spent here is not counted towards the benchmark time.
     */
    protected abstract Map<ExpressionContext, BlockValSet> createBlockValSetMap();

    /**
     * Returns the level at which the expected result should be created.
     *
     * By default, the expected result is created at the {@link Level#Trial} level.
     */
    protected Level getExpectedResultLevel() {
      return Level.Trial;
    }

    /**
     * Creates the expected result of the aggregation function.
     *
     * This method will be called at the level returned by {@link #getExpectedResultLevel()}.
     * Therefore, time spent here is not counted towards the benchmark time.
     */
    protected abstract Object createExpectedResult(Map<ExpressionContext, BlockValSet> map);

    @Override
    protected AggregationFunction<?, ?> getAggregationFunction() {
      return _aggregationFunction;
    }

    @Override
    protected Object getExpectedResult() {
      return _expectedResult;
    }

    @Override
    protected AggregationResultHolder getResultHolder() {
      return _resultHolder;
    }

    @Override
    public Map<ExpressionContext, BlockValSet> getBlockValSetMap() {
      return _blockValSetMap;
    }

    @Setup(Level.Trial)
    public void setupTrial() {
      onSetupLevel(Level.Trial);
    }

    @Setup(Level.Iteration)
    public void setupIteration() {
      onSetupLevel(Level.Iteration);
    }

    private void onSetupLevel(Level level) {
      if (getAggregationFunctionLevel() == level) {
        _aggregationFunction = createAggregationFunction();
      }
      if (getResultHolderLevel() == level) {
        _resultHolder = createResultHolder();
      }
      if (getBlockValSetMapLevel() == level) {
        _blockValSetMap = createBlockValSetMap();
      }
      if (getExpectedResultLevel() == level) {
        _expectedResult = createExpectedResult(_blockValSetMap);
      }
    }
  }

  @Benchmark
  public void test(Blackhole bh) {
    AggregationResultHolder resultHolder = getResultHolder();
    resetResultHolder(resultHolder);
    Map<ExpressionContext, BlockValSet> blockValSetMap = getBlockValSetMap();

    getAggregationFunction().aggregate(DocIdSetPlanNode.MAX_DOC_PER_CALL, resultHolder, blockValSetMap);

    Comparable finalResult = getAggregationFunction().extractFinalResult(resultHolder.getResult());

    verifyResult(bh, finalResult, getExpectedResult());
  }
}
