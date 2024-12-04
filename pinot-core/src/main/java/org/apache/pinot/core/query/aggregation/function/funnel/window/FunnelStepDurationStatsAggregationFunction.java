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
package org.apache.pinot.core.query.aggregation.function.funnel.window;

import it.unimi.dsi.fastutil.doubles.DoubleArrayList;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.core.query.aggregation.function.funnel.FunnelStepEvent;
import org.apache.pinot.segment.local.aggregator.AvgValueAggregator;
import org.apache.pinot.segment.local.aggregator.PercentileEstValueAggregator;
import org.apache.pinot.segment.local.customobject.AvgPair;
import org.apache.pinot.segment.local.customobject.QuantileDigest;
import org.apache.pinot.segment.spi.AggregationFunctionType;


public class FunnelStepDurationStatsAggregationFunction extends FunnelBaseAggregationFunction<DoubleArrayList> {

  private static final AvgValueAggregator AVG_VALUE_AGGREGATOR = new AvgValueAggregator();
  private static final PercentileEstValueAggregator PERCENTILE_EST_VALUE_AGGREGATOR =
      new PercentileEstValueAggregator();

  private final List<String> _durationFunctions = new ArrayList<>();

  public FunnelStepDurationStatsAggregationFunction(List<ExpressionContext> arguments) {
    super(arguments);
    if (_extraArguments.get("DURATIONFUNCTIONS") != null) {
      String[] durationFunctions = _extraArguments.get("DURATIONFUNCTIONS").split(",");
      for (String durationFunction : durationFunctions) {
        String functionName = durationFunction.trim().toUpperCase();
        if (functionName.equals("AVG") || functionName.equals("MEDIAN") || functionName.equals("MIN")
            || functionName.equals("MAX")) {
          _durationFunctions.add(functionName);
        } else if (functionName.startsWith("PERCENTILE")) {
          try {
            double quantile = Double.parseDouble(functionName.substring("PERCENTILE".length())) / 100.0;
            if (quantile < 0 || quantile > 1) {
              throw new IllegalArgumentException("Invalid percentile value: " + quantile);
            }
          } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid percentile function name: " + functionName + ", must be "
                + "PERCENTILE followed by a double value between 0 and 100");
          }
          _durationFunctions.add(functionName);
        } else {
          throw new IllegalArgumentException("Unsupported duration function: " + functionName);
        }
      }
    }
  }

  @Override
  public AggregationFunctionType getType() {
    return AggregationFunctionType.FUNNELSTEPDURATIONSTATS;
  }

  @Override
  public DataSchema.ColumnDataType getFinalResultColumnType() {
    return DataSchema.ColumnDataType.DOUBLE_ARRAY;
  }

  @Override
  public DoubleArrayList extractFinalResult(PriorityQueue<FunnelStepEvent> stepEvents) {
    if (stepEvents == null || stepEvents.isEmpty()) {
      return new DoubleArrayList();
    }
    Map<Integer, List<Object>> stepValueAggregators = initValueAggregator(_durationFunctions);
    boolean hasMatchedFunnel = false;
    ArrayDeque<FunnelStepEvent> slidingWindow = new ArrayDeque<>();
    while (!stepEvents.isEmpty()) {
      fillWindow(stepEvents, slidingWindow);
      if (slidingWindow.isEmpty()) {
        break;
      }
      int maxSteps = processWindow(slidingWindow);
      if (maxSteps == _numSteps) {
        applyStepDurations(stepValueAggregators, slidingWindow);
        hasMatchedFunnel = true;
      }
      if (!slidingWindow.isEmpty()) {
        slidingWindow.pollFirst();
      }
    }
    if (!hasMatchedFunnel) {
      return new DoubleArrayList();
    }
    return getStepDurationResults(stepValueAggregators);
  }

  private void applyStepDurations(Map<Integer, List<Object>> stepAggregatorValues,
      ArrayDeque<FunnelStepEvent> slidingWindow) {
    List<Long> stepTimestamp = new ArrayList<>();
    for (FunnelStepEvent event : slidingWindow) {
      int step = event.getStep();
      if (stepTimestamp.size() <= step) {
        stepTimestamp.add(event.getTimestamp());
      }
    }
    for (int i = 0; i < stepTimestamp.size() - 1; i++) {
      long duration = stepTimestamp.get(i + 1) - stepTimestamp.get(i);
      for (Object stepAggregatorValue : stepAggregatorValues.get(i)) {
        if (stepAggregatorValue instanceof AvgPair) {
          AVG_VALUE_AGGREGATOR.applyRawValue((AvgPair) stepAggregatorValue, duration);
        } else if (stepAggregatorValue instanceof QuantileDigest) {
          PERCENTILE_EST_VALUE_AGGREGATOR.applyRawValue((QuantileDigest) stepAggregatorValue, duration);
        }
      }
    }
  }

  private Map<Integer, List<Object>> initValueAggregator(List<String> durationFunctions) {
    Map<Integer, List<Object>> stepValueAggregators = new HashMap<>();
    for (int step = 0; step < _numSteps - 1; step++) {
      List<Object> aggregatorValues = new ArrayList<>();
      if (durationFunctions.contains("AVG")) {
        aggregatorValues.add(new AvgPair());
        if (durationFunctions.size() > 1) {
          aggregatorValues.add(new QuantileDigest(0));
        }
      } else {
        aggregatorValues.add(new QuantileDigest(0));
      }
      stepValueAggregators.put(step, aggregatorValues);
    }
    return stepValueAggregators;
  }

  private DoubleArrayList getStepDurationResults(Map<Integer, List<Object>> valueAggregatorResults) {
    DoubleArrayList result = new DoubleArrayList(_durationFunctions.size() * _numSteps);
    for (int step = 0; step < _numSteps - 1; step++) {
      AtomicReference<AvgPair> avgPair = new AtomicReference<>();
      AtomicReference<QuantileDigest> quantileDigest = new AtomicReference<>();
      valueAggregatorResults.get(step).forEach(valueAggregator -> {
        if (valueAggregator instanceof AvgPair) {
          avgPair.set((AvgPair) valueAggregator);
        }
        if (valueAggregator instanceof QuantileDigest) {
          quantileDigest.set((QuantileDigest) valueAggregator);
        }
      });
      for (int i = 0; i < _durationFunctions.size(); i++) {
        String durationFunction = _durationFunctions.get(i);
        if (durationFunction.equals("AVG")) {
          result.add(avgPair.get().getSum() / avgPair.get().getCount());
        } else if (durationFunction.equals("MEDIAN")) {
          result.add(quantileDigest.get().getQuantile(0.5));
        } else if (durationFunction.equals("MIN")) {
          result.add(quantileDigest.get().getQuantile(0));
        } else if (durationFunction.equals("MAX")) {
          result.add(quantileDigest.get().getQuantile(1));
        } else if (durationFunction.startsWith("PERCENTILE")) {
          double quantile = Double.parseDouble(durationFunction.substring("PERCENTILE".length())) / 100.0;
          result.add(quantileDigest.get().getQuantile(quantile));
        }
      }
    }
    return result;
  }

  protected Integer processWindow(ArrayDeque<FunnelStepEvent> slidingWindow) {
    int maxStep = 0;
    long previousTimestamp = -1;
    for (FunnelStepEvent event : slidingWindow) {
      int currentEventStep = event.getStep();
      // If the same condition holds for the sequence of events, then such repeating event interrupts further
      // processing.
      if (_modes.hasStrictDeduplication()) {
        if (currentEventStep == maxStep - 1) {
          return maxStep;
        }
      }
      // Don't allow interventions of other events. E.g. in the case of A->B->D->C, it stops finding A->B->C at the D
      // and the max event level is 2.
      if (_modes.hasStrictOrder()) {
        if (currentEventStep != maxStep) {
          return maxStep;
        }
      }
      // Apply conditions only to events with strictly increasing timestamps.
      if (_modes.hasStrictIncrease()) {
        if (previousTimestamp == event.getTimestamp()) {
          continue;
        }
      }
      if (maxStep == currentEventStep) {
        maxStep++;
        previousTimestamp = event.getTimestamp();
      }
      if (maxStep == _numSteps) {
        break;
      }
    }
    return maxStep;
  }

  @Override
  public DoubleArrayList mergeFinalResult(DoubleArrayList finalResult1, DoubleArrayList finalResult2) {
    if (finalResult1 == null) {
      return finalResult2;
    }
    return finalResult1;
  }
}
