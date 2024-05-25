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
package org.apache.pinot.core.query.aggregation.function.funnel;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.stream.Collectors;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.core.common.BlockValSet;
import org.apache.pinot.core.query.aggregation.AggregationResultHolder;
import org.apache.pinot.core.query.aggregation.ObjectAggregationResultHolder;
import org.apache.pinot.core.query.aggregation.function.AggregationFunction;
import org.apache.pinot.core.query.aggregation.groupby.GroupByResultHolder;
import org.apache.pinot.core.query.aggregation.groupby.ObjectGroupByResultHolder;
import org.apache.pinot.segment.spi.AggregationFunctionType;


public class FunnelMaxStepAggregationFunction implements AggregationFunction<PriorityQueue<FunnelStepEvent>, Long> {
  private final ExpressionContext _timestampExpression;
  private final long _windowSize;
  private final List<ExpressionContext> _stepExpressions;
  private final FunnelModes _modes = new FunnelModes();
  private final int _numSteps;

  public FunnelMaxStepAggregationFunction(List<ExpressionContext> arguments) {
    int numArguments = arguments.size();
    Preconditions.checkArgument(numArguments > 2,
        "FUNNELMAXSTEP expects >= 3 arguments, got: %s. The function can be used as "
            + "funnelMaxStep(timestampExpression, windowSize, ARRAY[stepExpression, ..], [mode, [mode, ... ]])",
        numArguments);
    _timestampExpression = arguments.get(0);
    _windowSize = arguments.get(1).getLiteral().getLongValue();
    Preconditions.checkArgument(_windowSize > 0, "Window size must be > 0");
    ExpressionContext stepExpressionContext = arguments.get(2);
    if (stepExpressionContext.getFunction() != null) {
      // LEAF stage init this function like funnelmaxstep($1,'1000',arrayValueConstructor($2,$3,$4,...))
      _stepExpressions = stepExpressionContext.getFunction().getArguments();
    } else {
      // Intermediate Stage init this function like funnelmaxstep($1,'1000',__PLACEHOLDER__)
      _stepExpressions = ImmutableList.of();
    }
    if (numArguments > 3) {
      arguments.subList(3, numArguments)
          .forEach(arg -> _modes.add(Mode.valueOf(arg.getLiteral().getStringValue().toUpperCase())));
    }
    _numSteps = _stepExpressions.size();
  }

  @Override
  public AggregationFunctionType getType() {
    return AggregationFunctionType.FUNNELMAXSTEP;
  }

  @Override
  public String getResultColumnName() {
    return getType().getName().toLowerCase() + "(" + _windowSize + ")  (" + _timestampExpression.toString() + ", "
        + _stepExpressions.stream().map(ExpressionContext::toString).collect(Collectors.joining(",")) + ")";
  }

  @Override
  public List<ExpressionContext> getInputExpressions() {
    List<ExpressionContext> inputs = new ArrayList<>(1 + _numSteps);
    inputs.add(_timestampExpression);
    inputs.addAll(_stepExpressions);
    return inputs;
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
    long[] timestampBlock = blockValSetMap.get(_timestampExpression).getLongValuesSV();
    List<int[]> stepBlocks = new ArrayList<>(_numSteps);
    for (ExpressionContext stepExpression : _stepExpressions) {
      stepBlocks.add(blockValSetMap.get(stepExpression).getIntValuesSV());
    }
    PriorityQueue<FunnelStepEvent> stepEvents = aggregationResultHolder.getResult();
    if (stepEvents == null) {
      stepEvents = new PriorityQueue<>();
      aggregationResultHolder.setValue(stepEvents);
    }
    for (int i = 0; i < length; i++) {
      for (int j = 0; j < _numSteps; j++) {
        if (stepBlocks.get(j)[i] == 1) {
          stepEvents.add(new FunnelStepEvent(timestampBlock[i], j));
          break;
        }
      }
    }
  }

  @Override
  public void aggregateGroupBySV(int length, int[] groupKeyArray, GroupByResultHolder groupByResultHolder,
      Map<ExpressionContext, BlockValSet> blockValSetMap) {
    long[] timestampBlock = blockValSetMap.get(_timestampExpression).getLongValuesSV();
    List<int[]> stepBlocks = new ArrayList<>(_numSteps);
    for (ExpressionContext stepExpression : _stepExpressions) {
      stepBlocks.add(blockValSetMap.get(stepExpression).getIntValuesSV());
    }
    for (int i = 0; i < length; i++) {
      int groupKey = groupKeyArray[i];
      for (int j = 0; j < _numSteps; j++) {
        if (stepBlocks.get(j)[i] == 1) {
          PriorityQueue<FunnelStepEvent> stepEvents = groupByResultHolder.getResult(groupKey);
          if (stepEvents == null) {
            stepEvents = new PriorityQueue<>();
            groupByResultHolder.setValueForKey(groupKey, stepEvents);
          }
          stepEvents.add(new FunnelStepEvent(timestampBlock[i], j));
          break;
        }
      }
    }
  }

  @Override
  public void aggregateGroupByMV(int length, int[][] groupKeysArray, GroupByResultHolder groupByResultHolder,
      Map<ExpressionContext, BlockValSet> blockValSetMap) {
    long[] timestampBlock = blockValSetMap.get(_timestampExpression).getLongValuesSV();
    List<int[]> stepBlocks = new ArrayList<>(_numSteps);
    for (ExpressionContext stepExpression : _stepExpressions) {
      stepBlocks.add(blockValSetMap.get(stepExpression).getIntValuesSV());
    }
    for (int i = 0; i < length; i++) {
      int[] groupKeys = groupKeysArray[i];
      for (int j = 0; j < _numSteps; j++) {
        if (stepBlocks.get(j)[i] == 1) {
          for (int groupKey : groupKeys) {
            PriorityQueue<FunnelStepEvent> stepEvents = groupByResultHolder.getResult(groupKey);
            if (stepEvents == null) {
              stepEvents = new PriorityQueue<>();
              groupByResultHolder.setValueForKey(groupKey, stepEvents);
            }
            stepEvents.add(new FunnelStepEvent(timestampBlock[i], j));
          }
          break;
        }
      }
    }
  }

  @Override
  public PriorityQueue<FunnelStepEvent> extractAggregationResult(AggregationResultHolder aggregationResultHolder) {
    return aggregationResultHolder.getResult();
  }

  @Override
  public PriorityQueue<FunnelStepEvent> extractGroupByResult(GroupByResultHolder groupByResultHolder, int groupKey) {
    return groupByResultHolder.getResult(groupKey);
  }

  @Override
  public PriorityQueue<FunnelStepEvent> merge(PriorityQueue<FunnelStepEvent> intermediateResult1,
      PriorityQueue<FunnelStepEvent> intermediateResult2) {
    if (intermediateResult1 == null) {
      return intermediateResult2;
    }
    if (intermediateResult2 == null) {
      return intermediateResult1;
    }
    intermediateResult1.addAll(intermediateResult2);
    return intermediateResult1;
  }

  @Override
  public DataSchema.ColumnDataType getIntermediateResultColumnType() {
    return DataSchema.ColumnDataType.OBJECT;
  }

  @Override
  public DataSchema.ColumnDataType getFinalResultColumnType() {
    return DataSchema.ColumnDataType.LONG;
  }

  @Override
  public Long extractFinalResult(PriorityQueue<FunnelStepEvent> stepEvents) {
    long finalMaxStep = 0;
    if (stepEvents == null || stepEvents.isEmpty()) {
      return finalMaxStep;
    }
    ArrayDeque<FunnelStepEvent> slidingWindow = new ArrayDeque<>();
    while (!stepEvents.isEmpty()) {
      fillWindow(stepEvents, slidingWindow);
      if (slidingWindow.isEmpty()) {
        break;
      }
      int maxSteps = processWindow(slidingWindow);
      finalMaxStep = Math.max(finalMaxStep, maxSteps);
      if (finalMaxStep == _numSteps) {
        break;
      }
      if (!slidingWindow.isEmpty()) {
        slidingWindow.pollFirst();
      }
    }
    return finalMaxStep;
  }

  /**
   * Fill the sliding window with the events that fall into the window.
   * Note that the events from stepEvents are dequeued and added to the sliding window.
   * This method ensure the first event from the sliding window is the first step event.
   * @param stepEvents The priority queue of step events
   * @param slidingWindow The sliding window with events that fall into the window
   */
  private void fillWindow(PriorityQueue<FunnelStepEvent> stepEvents, ArrayDeque<FunnelStepEvent> slidingWindow) {
    // Ensure for the sliding window, the first event is the first step
    while ((!slidingWindow.isEmpty()) && slidingWindow.peek().getStep() != 0) {
      slidingWindow.pollFirst();
    }
    if (slidingWindow.isEmpty()) {
      while (!stepEvents.isEmpty() && stepEvents.peek().getStep() != 0) {
        stepEvents.poll();
      }
      if (stepEvents.isEmpty()) {
        return;
      }
      slidingWindow.addLast(stepEvents.poll());
    }
    // SlidingWindow is not empty
    long windowStart = slidingWindow.peek().getTimestamp();
    long windowEnd = windowStart + _windowSize;
    while (!stepEvents.isEmpty() && (stepEvents.peek().getTimestamp() < windowEnd)) {
      slidingWindow.addLast(stepEvents.poll());
    }
  }

  private int processWindow(ArrayDeque<FunnelStepEvent> slidingWindow) {
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
      previousTimestamp = event.getTimestamp();
      if (maxStep == currentEventStep) {
        maxStep++;
      }
      if (maxStep == _numSteps) {
        break;
      }
    }
    return maxStep;
  }

  @Override
  public Long mergeFinalResult(Long finalResult1, Long finalResult2) {
    return Math.max(finalResult1, finalResult2);
  }

  @Override
  public String toExplainString() {
    //@formatter:off
    return "WindowFunnelAggregationFunction{"
        + "timestampExpression=" + _timestampExpression
        + ", windowSize=" + _windowSize
        + ", stepExpressions=" + _stepExpressions
        + '}';
    //@formatter:on
  }

  enum Mode {
    STRICT_DEDUPLICATION(1), STRICT_ORDER(2), STRICT_INCREASE(4);

    private final int _value;

    Mode(int value) {
      _value = value;
    }

    public int getValue() {
      return _value;
    }
  }

  static class FunnelModes {
    private int _bitmask = 0;

    public void add(Mode mode) {
      _bitmask |= mode.getValue();
    }

    public void remove(Mode mode) {
      _bitmask &= ~mode.getValue();
    }

    public boolean contains(Mode mode) {
      return (_bitmask & mode.getValue()) != 0;
    }

    public boolean hasStrictDeduplication() {
      return contains(Mode.STRICT_DEDUPLICATION);
    }

    public boolean hasStrictOrder() {
      return contains(Mode.STRICT_ORDER);
    }

    public boolean hasStrictIncrease() {
      return contains(Mode.STRICT_INCREASE);
    }
  }
}
