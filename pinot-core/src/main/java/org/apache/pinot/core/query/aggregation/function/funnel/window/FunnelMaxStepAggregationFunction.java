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

import java.util.ArrayDeque;
import java.util.List;
import java.util.PriorityQueue;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.core.query.aggregation.function.funnel.FunnelStepEvent;
import org.apache.pinot.segment.spi.AggregationFunctionType;


public class FunnelMaxStepAggregationFunction extends FunnelBaseAggregationFunction<Integer> {

  public FunnelMaxStepAggregationFunction(List<ExpressionContext> arguments) {
    super(arguments);
  }

  @Override
  public AggregationFunctionType getType() {
    return AggregationFunctionType.FUNNELMAXSTEP;
  }

  @Override
  public DataSchema.ColumnDataType getFinalResultColumnType() {
    return DataSchema.ColumnDataType.INT;
  }

  @Override
  public Integer extractFinalResult(PriorityQueue<FunnelStepEvent> stepEvents) {
    int finalMaxStep = 0;
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
  public Integer mergeFinalResult(Integer finalResult1, Integer finalResult2) {
    return Math.max(finalResult1, finalResult2);
  }
}
