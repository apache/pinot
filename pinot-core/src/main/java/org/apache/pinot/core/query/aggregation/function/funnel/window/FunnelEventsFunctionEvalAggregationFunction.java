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

import com.google.common.base.Preconditions;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.stream.Collectors;
import org.apache.pinot.common.CustomObject;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.core.common.BlockValSet;
import org.apache.pinot.core.query.aggregation.AggregationResultHolder;
import org.apache.pinot.core.query.aggregation.ObjectAggregationResultHolder;
import org.apache.pinot.core.query.aggregation.function.AggregationFunction;
import org.apache.pinot.core.query.aggregation.function.funnel.FunnelStepEvent;
import org.apache.pinot.core.query.aggregation.function.funnel.FunnelStepEventWithExtraFields;
import org.apache.pinot.core.query.aggregation.groupby.GroupByResultHolder;
import org.apache.pinot.core.query.aggregation.groupby.ObjectGroupByResultHolder;
import org.apache.pinot.segment.spi.AggregationFunctionType;


public class FunnelEventsFunctionEvalAggregationFunction
    implements AggregationFunction<PriorityQueue<FunnelStepEventWithExtraFields>, ObjectArrayList<String>> {
  private final static int INTERMEDIATE_RESULT_SERDE_VERSION = 0;

  protected final ExpressionContext _timestampExpression;
  protected final long _windowSize;
  protected final List<ExpressionContext> _stepExpressions;
  protected final FunnelBaseAggregationFunction.FunnelModes _modes = new FunnelBaseAggregationFunction.FunnelModes();
  protected final int _numSteps;
  protected final int _numExtraFields;
  protected final List<ExpressionContext> _extraExpressions;
  protected long _maxStepDuration = 0L;

  public FunnelEventsFunctionEvalAggregationFunction(List<ExpressionContext> arguments) {
    int numArguments = arguments.size();
    Preconditions.checkArgument(numArguments > 3,
        "FUNNEL_EVENTS_FUNCTION_EVAL expects >= 4 arguments, got: %s. The function can be used as "
            + getType().getName() + "(timestampExpression, windowSize, numberSteps, stepExpression, "
            + "[stepExpression, ..], [mode, [mode, ... ]])",
        numArguments);
    _timestampExpression = arguments.get(0);
    _windowSize = arguments.get(1).getLiteral().getLongValue();
    Preconditions.checkArgument(_windowSize > 0, "Window size must be > 0");
    _numSteps = arguments.get(2).getLiteral().getIntValue();
    Preconditions.checkArgument(numArguments >= 3 + _numSteps,
        "FUNNEL_EVENTS_FUNCTION_EVAL expects >= " + (3 + _numSteps)
            + " arguments, got: %s. The function can be used as "
            + getType().getName() + "(timestampExpression, windowSize, numberSteps, stepExpression, "
            + "[stepExpression, ..], [extraArgument/mode, [extraArgument/mode, ... ]])",
        numArguments);
    _stepExpressions = arguments.subList(3, 3 + _numSteps);
    _numExtraFields = arguments.get(3 + _numSteps).getLiteral().getIntValue();
    Preconditions.checkArgument(numArguments >= 4 + _numSteps + _numExtraFields,
        "FUNNEL_EVENTS_FUNCTION_EVAL expects >= " + (4 + _numSteps + _numExtraFields)
            + " arguments, got: %s. The function can be used as "
            + getType().getName() + "(timestampExpression, windowSize, numberSteps, stepExpression, "
            + "[stepExpression, ..], [extraArgument/mode, [extraArgument/mode, ... ]])",
        numArguments);
    _extraExpressions = arguments.subList(4 + _numSteps, 4 + _numSteps + _numExtraFields);

    for (int i = 4 + _numSteps + _numExtraFields; i < numArguments; i++) {
      String extraArgument = arguments.get(i).getLiteral().getStringValue().toUpperCase();
      String[] parsedExtraArguments = extraArgument.split("=");
      if (parsedExtraArguments.length == 2) {
        String key = parsedExtraArguments[0].toUpperCase();
        switch (key) {
          case FunnelBaseAggregationFunction.FunnelConfigs.MAX_STEP_DURATION:
            _maxStepDuration = Long.parseLong(parsedExtraArguments[1]);
            Preconditions.checkArgument(_maxStepDuration > 0, "MaxStepDuration must be > 0");
            break;
          case FunnelBaseAggregationFunction.FunnelConfigs.MODE:
            for (String modeStr : parsedExtraArguments[1].split(",")) {
              _modes.add(FunnelBaseAggregationFunction.Mode.valueOf(modeStr.trim()));
            }
            break;
          default:
            throw new IllegalArgumentException("Unrecognized arguments: " + extraArgument);
        }
        continue;
      }
      try {
        _modes.add(FunnelBaseAggregationFunction.Mode.valueOf(extraArgument));
      } catch (Exception e) {
        throw new RuntimeException("Unrecognized extra argument for funnel function: " + extraArgument, e);
      }
    }
  }

  @Override
  public String getResultColumnName() {
    return
        String.format("%s(%d)(%s,%s,%s)", getType().getName(), _windowSize, _timestampExpression.toString(),
            _stepExpressions.stream().map(ExpressionContext::toString).collect(Collectors.joining(",")),
            (_numExtraFields > 0 ? ", " + _extraExpressions.stream().map(ExpressionContext::toString)
                .collect(Collectors.joining(",")) : "")
        );
  }

  @Override
  public List<ExpressionContext> getInputExpressions() {
    List<ExpressionContext> inputs = new ArrayList<>(1 + _numSteps + _numExtraFields);
    inputs.add(_timestampExpression);
    inputs.addAll(_stepExpressions);
    inputs.addAll(_extraExpressions);
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
    PriorityQueue<FunnelStepEventWithExtraFields> stepEvents = aggregationResultHolder.getResult();
    if (stepEvents == null) {
      stepEvents = new PriorityQueue<>();
      aggregationResultHolder.setValue(stepEvents);
    }
    List<Object> extraFieldsBlocks = getExtraFieldsBlocks(blockValSetMap);
    for (int i = 0; i < length; i++) {
      boolean stepFound = false;
      for (int j = 0; j < _numSteps; j++) {
        if (stepBlocks.get(j)[i] == 1) {
          List<Object> extraFields = extractExtraFields(extraFieldsBlocks, i);
          stepEvents.add(new FunnelStepEventWithExtraFields(new FunnelStepEvent(timestampBlock[i], j), extraFields));
          stepFound = true;
          break;
        }
      }
      // If the mode is KEEP_ALL and no step is found, add a dummy step event with step -1
      if (_modes.hasKeepAll() && !stepFound) {
        List<Object> extraFields = extractExtraFields(extraFieldsBlocks, i);
        stepEvents.add(new FunnelStepEventWithExtraFields(new FunnelStepEvent(timestampBlock[i], -1), extraFields));
      }
    }
  }

  private List<Object> getExtraFieldsBlocks(Map<ExpressionContext, BlockValSet> blockValSetMap) {
    List<Object> extraFieldsBlocks = new ArrayList<>(_numExtraFields);
    for (ExpressionContext extraExpression : _extraExpressions) {
      BlockValSet blockValSet = blockValSetMap.get(extraExpression);
      switch (blockValSet.getValueType()) {
        case INT:
          extraFieldsBlocks.add(blockValSet.getIntValuesSV());
          break;
        case LONG:
        case TIMESTAMP:
          extraFieldsBlocks.add(blockValSet.getLongValuesSV());
          break;
        case FLOAT:
          extraFieldsBlocks.add(blockValSet.getFloatValuesSV());
          break;
        case DOUBLE:
          extraFieldsBlocks.add(blockValSet.getDoubleValuesSV());
          break;
        case STRING:
          extraFieldsBlocks.add(blockValSet.getStringValuesSV());
          break;
        default:
          throw new IllegalArgumentException("Unsupported data type for extra field: " + extraExpression + " - "
              + blockValSet.getValueType());
      }
    }
    return extraFieldsBlocks;
  }

  private List<Object> extractExtraFields(List<Object> extraFieldsBlocks, int i) {
    List<Object> extraFields = new ArrayList<>(_numExtraFields);
    for (Object extraFieldsBlock : extraFieldsBlocks) {
      switch (extraFieldsBlock.getClass().getComponentType().getSimpleName()) {
        case "int":
          extraFields.add(((int[]) extraFieldsBlock)[i]);
          break;
        case "long":
          extraFields.add(((long[]) extraFieldsBlock)[i]);
          break;
        case "float":
          extraFields.add(((float[]) extraFieldsBlock)[i]);
          break;
        case "double":
          extraFields.add(((double[]) extraFieldsBlock)[i]);
          break;
        case "String":
          extraFields.add(((String[]) extraFieldsBlock)[i]);
          break;
        default:
          throw new IllegalArgumentException(
              "Unsupported data type for extra field: " + extraFieldsBlock.getClass().getComponentType()
                  .getSimpleName());
      }
    }
    return extraFields;
  }

  @Override
  public void aggregateGroupBySV(int length, int[] groupKeyArray, GroupByResultHolder groupByResultHolder,
      Map<ExpressionContext, BlockValSet> blockValSetMap) {
    long[] timestampBlock = blockValSetMap.get(_timestampExpression).getLongValuesSV();
    List<int[]> stepBlocks = new ArrayList<>(_numSteps);
    for (ExpressionContext stepExpression : _stepExpressions) {
      stepBlocks.add(blockValSetMap.get(stepExpression).getIntValuesSV());
    }
    List<Object> extraFieldsBlocks = getExtraFieldsBlocks(blockValSetMap);
    for (int i = 0; i < length; i++) {
      int groupKey = groupKeyArray[i];
      boolean stepFound = false;
      for (int j = 0; j < _numSteps; j++) {
        if (stepBlocks.get(j)[i] == 1) {
          PriorityQueue<FunnelStepEventWithExtraFields> stepEvents = getFunnelStepEvents(groupByResultHolder, groupKey);
          List<Object> extraFields = extractExtraFields(extraFieldsBlocks, i);
          stepEvents.add(new FunnelStepEventWithExtraFields(new FunnelStepEvent(timestampBlock[i], j), extraFields));
          stepFound = true;
          break;
        }
      }
      // If the mode is KEEP_ALL and no step is found, add a dummy step event with step -1
      if (_modes.hasKeepAll() && !stepFound) {
        PriorityQueue<FunnelStepEventWithExtraFields> stepEvents = getFunnelStepEvents(groupByResultHolder, groupKey);
        List<Object> extraFields = extractExtraFields(extraFieldsBlocks, i);
        stepEvents.add(new FunnelStepEventWithExtraFields(new FunnelStepEvent(timestampBlock[i], -1), extraFields));
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
    List<Object> extraFieldsBlocks = getExtraFieldsBlocks(blockValSetMap);
    for (int i = 0; i < length; i++) {
      int[] groupKeys = groupKeysArray[i];
      boolean stepFound = false;
      for (int j = 0; j < _numSteps; j++) {
        if (stepBlocks.get(j)[i] == 1) {
          for (int groupKey : groupKeys) {
            PriorityQueue<FunnelStepEventWithExtraFields> stepEvents =
                getFunnelStepEvents(groupByResultHolder, groupKey);
            List<Object> extraFields = extractExtraFields(extraFieldsBlocks, i);
            stepEvents.add(new FunnelStepEventWithExtraFields(new FunnelStepEvent(timestampBlock[i], j), extraFields));
          }
          stepFound = true;
          break;
        }
      }
      // If the mode is KEEP_ALL and no step is found, add a dummy step event with step -1
      if (_modes.hasKeepAll() && !stepFound) {
        for (int groupKey : groupKeys) {
          PriorityQueue<FunnelStepEventWithExtraFields> stepEvents = getFunnelStepEvents(groupByResultHolder, groupKey);
          List<Object> extraFields = extractExtraFields(extraFieldsBlocks, i);
          stepEvents.add(new FunnelStepEventWithExtraFields(new FunnelStepEvent(timestampBlock[i], -1), extraFields));
        }
      }
    }
  }

  private static PriorityQueue<FunnelStepEventWithExtraFields> getFunnelStepEvents(
      GroupByResultHolder groupByResultHolder,
      int groupKey) {
    PriorityQueue<FunnelStepEventWithExtraFields> stepEvents = groupByResultHolder.getResult(groupKey);
    if (stepEvents == null) {
      stepEvents = new PriorityQueue<>();
      groupByResultHolder.setValueForKey(groupKey, stepEvents);
    }
    return stepEvents;
  }

  @Override
  public PriorityQueue<FunnelStepEventWithExtraFields> extractAggregationResult(
      AggregationResultHolder aggregationResultHolder) {
    return aggregationResultHolder.getResult();
  }

  @Override
  public PriorityQueue<FunnelStepEventWithExtraFields> extractGroupByResult(GroupByResultHolder groupByResultHolder,
      int groupKey) {
    return groupByResultHolder.getResult(groupKey);
  }

  @Override
  public PriorityQueue<FunnelStepEventWithExtraFields> merge(
      PriorityQueue<FunnelStepEventWithExtraFields> intermediateResult1,
      PriorityQueue<FunnelStepEventWithExtraFields> intermediateResult2) {
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
  public SerializedIntermediateResult serializeIntermediateResult(
      PriorityQueue<FunnelStepEventWithExtraFields> funnelStepEvents) {
    int numEvents = funnelStepEvents.size();
    List<byte[]> serializedEvents = new ArrayList<>(numEvents);
    long bufferSize = Integer.BYTES; // Start with size for number of events

    // First pass: Serialize each event and calculate total buffer size
    for (FunnelStepEventWithExtraFields funnelStepEvent : funnelStepEvents) {
      byte[] eventBytes = funnelStepEvent.getBytes(); // Costly operation, compute only once
      serializedEvents.add(eventBytes); // Store serialized form
      bufferSize += Integer.BYTES; // Add size for storing length
      bufferSize += eventBytes.length; // Add size of serialized content
    }

    // Ensure the total buffer size doesn't exceed 2GB
    Preconditions.checkState(bufferSize <= Integer.MAX_VALUE, "Buffer size exceeds 2GB");

    // Allocate buffer
    byte[] bytes = new byte[(int) bufferSize];
    ByteBuffer byteBuffer = ByteBuffer.wrap(bytes);

    // Second pass: Write data to the buffer
    byteBuffer.putInt(numEvents); // Write number of events
    for (byte[] eventBytes : serializedEvents) {
      byteBuffer.putInt(eventBytes.length); // Write length of each event
      byteBuffer.put(eventBytes); // Write event content
    }
    return new SerializedIntermediateResult(INTERMEDIATE_RESULT_SERDE_VERSION, bytes);
  }

  @Override
  public PriorityQueue<FunnelStepEventWithExtraFields> deserializeIntermediateResult(CustomObject customObject) {
    if (customObject.getType() == 0) {
      ByteBuffer byteBuffer = customObject.getBuffer();
      int size = byteBuffer.getInt();
      if (size == 0) {
        return new PriorityQueue<>();
      }
      PriorityQueue<FunnelStepEventWithExtraFields> funnelStepEvents = new PriorityQueue<>(size);
      for (int i = 0; i < size; i++) {
        int funnelStepEventWithExtraFieldsByteSize = byteBuffer.getInt();
        byte[] bytes = new byte[funnelStepEventWithExtraFieldsByteSize];
        byteBuffer.get(bytes);
        funnelStepEvents.add(new FunnelStepEventWithExtraFields(bytes));
      }
      return funnelStepEvents;
    }
    throw new IllegalArgumentException("Unsupported serialized intermediate result version: " + customObject.getType());
  }

  /**
   * Fill the sliding window with the events that fall into the window.
   * Note that the events from stepEvents are dequeued and added to the sliding window.
   * This method ensure the first event from the sliding window is the first step event.
   *
   * @param stepEvents    The priority queue of step events
   * @param slidingWindow The sliding window with events that fall into the window
   */
  protected void fillWindow(PriorityQueue<FunnelStepEventWithExtraFields> stepEvents,
      ArrayDeque<FunnelStepEventWithExtraFields> slidingWindow) {
    // Ensure for the sliding window, the first event is the first step
    while ((!slidingWindow.isEmpty()) && slidingWindow.peek().getFunnelStepEvent().getStep() != 0) {
      slidingWindow.pollFirst();
    }
    if (slidingWindow.isEmpty()) {
      while (!stepEvents.isEmpty() && stepEvents.peek().getFunnelStepEvent().getStep() != 0) {
        stepEvents.poll();
      }
      if (stepEvents.isEmpty()) {
        return;
      }
      slidingWindow.addLast(stepEvents.poll());
    }
    // SlidingWindow is not empty
    long windowStart = slidingWindow.peek().getFunnelStepEvent().getTimestamp();
    long windowEnd = windowStart + _windowSize;
    while (!stepEvents.isEmpty() && (stepEvents.peek().getFunnelStepEvent().getTimestamp() < windowEnd)) {
      if (_maxStepDuration > 0) {
        // When maxStepDuration > 0, we need to check if the event_to_add has a timestamp within the max duration
        // from the last event in the sliding window. If not, we break the loop.
        if (stepEvents.peek().getFunnelStepEvent().getTimestamp() - slidingWindow.getLast().getFunnelStepEvent()
            .getTimestamp() > _maxStepDuration) {
          break;
        }
      }
      slidingWindow.addLast(stepEvents.poll());
    }
  }

  @Override
  public String toExplainString() {
    //@formatter:off
    return getType().getName() + "{"
        + "timestampExpression=" + _timestampExpression
        + ", windowSize=" + _windowSize
        + ", stepExpressions=" + _stepExpressions
        + ", extraExpressions=" + _extraExpressions
        + '}';
    //@formatter:on
  }

  @Override
  public AggregationFunctionType getType() {
    return AggregationFunctionType.FUNNELEVENTSFUNCTIONEVAL;
  }

  @Override
  public DataSchema.ColumnDataType getFinalResultColumnType() {
    return DataSchema.ColumnDataType.STRING_ARRAY;
  }

  @Override
  public ObjectArrayList<String> extractFinalResult(PriorityQueue<FunnelStepEventWithExtraFields> stepEvents) {
    ObjectArrayList<String> finalResults = new ObjectArrayList<>();
    List<List<Object[]>> matchedFunnelEventsExtraFields = new ArrayList<>(_numExtraFields);
    if (stepEvents == null || stepEvents.isEmpty()) {
      return finalResults;
    }
    ArrayDeque<FunnelStepEventWithExtraFields> slidingWindow = new ArrayDeque<>();
    while (!stepEvents.isEmpty()) {
      fillWindow(stepEvents, slidingWindow);
      if (slidingWindow.isEmpty()) {
        break;
      }

      long windowStart = slidingWindow.peek().getFunnelStepEvent().getTimestamp();

      int maxStep = 0;
      long previousTimestamp = -1;
      for (FunnelStepEventWithExtraFields event : slidingWindow) {
        int currentEventStep = event.getFunnelStepEvent().getStep();
        // If the same condition holds for the sequence of events, then such repeating event interrupts further
        // processing.
        if (_modes.hasStrictDeduplication()) {
          if (currentEventStep == maxStep - 1) {
            maxStep = 0;
          }
        }
        // Don't allow interventions of other events. E.g. in the case of A->B->D->C, it stops finding A->B->C at the D
        // and the max event level is 2.
        if (_modes.hasStrictOrder()) {
          if (currentEventStep != maxStep) {
            maxStep = 0;
          }
        }
        // Apply conditions only to events with strictly increasing timestamps.
        if (_modes.hasStrictIncrease()) {
          if (previousTimestamp == event.getFunnelStepEvent().getTimestamp()) {
            continue;
          }
        }
        previousTimestamp = event.getFunnelStepEvent().getTimestamp();
        if (maxStep == currentEventStep) {
          maxStep++;
        }
        if (maxStep == _numSteps) {
          matchedFunnelEventsExtraFields.add(extractFunnelEventsExtraFields(slidingWindow));
          maxStep = 0;
          windowStart = event.getFunnelStepEvent().getTimestamp();
        }
      }
      if (!slidingWindow.isEmpty()) {
        slidingWindow.pollFirst();
      }
      // sliding window should pop until current event:
      while (!slidingWindow.isEmpty() && slidingWindow.peek().getFunnelStepEvent().getTimestamp() < windowStart) {
        slidingWindow.pollFirst();
      }
    }

    evalFunctionOnMatchedFunnelEvents(matchedFunnelEventsExtraFields, finalResults);
    return finalResults;
  }

  private void evalFunctionOnMatchedFunnelEvents(List<List<Object[]>> matchedFunnelEventsExtraFields,
      ObjectArrayList<String> finalResults) {
    StringBuilder arrayAssignments = new StringBuilder(Integer.toString(matchedFunnelEventsExtraFields.size()));
    for (List<Object[]> funnelEventsExtraField : matchedFunnelEventsExtraFields) {
      arrayAssignments.append(", ").append(funnelEventsExtraField.size() * _numExtraFields);
    }
    finalResults.add(arrayAssignments.toString());
    for (List<Object[]> matchedFunnelEventsExtraField : matchedFunnelEventsExtraFields) {
      for (Object[] extraFields : matchedFunnelEventsExtraField) {
        for (Object extraField : extraFields) {
          finalResults.add(extraField.toString());
        }
      }
    }
  }

  private List<Object[]> extractFunnelEventsExtraFields(ArrayDeque<FunnelStepEventWithExtraFields> slidingWindow) {
    List<Object[]> results = new ArrayList<>();
    int step = 0;
    for (FunnelStepEventWithExtraFields event : slidingWindow) {
      if (event.getFunnelStepEvent().getStep() == step) {
        Object[] extraFields = new Object[_numExtraFields];
        List<Object> extraFieldsList = event.getExtraFields();
        for (int i = 0; i < _numExtraFields; i++) {
          extraFields[i] = extraFieldsList.get(i);
        }
        results.add(extraFields);
        step++;
      }
    }
    return results;
  }

  @Override
  public ObjectArrayList<String> mergeFinalResult(ObjectArrayList<String> finalResult1,
      ObjectArrayList<String> finalResult2) {
    if (finalResult1 == null) {
      return finalResult2;
    }
    if (finalResult2 == null) {
      return finalResult1;
    }
    finalResult1.addAll(finalResult2);
    return finalResult1;
  }
}
