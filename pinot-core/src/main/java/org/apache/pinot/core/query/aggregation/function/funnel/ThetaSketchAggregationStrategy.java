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

import java.util.List;
import org.apache.datasketches.theta.UpdatableThetaSketch;
import org.apache.datasketches.theta.UpdatableThetaSketchBuilder;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.segment.spi.index.reader.Dictionary;


/**
 * Aggregation strategy leveraging theta sketch algebra (unions/intersections).
 */
class ThetaSketchAggregationStrategy extends AggregationStrategy<UpdatableThetaSketch[]> {
  final UpdatableThetaSketchBuilder _updateSketchBuilder;

  public ThetaSketchAggregationStrategy(List<ExpressionContext> stepExpressions,
      List<ExpressionContext> correlateByExpressions, int nominalEntries) {
    super(stepExpressions, correlateByExpressions);
    _updateSketchBuilder = new UpdatableThetaSketchBuilder().setNominalEntries(nominalEntries);
  }

  @Override
  public UpdatableThetaSketch[] createAggregationResult(Dictionary dictionary) {
    final UpdatableThetaSketch[] stepsSketches = new UpdatableThetaSketch[_numSteps];
    for (int n = 0; n < _numSteps; n++) {
      stepsSketches[n] = _updateSketchBuilder.build();
    }
    return stepsSketches;
  }

  @Override
  public UpdatableThetaSketch[] createAggregationResultMultiKey(Dictionary[] dictionaries) {
    final UpdatableThetaSketch[] stepsSketches = new UpdatableThetaSketch[_numSteps];
    for (int n = 0; n < _numSteps; n++) {
      stepsSketches[n] = _updateSketchBuilder.build();
    }
    return stepsSketches;
  }

  @Override
  void add(Dictionary dictionary, UpdatableThetaSketch[] stepsSketches, int step, int correlationId) {
    final UpdatableThetaSketch sketch = stepsSketches[step];
    switch (dictionary.getValueType()) {
      case INT:
        sketch.update(dictionary.getIntValue(correlationId));
        break;
      case LONG:
        sketch.update(dictionary.getLongValue(correlationId));
        break;
      case FLOAT:
        sketch.update(dictionary.getFloatValue(correlationId));
        break;
      case DOUBLE:
        sketch.update(dictionary.getDoubleValue(correlationId));
        break;
      case STRING:
        sketch.update(dictionary.getStringValue(correlationId));
        break;
      default:
        throw new IllegalStateException(
            "Illegal CORRELATED_BY column data type for FUNNEL_COUNT aggregation function: "
                + dictionary.getValueType());
    }
  }

  @Override
  void addMultiKey(UpdatableThetaSketch[] stepsSketches, int step, Dictionary[] dictionaries,
      int[] correlationDictIds) {
    stepsSketches[step].update(DictIdsWrapper.toCompositeString(dictionaries, correlationDictIds));
  }
}
