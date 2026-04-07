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
import org.apache.datasketches.theta.UpdateSketch;
import org.apache.datasketches.theta.UpdateSketchBuilder;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.segment.spi.index.reader.Dictionary;


/**
 * Aggregation strategy leveraging theta sketch algebra (unions/intersections).
 */
class ThetaSketchAggregationStrategy extends AggregationStrategy<UpdateSketch[]> {
  final UpdateSketchBuilder _updateSketchBuilder;

  public ThetaSketchAggregationStrategy(List<ExpressionContext> stepExpressions,
      List<ExpressionContext> correlateByExpressions, int nominalEntries) {
    super(stepExpressions, correlateByExpressions);
    _updateSketchBuilder = new UpdateSketchBuilder().setNominalEntries(nominalEntries);
  }

  @Override
  public UpdateSketch[] createAggregationResult(Dictionary[] dictionaries) {
    final UpdateSketch[] stepsSketches = new UpdateSketch[_numSteps];
    for (int n = 0; n < _numSteps; n++) {
      stepsSketches[n] = _updateSketchBuilder.build();
    }
    return stepsSketches;
  }

  @Override
  void add(UpdateSketch[] stepsSketches, int step, Dictionary[] dictionaries, int[] correlationDictIds) {
    final UpdateSketch sketch = stepsSketches[step];
    if (dictionaries.length == 1) {
      addSingleKey(sketch, dictionaries[0], correlationDictIds[0]);
    } else {
      addCompositeKey(sketch, dictionaries, correlationDictIds);
    }
  }

  private void addSingleKey(UpdateSketch sketch, Dictionary dictionary, int correlationId) {
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

  private void addCompositeKey(UpdateSketch sketch, Dictionary[] dictionaries, int[] correlationDictIds) {
    sketch.update(DictIdsWrapper.toCompositeString(dictionaries, correlationDictIds));
  }
}
