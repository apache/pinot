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
  public UpdateSketch[] createAggregationResult(Dictionary dictionary) {
    final UpdateSketch[] stepsSketches = new UpdateSketch[_numSteps];
    for (int n = 0; n < _numSteps; n++) {
      stepsSketches[n] = _updateSketchBuilder.build();
    }
    return stepsSketches;
  }

  @Override
  void add(Dictionary dictionary, UpdateSketch[] stepsSketches, int step, int correlationId) {
    final UpdateSketch sketch = stepsSketches[step];
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
        throw new IllegalStateException("Illegal CORRELATED_BY column data type for FUNNEL_COUNT aggregation function: "
            + dictionary.getValueType());
    }
  }
}
