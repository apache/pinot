package org.apache.pinot.core.query.aggregation.function.funnel;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.datasketches.theta.Sketch;
import org.apache.datasketches.theta.UpdateSketch;
import org.apache.datasketches.theta.UpdateSketchBuilder;


class ThetaSketchResultExtractionStrategy implements ResultExtractionStrategy<UpdateSketch[], List<Sketch>> {
  private static final Sketch EMPTY_SKETCH = new UpdateSketchBuilder().build().compact();

  protected final int _numSteps;

  ThetaSketchResultExtractionStrategy(int numSteps) {
    _numSteps = numSteps;
  }

  @Override
  public List<Sketch> extractIntermediateResult(UpdateSketch[] stepsSketches) {
    if (stepsSketches == null) {
      return Collections.nCopies(_numSteps, EMPTY_SKETCH);
    }
    return Arrays.asList(stepsSketches);
  }
}
