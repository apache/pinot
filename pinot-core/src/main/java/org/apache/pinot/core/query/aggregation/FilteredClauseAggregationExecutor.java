package org.apache.pinot.core.query.aggregation;

import java.util.ArrayList;
import java.util.List;
import org.apache.pinot.core.operator.blocks.CombinedTransformBlock;
import org.apache.pinot.core.operator.blocks.TransformBlock;
import org.apache.pinot.core.query.aggregation.function.AggregationFunction;
import org.apache.pinot.core.query.aggregation.function.AggregationFunctionUtils;
import org.apache.pinot.core.query.aggregation.function.FilterableAggregation;


public class FilteredClauseAggregationExecutor implements AggregationExecutor {
  protected final AggregationFunction[] _aggregationFunctions;
  protected final AggregationResultHolder[] _aggregationResultHolders;

  public FilteredClauseAggregationExecutor(AggregationFunction[] aggregationFunctions) {
    _aggregationFunctions = aggregationFunctions;
    int numAggregationFunctions = aggregationFunctions.length;
    _aggregationResultHolders = new AggregationResultHolder[numAggregationFunctions];
    for (int i = 0; i < numAggregationFunctions; i++) {
      _aggregationResultHolders[i] = aggregationFunctions[i].createAggregationResultHolder();
    }
  }

  @Override
  public void aggregate(TransformBlock transformBlock) {
    if (!(transformBlock instanceof CombinedTransformBlock)) {
      throw new IllegalArgumentException("FilteredClauseAggregationExecutor only works"
          + "with CombinedTransformBlock");
    }

    CombinedTransformBlock combinedTransformBlock = (CombinedTransformBlock) transformBlock;
    List<TransformBlock> transformBlockList = combinedTransformBlock.getTransformBlockList();
    int numAggregations = _aggregationFunctions.length;
    int transformListOffset = 0;

    for (int i = 0; i < numAggregations; i++) {
      AggregationFunction aggregationFunction = _aggregationFunctions[i];

      if (!(aggregationFunction instanceof FilterableAggregation)) {
        throw new IllegalStateException("Non filterable aggregation seen");
      }

      if (((FilterableAggregation) aggregationFunction).isFilteredAggregation()) {
        TransformBlock innerTransformBlock = transformBlockList.get(transformListOffset++);

        if (innerTransformBlock != null) {
          int length = innerTransformBlock.getNumDocs();
          aggregationFunction.aggregate(length, _aggregationResultHolders[i],
              AggregationFunctionUtils.getBlockValSetMap(aggregationFunction, innerTransformBlock));
        }
      } else {
        TransformBlock innerTransformBlock = combinedTransformBlock.getNonFilteredAggBlock();
        int length = innerTransformBlock.getNumDocs();

        aggregationFunction.aggregate(length, _aggregationResultHolders[i],
            AggregationFunctionUtils.getBlockValSetMap(aggregationFunction, innerTransformBlock));
      }
    }
  }

  @Override
  public List<Object> getResult() {
    int numFunctions = _aggregationFunctions.length;
    List<Object> aggregationResults = new ArrayList<>(numFunctions);
    for (int i = 0; i < numFunctions; i++) {
      aggregationResults.add(_aggregationFunctions[i].extractAggregationResult(_aggregationResultHolders[i]));
    }
    return aggregationResults;
  }
}
