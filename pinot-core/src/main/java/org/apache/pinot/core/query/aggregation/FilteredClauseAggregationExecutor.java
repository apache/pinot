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
    List<AggregationFunction> filteredAggFunctions = new ArrayList<>();
    List<AggregationFunction> nonFilteredAggFunctions = new ArrayList<>();
    int aggHolderOffset = 0;

    for (AggregationFunction aggregationFunction : _aggregationFunctions) {
      if (!(aggregationFunction instanceof FilterableAggregation)) {
        throw new IllegalStateException("Non filterable aggregation seen");
      }

      if (((FilterableAggregation) aggregationFunction).isFilteredAggregation()) {
        filteredAggFunctions.add(aggregationFunction);
      } else {
        nonFilteredAggFunctions.add(aggregationFunction);
      }
    }

    int numFilteredAggregationFunctions = filteredAggFunctions.size();

    for (int i = 0; i < numFilteredAggregationFunctions; i++) {
      AggregationFunction aggregationFunction = filteredAggFunctions.get(i);
      TransformBlock innerTransformBlock = transformBlockList.get(i);

      if (innerTransformBlock != null) {
        int length = innerTransformBlock.getNumDocs();
        aggregationFunction.aggregate(length, _aggregationResultHolders[aggHolderOffset++],
            AggregationFunctionUtils.getBlockValSetMap(aggregationFunction, innerTransformBlock));
      }
    }

    int numNonFilteredAggregationFunctions = nonFilteredAggFunctions.size();

    for (int i = 0; i < numNonFilteredAggregationFunctions; i++) {
      int length = combinedTransformBlock.getNonFilteredAggBlock().getNumDocs();
        AggregationFunction aggregationFunction = nonFilteredAggFunctions.get(i);
        aggregationFunction.aggregate(length, _aggregationResultHolders[aggHolderOffset++],
            AggregationFunctionUtils.getBlockValSetMap(aggregationFunction, transformBlock));
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
