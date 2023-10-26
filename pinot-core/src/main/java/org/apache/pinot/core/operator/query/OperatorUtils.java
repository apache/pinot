package org.apache.pinot.core.operator.query;

import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.core.operator.BaseProjectOperator;
import org.apache.pinot.core.operator.filter.BaseFilterOperator;
import org.apache.pinot.core.plan.DocIdSetPlanNode;
import org.apache.pinot.core.plan.FilterPlanNode;
import org.apache.pinot.core.plan.ProjectPlanNode;
import org.apache.pinot.core.query.aggregation.function.AggregationFunction;
import org.apache.pinot.core.query.aggregation.function.AggregationFunctionUtils;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.core.startree.CompositePredicateEvaluator;
import org.apache.pinot.core.startree.StarTreeUtils;
import org.apache.pinot.core.startree.plan.StarTreeProjectPlanNode;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.index.startree.AggregationFunctionColumnPair;
import org.apache.pinot.segment.spi.index.startree.StarTreeV2;


public class OperatorUtils {
  public static BaseProjectOperator<?> getProjectionOperator(
      QueryContext queryContext,
      IndexSegment indexSegment,
      AggregationFunction[] aggregationFunctions,
      FilterPlanNode filterPlanNode,
      BaseFilterOperator filterOperator,
      @Nullable List<ExpressionContext> groupByExpressionsList) {

    ExpressionContext[] groupByExpressions = null;
    if (groupByExpressionsList != null) {
      groupByExpressions = groupByExpressionsList.toArray(new ExpressionContext[0]);
    }

    List<StarTreeV2> starTrees = indexSegment.getStarTrees();
    if (starTrees != null && !queryContext.isSkipStarTree() && !queryContext.isNullHandlingEnabled()) {
      AggregationFunctionColumnPair[] aggregationFunctionColumnPairs =
          StarTreeUtils.extractAggregationFunctionPairs(aggregationFunctions);
      if (aggregationFunctionColumnPairs != null) {
        Map<String, List<CompositePredicateEvaluator>> predicateEvaluatorsMap =
            StarTreeUtils.extractPredicateEvaluatorsMap(indexSegment, queryContext.getFilter(),
                filterPlanNode.getPredicateEvaluators());
        if (predicateEvaluatorsMap != null) {
          for (StarTreeV2 starTreeV2 : starTrees) {
            if (StarTreeUtils.isFitForStarTree(starTreeV2.getMetadata(), aggregationFunctionColumnPairs,
                groupByExpressions, predicateEvaluatorsMap.keySet())) {
              return new StarTreeProjectPlanNode(queryContext, starTreeV2, aggregationFunctionColumnPairs,
                  groupByExpressions, predicateEvaluatorsMap).run();
            }
          }
        }
      }
    }

    Set<ExpressionContext> expressionsToTransform =
        AggregationFunctionUtils.collectExpressionsToTransform(aggregationFunctions, groupByExpressionsList);
    return new ProjectPlanNode(indexSegment, queryContext, expressionsToTransform, DocIdSetPlanNode.MAX_DOC_PER_CALL,
        filterOperator).run();
  }
}
