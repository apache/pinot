package org.apache.pinot.core.plan;

import org.apache.pinot.core.operator.query.DictionaryBasedDistinctOperator;
import org.apache.pinot.core.query.aggregation.function.AggregationFunction;
import org.apache.pinot.core.query.aggregation.function.DistinctAggregationFunction;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.index.reader.Dictionary;

import java.util.HashMap;
import java.util.Map;

/**
 * Execute a DISTINCT operation using dictionary based plan
 */
public class DictionaryBasedDistinctPlanNode implements PlanNode {
    private final IndexSegment _indexSegment;
    private final DistinctAggregationFunction _distinctAggregationFunction;
    private final Map<String, Dictionary> _dictionaryMap;
    private final TransformPlanNode _transformPlanNode;

    /**
     * Constructor for the class.
     *
     * @param indexSegment Segment to process
     * @param queryContext Query context
     */
    public DictionaryBasedDistinctPlanNode(IndexSegment indexSegment, QueryContext queryContext) {
        _indexSegment = indexSegment;
        AggregationFunction[] aggregationFunctions = queryContext.getAggregationFunctions();

        assert aggregationFunctions != null && aggregationFunctions.length == 1
                && aggregationFunctions[0] instanceof DistinctAggregationFunction;

        _distinctAggregationFunction = (DistinctAggregationFunction) aggregationFunctions[0];

        _dictionaryMap = new HashMap<>();

        String column = _distinctAggregationFunction.getInputExpressions().get(0).getIdentifier();
        _dictionaryMap.computeIfAbsent(column, k -> _indexSegment.getDataSource(k).getDictionary());

        _transformPlanNode =
                new TransformPlanNode(_indexSegment, queryContext, _distinctAggregationFunction.getInputExpressions(),
                        DocIdSetPlanNode.MAX_DOC_PER_CALL);
    }

    @Override
    public DictionaryBasedDistinctOperator run() {
        return new DictionaryBasedDistinctOperator(_indexSegment, _distinctAggregationFunction, _dictionaryMap,
                _indexSegment.getSegmentMetadata().getTotalDocs(), _transformPlanNode.run());
    }
}
