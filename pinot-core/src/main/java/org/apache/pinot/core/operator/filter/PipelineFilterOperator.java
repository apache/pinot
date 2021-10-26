package org.apache.pinot.core.operator.filter;

import org.apache.pinot.core.operator.blocks.FilterBlock;

public class PipelineFilterOperator extends BaseFilterOperator {
    private static final String OPERATOR_NAME = "ExpressionFilterOperator";

    private final BaseFilterOperator _mainFilterOperator;
    private BaseFilterOperator[] _filterClausePredicates;

    public PipelineFilterOperator(BaseFilterOperator mainFilterOperator,
                                  BaseFilterOperator[] filterClausePredicates) {
        _mainFilterOperator = mainFilterOperator;
        _filterClausePredicates = filterClausePredicates;
    }


    @Override
    protected FilterBlock getNextBlock() {
        
    }

    @Override
    public String getOperatorName() {
        return OPERATOR_NAME;
    }
}