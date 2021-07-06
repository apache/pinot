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
package org.apache.pinot.core.operator.combine;

import it.unimi.dsi.fastutil.doubles.DoubleOpenHashSet;
import it.unimi.dsi.fastutil.floats.FloatOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import it.unimi.dsi.fastutil.objects.ObjectOpenHashSet;
import java.util.AbstractCollection;
import java.util.List;
import java.util.concurrent.ExecutorService;
import org.apache.pinot.core.common.Operator;
import org.apache.pinot.core.operator.blocks.IntermediateResultsBlock;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.spi.utils.ByteArray;

/**
 * Operator which combines partial results for DISTINCT operation when being executed
 * using the dictionary
 */
public class DistinctUsingDictionaryCombineOperator extends BaseCombineOperator {
    private static final String OPERATOR_NAME = "DistinctUsingDictionaryCombineOperator";

    private final boolean _hasOrderBy;

    public DistinctUsingDictionaryCombineOperator(List<Operator> operators, QueryContext queryContext, ExecutorService executorService,
                                   long endTimeMs) {
        super(operators, queryContext, executorService, endTimeMs);
        _hasOrderBy = queryContext.getOrderByExpressions() != null;
    }

    @Override
    public String getOperatorName() {
        return OPERATOR_NAME;
    }

    @Override
    protected boolean isQuerySatisfied(IntermediateResultsBlock resultsBlock) {
        if (_hasOrderBy) {
            return false;
        }
        List<Object> result = resultsBlock.getAggregationResult();
        assert result != null && result.size() == 1 && result.get(0) instanceof AbstractCollection;
        AbstractCollection abstractCollection = (AbstractCollection) result.get(0);
        return abstractCollection.size() >= _queryContext.getLimit();
    }

    @Override
    protected void mergeResultsBlocks(IntermediateResultsBlock mergedBlock, IntermediateResultsBlock blockToMerge) {
        // TODO: Use a separate way to represent DISTINCT instead of aggregation.
        List<Object> mergedResults = mergedBlock.getAggregationResult();
        assert mergedResults != null && mergedResults.size() == 1 && mergedResults.get(0) instanceof AbstractCollection;

        List<Object> resultsToMerge = blockToMerge.getAggregationResult();
        assert resultsToMerge != null && resultsToMerge.size() == 1 && resultsToMerge.get(0) instanceof AbstractCollection;

        if (mergedResults.get(0) instanceof IntOpenHashSet) {
            assert resultsToMerge.get(0) instanceof IntOpenHashSet;
            IntOpenHashSet mergedSet = (IntOpenHashSet) mergedResults.get(0);
            IntOpenHashSet toMergeSet = (IntOpenHashSet) resultsToMerge.get(0);

            mergedSet.addAll(toMergeSet);
        } else if (mergedResults.get(0) instanceof LongOpenHashSet) {
            assert resultsToMerge.get(0) instanceof LongOpenHashSet;
            LongOpenHashSet mergedSet = (LongOpenHashSet) mergedResults.get(0);
            LongOpenHashSet toMergeSet = (LongOpenHashSet) resultsToMerge.get(0);

            mergedSet.addAll(toMergeSet);
        } else if (mergedResults.get(0) instanceof FloatOpenHashSet) {
            assert resultsToMerge.get(0) instanceof FloatOpenHashSet;
            FloatOpenHashSet mergedSet = (FloatOpenHashSet) mergedResults.get(0);
            FloatOpenHashSet toMergeSet = (FloatOpenHashSet) resultsToMerge.get(0);

             mergedSet.addAll(toMergeSet);
        } else if (mergedResults.get(0) instanceof DoubleOpenHashSet) {
            assert resultsToMerge.get(0) instanceof DoubleOpenHashSet;
            DoubleOpenHashSet mergedSet = (DoubleOpenHashSet) mergedResults.get(0);
            DoubleOpenHashSet toMergeSet = (DoubleOpenHashSet) resultsToMerge.get(0);

            mergedSet.addAll( toMergeSet);
        } else if (mergedResults.get(0) instanceof ObjectOpenHashSet) {
            assert resultsToMerge.get(0) instanceof ObjectOpenHashSet;

            ObjectOpenHashSet mergedSet = (ObjectOpenHashSet) mergedResults.get(0);
            ObjectOpenHashSet toMergeSet = (ObjectOpenHashSet) resultsToMerge.get(0);

            validateArgumentTypes(mergedSet, toMergeSet);

            mergedSet.addAll( toMergeSet);
        } else {
            throw new IllegalStateException();
        }
    }

    /**
     * Validate that merged set and set to be merged are of the same type when the sets are of ObjectOpenHashSet type
     */
    private void validateArgumentTypes(ObjectOpenHashSet mergedSet, ObjectOpenHashSet toMergeSet) {
        Object setObject = mergedSet.iterator().next();
        Object toMergeObject = toMergeSet.iterator().next();

        if (setObject instanceof String) {
            assert toMergeObject instanceof String;
        } else if (setObject instanceof ByteArray) {
            assert toMergeObject instanceof ByteArray;
        } else {
            throw new IllegalStateException("Unknown type encountered");
        }
    }
}
