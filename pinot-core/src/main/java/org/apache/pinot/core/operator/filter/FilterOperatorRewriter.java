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
package org.apache.pinot.core.operator.filter;

import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.pinot.core.common.Operator;
import org.apache.pinot.core.query.request.context.QueryContext;

public class FilterOperatorRewriter {

    private FilterOperatorRewriter() {
    }

    /**
     * The function should be invoked after FilterPlanNode#constructPhysicalOperator to reorder the filter operators
     * @param op
     * @return priority of the operator
     */
    public static int reorder(QueryContext qc, BaseFilterOperator op) {
        if (op instanceof EmptyFilterOperator || op instanceof MatchAllFilterOperator) {
            throw new RuntimeException("Not expected to encounter empty or matchAll operators");
        }
        if (op instanceof AndFilterOperator) {
            List<? extends Operator> kids = op.getChildOperators();
            int len = kids.size();
            // an array of (priority of child ops, the position of child ops)
            int[][] priorities = new int[len][2];
            int maxPriority = Integer.MIN_VALUE;
            for (int i = 0; i < len; i++) {
                BaseFilterOperator child = (BaseFilterOperator) kids.get(i);
                priorities[i][0] = reorder(qc, child);
                priorities[i][1] = i;
                maxPriority = Math.max(maxPriority, priorities[i][0]);
            }
            Arrays.sort(priorities, Comparator.comparingInt(a -> a[0]));
            Map<Operator, Integer> priorityMap = new HashMap<>();
            for (int i = 0; i < len; i++) {
                priorityMap.put(kids.get(priorities[i][1]), priorities[i][0]);
            }
            kids.sort(Comparator.comparingInt(priorityMap::get));
            return maxPriority;
        }
        if (op instanceof OrFilterOperator) {
            return op.getChildOperators().stream().map(child -> reorder(qc, (BaseFilterOperator) child))
                    .max(Integer::compareTo).orElseThrow();
        }
        if (op instanceof NotFilterOperator) {
            return reorder(qc, ((NotFilterOperator) op).getChildFilterOperator());
        }
        return getNonLogicalOperatorPriority(qc, op);
    }

    public static int getNonLogicalOperatorPriority(QueryContext queryContext, BaseFilterOperator op) {
        if (op instanceof SortedIndexBasedFilterOperator) {
            return PrioritizedFilterOperator.HIGH_PRIORITY;
        }
        if (op instanceof BitmapBasedFilterOperator) {
            return PrioritizedFilterOperator.MEDIUM_PRIORITY;
        }
        if (op instanceof RangeIndexBasedFilterOperator
                || op instanceof TextContainsFilterOperator
                || op instanceof TextMatchFilterOperator || op instanceof JsonMatchFilterOperator
                || op instanceof H3IndexFilterOperator
                || op instanceof H3InclusionIndexFilterOperator) {
            return PrioritizedFilterOperator.LOW_PRIORITY;
        }
        if (op instanceof ScanBasedFilterOperator) {
            int basePriority = PrioritizedFilterOperator.SCAN_PRIORITY;
            if (queryContext.isSkipScanFilterReorder()) {
                return basePriority;
            }

            if (((ScanBasedFilterOperator) op).getDataSourceMetadata().isSingleValue()) {
                return basePriority;
            } else {
                // Lower priority for multi-value column
                return basePriority + 50;
            }
        }
        if (op instanceof ExpressionFilterOperator) {
            return PrioritizedFilterOperator.EXPRESSION_PRIORITY;
        }
        return PrioritizedFilterOperator.UNKNOWN_FILTER_PRIORITY;
    }
}
