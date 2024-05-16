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
import java.util.List;
import org.apache.pinot.core.common.Operator;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.segment.spi.datasource.DataSourceMetadata;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertTrue;

public class FilterOperatorRewriterTest {


    @Test
    public void testNestedANDOR() {
        // input: Expression OR (ScanBase AND (jsonMatch OR bitMap))
        // output: Expression OR ((jsonMatch OR bitMap) AND ScanBase)
        BitmapBasedFilterOperator bitmap = mock(BitmapBasedFilterOperator.class);
        JsonMatchFilterOperator jsonMatch = mock(JsonMatchFilterOperator.class);
        DataSourceMetadata ds = mock(DataSourceMetadata.class);
        when(ds.isSingleValue()).thenReturn(true);
        ScanBasedFilterOperator scanBasedFilterOperator = mock(ScanBasedFilterOperator.class);
        when(scanBasedFilterOperator.getDataSourceMetadata()).thenReturn(ds);
        ExpressionFilterOperator expr = mock(ExpressionFilterOperator.class);
        QueryContext qc = mock(QueryContext.class);
        when(qc.isSkipScanFilterReorder()).thenReturn(true);
        OrFilterOperator or1 = new OrFilterOperator(Arrays.asList(jsonMatch, bitmap), null, 10, true);
        AndFilterOperator and = new AndFilterOperator(Arrays.asList(or1, scanBasedFilterOperator), null, 10, true);
        OrFilterOperator or2 = new OrFilterOperator(Arrays.asList(expr, and), null, 10, true);
        int prio = FilterOperatorRewriter.reorder(qc, or2);
        Assert.assertEquals(prio, PrioritizedFilterOperator.EXPRESSION_PRIORITY);
        List<? extends Operator> kids = or2.getChildOperators();
        assertTrue(kids.get(0) instanceof ExpressionFilterOperator);
        assertTrue(kids.get(1) instanceof AndFilterOperator);
        kids = ((AndFilterOperator) kids.get(1)).getChildOperators();
        assertTrue(kids.get(0) instanceof OrFilterOperator);
        assertTrue(kids.get(1) instanceof ScanBasedFilterOperator);
        kids = ((OrFilterOperator) kids.get(0)).getChildOperators();
        assertTrue(kids.get(0) instanceof JsonMatchFilterOperator);
        assertTrue(kids.get(1) instanceof BitmapBasedFilterOperator);
    }
}
