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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.core.operator.blocks.results.SelectionResultsBlock;
import org.apache.pinot.core.operator.combine.merger.SelectionOnlyResultsBlockMerger;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.core.query.request.context.utils.QueryContextConverterUtils;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class SelectionOnlyResultBlockMergerTest {

    @Test(dataProvider = "mergeBlocksDataProvider")
    public void testMergeResultsBlocks(String[] mergedColumns, Object[][] mergedData,
                                       String[] toMergeColumns, Object[][] toMergedData,
                                       String[] expectedColumns, QueryContext queryContext) {

        SelectionOnlyResultsBlockMerger merger = new SelectionOnlyResultsBlockMerger(queryContext);

        SelectionResultsBlock mergedBlock = createSelectionResultsBlock(mergedColumns, mergedData, queryContext);
        SelectionResultsBlock blockToMerge = createSelectionResultsBlock(toMergeColumns, toMergedData, queryContext);
        int expectedRowCount = Math.min(mergedBlock.getNumRows() + blockToMerge.getNumRows(), queryContext.getLimit());

        merger.mergeResultsBlocks(mergedBlock, blockToMerge);

        assert new HashSet<>(Arrays.asList(mergedBlock.getDataSchema().getColumnNames()))
                .equals(new HashSet<>(Arrays.asList(expectedColumns)));
        assert mergedBlock.getNumRows() == expectedRowCount;
    }

    @DataProvider(name = "mergeBlocksDataProvider")
    public Object[][] mergeBlocksDataProvider() {
        String query = "SELECT * FROM testTable";
        QueryContext queryContext = QueryContextConverterUtils.getQueryContext(query);
        queryContext.setIsSelectStarQuery(true);

        return new Object[][] {
                // Same columns, same data
                {new String[]{"col1", "col2"}, new Object[][] {{"row1_col1", "row1_col2"}, {"row2_col1", "row2_col2"}},
                 new String[]{"col1", "col2"}, new Object[][] {{"row1_col1", "row1_col2"}, {"row2_col1", "row2_col2"}},
                 new String[]{"col1", "col2"}, queryContext},
                // Second block has more columns
                {new String[]{"col1", "col2"}, new Object[][] {{"row1_col1", "row1_col2"}, {"row2_col1", "row2_col2"}},
                 new String[]{"col1", "col2", "col3"}, new Object[][] {{"row1_col1", "row1_col2", "row1_col3"},
                        {"row2_col1", "row2_col2", "row2_col3"}},
                 new String[]{"col1", "col2"}, queryContext},
                // First block has more columns
                {new String[]{"col1", "col2", "col3"}, new Object[][] {{"row1_col1", "row1_col2", "row1_col3"},
                        {"row2_col1", "row2_col2", "row2_col3"}},
                new String[]{"col1", "col2"}, new Object[][] {{"row1_col1", "row1_col2"}, {"row2_col1", "row2_col2"}},
                new String[]{"col1", "col2"}, queryContext}
        };
    }

    private SelectionResultsBlock createSelectionResultsBlock(String[] columns, Object[][] data, QueryContext context) {
        DataSchema schema = new DataSchema(columns, new DataSchema.ColumnDataType[columns.length]);
        Arrays.fill(schema.getColumnDataTypes(), DataSchema.ColumnDataType.STRING);
        ArrayList<Object[]> rows = new ArrayList<>(Arrays.asList(data));
        return new SelectionResultsBlock(schema, rows, context);
    }
}
