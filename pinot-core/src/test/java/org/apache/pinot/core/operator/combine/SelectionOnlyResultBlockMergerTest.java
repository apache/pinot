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
import java.util.Comparator;
import java.util.List;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.core.operator.blocks.results.SelectionResultsBlock;
import org.apache.pinot.core.operator.combine.merger.ResultsBlockMerger;
import org.apache.pinot.core.operator.combine.merger.SelectionOnlyResultsBlockMerger;
import org.apache.pinot.core.operator.combine.merger.SelectionOrderByResultsBlockMerger;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.core.query.request.context.utils.QueryContextConverterUtils;
import org.apache.pinot.core.query.utils.OrderByComparatorFactory;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class SelectionOnlyResultBlockMergerTest {

    @Test(dataProvider = "mergeBlocksDataProvider")
    public void testMergeResultsBlocks(String[] mergedColumns, Object[][] mergedData,
                                       String[] toMergeColumns, Object[][] toMergeData,
                                       String[] expectedColumns, Object[][] expectedRows,
                                       QueryContext queryContext) {
        ResultsBlockMerger<SelectionResultsBlock> merger;
        if (queryContext.getOrderByExpressions() != null) {
            merger = new SelectionOrderByResultsBlockMerger(queryContext);
        } else {
            merger = new SelectionOnlyResultsBlockMerger(queryContext);
        }
        SelectionResultsBlock mergedBlock = createSelectionResultsBlock(mergedColumns, mergedData, queryContext);
        SelectionResultsBlock blockToMerge = createSelectionResultsBlock(toMergeColumns, toMergeData, queryContext);
        // Perform merge
        merger.mergeResultsBlocks(mergedBlock, blockToMerge);
        // Validate schema
        assert Arrays.equals(mergedBlock.getDataSchema().getColumnNames(), expectedColumns);
        // Validate rows
        List<Object[]> actualRows = mergedBlock.getRows();
        assert actualRows.size() == expectedRows.length;
        for (int i = 0; i < expectedRows.length; i++) {
            assert Arrays.equals(actualRows.get(i), expectedRows[i]);
        }
    }

    @DataProvider(name = "mergeBlocksDataProvider")
    public Object[][] mergeBlocksDataProvider() {
        List<Object[]> testCases = new ArrayList<>();
        String query = "SELECT * FROM testTable; SET isSelectStarQuery=true";
        QueryContext queryContext = QueryContextConverterUtils.getQueryContext(query);

        // Base columns
        String[] baseColumns = {"col1", "col2", "col3"};

        // Case 1: Both blocks have exactly base columns
        {
            Object[][] mergedData = {
                    {"row1_col1", "row1_col2", "row1_col3"},
                    {"row2_col1", "row2_col2", "row2_col3"}
            };
            Object[][] toMergeData = {
                    {"row3_col1", "row3_col2", "row3_col3"},
                    {"row4_col1", "row4_col2", "row4_col3"}
            };
            Object[][] expectedRows = {
                    {"row1_col1", "row1_col2", "row1_col3"},
                    {"row2_col1", "row2_col2", "row2_col3"},
                    {"row3_col1", "row3_col2", "row3_col3"},
                    {"row4_col1", "row4_col2", "row4_col3"}
            };
            testCases.add(new Object[]{baseColumns, mergedData, baseColumns, toMergeData, baseColumns,
                    expectedRows, queryContext});
        }

        // Case 2: Second block has extra column (col4)
        {
            Object[][] mergedData = {
                    {"row1_col1", "row1_col2", "row1_col3"},
                    {"row2_col1", "row2_col2", "row2_col3"}
            };
            String[] toMergeColumns = new String[]{"col1", "col2", "col3", "col4"};
            Object[][] toMergeData = {
                    {"row3_col1", "row3_col2", "row3_col3", "row3_col4"},
                    {"row4_col1", "row4_col2", "row4_col3", "row4_col4"}
            };
            Object[][] expectedRows = {
                    {"row1_col1", "row1_col2", "row1_col3"},
                    {"row2_col1", "row2_col2", "row2_col3"},
                    {"row3_col1", "row3_col2", "row3_col3"},
                    {"row4_col1", "row4_col2", "row4_col3"}
            };
            testCases.add(new Object[]{baseColumns, mergedData, toMergeColumns, toMergeData, baseColumns,
                    expectedRows, queryContext});
        }

        // Case 3: First block has extra column (col4)
        {
            String[] mergedColumns = new String[]{"col1", "col2", "col3", "col4"};
            Object[][] mergedData = {
                    {"row1_col1", "row1_col2", "row1_col3", "row1_col4"},
                    {"row2_col1", "row2_col2", "row2_col3", "row2_col4"}
            };
            String[] toMergeColumns = baseColumns;
            Object[][] toMergeData = {
                    {"row3_col1", "row3_col2", "row3_col3"},
                    {"row4_col1", "row4_col2", "row4_col3"}
            };
            String[] expectedColumns = baseColumns;
            Object[][] expectedRows = {
                    {"row1_col1", "row1_col2", "row1_col3"},
                    {"row2_col1", "row2_col2", "row2_col3"},
                    {"row3_col1", "row3_col2", "row3_col3"},
                    {"row4_col1", "row4_col2", "row4_col3"}
            };
            testCases.add(new Object[]{mergedColumns, mergedData, toMergeColumns, toMergeData, expectedColumns,
                    expectedRows, queryContext});
        }
        // Case 4: Both have different extra columns
        {
            String[] mergedColumns = new String[]{"col1", "col2", "col3", "col5"};
            Object[][] mergedData = {
                    {"row1_col1", "row1_col2", "row1_col3", "row1_col5"},
                    {"row2_col1", "row2_col2", "row2_col3", "row2_col5"}
            };
            String[] toMergeColumns = new String[]{"col3", "col4", "col5"};
            Object[][] toMergeData = {
                    {"row3_col3", "row3_col4", "row3_col5"},
                    {"row4_col3", "row4_col4", "row4_col5"}
            };
            String[] expectedColumns = new String[]{"col3", "col5"};
            Object[][] expectedRows = {
                    {"row1_col3", "row1_col5"},
                    {"row2_col3", "row2_col5"},
                    {"row3_col3", "row3_col5"},
                    {"row4_col3", "row4_col5"}
            };
            testCases.add(new Object[]{mergedColumns, mergedData, toMergeColumns, toMergeData, expectedColumns,
                    expectedRows, queryContext});
        }
        query = "SELECT * FROM testTable ORDER BY col3; SET isSelectStarQuery=true";
        queryContext = QueryContextConverterUtils.getQueryContext(query);
        {
            String[] mergedColumns = new String[]{"col1", "co12", "col3", "col5"};
            Object[][] sortedMergedData = {
                    {1, 2, 3, 4},
                    {5, 6, 7, 8},
            };
            String[] toMergeColumns = new String[]{"col3", "col5", "col6"};
            Object[][] sortedToMergeData = {
                    {4, 5, 6},
                    {8, 9, 10},
            };
            String[] expectedColumns = new String[]{"col3", "col5"};
            Object[][] expectedRows = {
                    {3, 4},
                    {4, 5},
                    {7, 8},
                    {8, 9}
            };
            testCases.add(new Object[]{mergedColumns, sortedMergedData, toMergeColumns, sortedToMergeData,
                    expectedColumns, expectedRows, queryContext});
        }
        return testCases.toArray(new Object[0][]);
    }

    private SelectionResultsBlock createSelectionResultsBlock(String[] columns, Object[][] data, QueryContext context) {
        DataSchema schema = new DataSchema(columns, new DataSchema.ColumnDataType[columns.length]);
        Arrays.fill(schema.getColumnDataTypes(), mapJavaTypeToColumnDataType(data[0][0].getClass()));
        ArrayList<Object[]> rows = new ArrayList<>(Arrays.asList(data));
        Comparator<Object[]> comparator = null;
        if (context.getOrderByExpressions() != null) {
            comparator = OrderByComparatorFactory.getComparator(context.getOrderByExpressions(), false);
        }
        return new SelectionResultsBlock(schema, rows, comparator, context);
    }

    private static DataSchema.ColumnDataType mapJavaTypeToColumnDataType(Class<?> clazz) {
        if (clazz == Integer.class) {
            return DataSchema.ColumnDataType.INT;
        } else if (clazz == Long.class) {
            return DataSchema.ColumnDataType.LONG;
        } else if (clazz == Float.class) {
            return DataSchema.ColumnDataType.FLOAT;
        } else if (clazz == Double.class) {
            return DataSchema.ColumnDataType.DOUBLE;
        } else if (clazz == String.class) {
            return DataSchema.ColumnDataType.STRING;
        } else if (clazz == Boolean.class) {
            return DataSchema.ColumnDataType.BOOLEAN;
        } else if (clazz == byte[].class) {
            return DataSchema.ColumnDataType.BYTES;
        }
        throw new IllegalArgumentException("Unsupported Java type: " + clazz);
    }
}
