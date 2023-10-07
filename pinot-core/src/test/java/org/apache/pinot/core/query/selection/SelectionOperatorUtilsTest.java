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
package org.apache.pinot.core.query.selection;

import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.core.query.request.context.utils.QueryContextConverterUtils;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;


public class SelectionOperatorUtilsTest {

  @Test
  public void testGetResultTableColumnIndices() {
    // Select * without order-by
    QueryContext queryContext = QueryContextConverterUtils.getQueryContext("SELECT * FROM testTable");
    DataSchema dataSchema = new DataSchema(new String[]{"col1", "col2", "col3"}, new ColumnDataType[]{
        ColumnDataType.INT, ColumnDataType.LONG, ColumnDataType.DOUBLE
    });
    assertEquals(SelectionOperatorUtils.getResultTableColumnIndices(queryContext, dataSchema), new int[]{0, 1, 2});

    // Select * with all segments pruned
    dataSchema = new DataSchema(new String[]{"*"}, new ColumnDataType[]{ColumnDataType.STRING});
    assertEquals(SelectionOperatorUtils.getResultTableColumnIndices(queryContext, dataSchema), new int[]{0});

    // Select * with order-by but LIMIT 0
    queryContext = QueryContextConverterUtils.getQueryContext("SELECT * FROM testTable ORDER BY col1 LIMIT 0");
    dataSchema = new DataSchema(new String[]{"col1", "col2", "col3"}, new ColumnDataType[]{
        ColumnDataType.INT, ColumnDataType.LONG, ColumnDataType.DOUBLE
    });
    assertEquals(SelectionOperatorUtils.getResultTableColumnIndices(queryContext, dataSchema), new int[]{0, 1, 2});

    // Select columns without order-by
    queryContext = QueryContextConverterUtils.getQueryContext("SELECT col1 + 1, col2 + 2 FROM testTable");
    // Intentionally make data schema not matching the string representation of the expression
    dataSchema = new DataSchema(new String[]{"add(col1+1)", "add(col2+2)"}, new ColumnDataType[]{
        ColumnDataType.DOUBLE, ColumnDataType.DOUBLE
    });
    assertEquals(SelectionOperatorUtils.getResultTableColumnIndices(queryContext, dataSchema), new int[]{0, 1});

    // Select duplicate columns without order-by
    queryContext = QueryContextConverterUtils.getQueryContext("SELECT col1 + 1, col2 + 2, col1 + 1 FROM testTable");
    // Intentionally make data schema not matching the string representation of the expression
    dataSchema = new DataSchema(new String[]{"add(col1+1)", "add(col2+2)"}, new ColumnDataType[]{
        ColumnDataType.DOUBLE, ColumnDataType.DOUBLE
    });
    assertEquals(SelectionOperatorUtils.getResultTableColumnIndices(queryContext, dataSchema), new int[]{0, 1, 0});

    // Select * with order-by
    queryContext = QueryContextConverterUtils.getQueryContext("SELECT * FROM testTable ORDER BY col3");
    dataSchema = new DataSchema(new String[]{"col3", "col1", "col2"}, new ColumnDataType[]{
        ColumnDataType.DOUBLE, ColumnDataType.INT, ColumnDataType.LONG
    });
    assertEquals(SelectionOperatorUtils.getResultTableColumnIndices(queryContext, dataSchema), new int[]{1, 2, 0});

    // Select * ordering on function
    queryContext = QueryContextConverterUtils.getQueryContext("SELECT * FROM testTable ORDER BY col1 + col2");
    // Intentionally make data schema not matching the string representation of the expression
    dataSchema = new DataSchema(new String[]{"add(col1+col2)", "col1", "col2", "col3"}, new ColumnDataType[]{
        ColumnDataType.DOUBLE, ColumnDataType.INT, ColumnDataType.LONG, ColumnDataType.DOUBLE
    });
    assertEquals(SelectionOperatorUtils.getResultTableColumnIndices(queryContext, dataSchema), new int[]{1, 2, 3});

    // Select * ordering on both column and function
    queryContext = QueryContextConverterUtils.getQueryContext("SELECT * FROM testTable ORDER BY col1 + col2, col2");
    // Intentionally make data schema not matching the string representation of the expression
    dataSchema = new DataSchema(new String[]{"add(col1+col2)", "col2", "col1", "col3"}, new ColumnDataType[]{
        ColumnDataType.DOUBLE, ColumnDataType.LONG, ColumnDataType.INT, ColumnDataType.DOUBLE
    });
    assertEquals(SelectionOperatorUtils.getResultTableColumnIndices(queryContext, dataSchema), new int[]{2, 1, 3});

    // Select columns with order-by
    queryContext = QueryContextConverterUtils.getQueryContext(
        "SELECT col1 + 1, col3, col2 + 2 FROM testTable ORDER BY col2 + 2, col4");
    // Intentionally make data schema not matching the string representation of the expression
    dataSchema = new DataSchema(new String[]{"add(col2+2)", "col4", "add(col1+1)", "col3"}, new ColumnDataType[]{
        ColumnDataType.DOUBLE, ColumnDataType.STRING, ColumnDataType.DOUBLE, ColumnDataType.DOUBLE
    });
    assertEquals(SelectionOperatorUtils.getResultTableColumnIndices(queryContext, dataSchema), new int[]{2, 3, 0});

    // Select duplicate columns with order-by
    queryContext = QueryContextConverterUtils.getQueryContext(
        "SELECT col1 + 1, col2 + 2, col1 + 1 FROM testTable ORDER BY col2 + 2, col4");
    // Intentionally make data schema not matching the string representation of the expression
    dataSchema = new DataSchema(new String[]{"add(col2+2)", "col4", "add(col1+1)"}, new ColumnDataType[]{
        ColumnDataType.DOUBLE, ColumnDataType.STRING, ColumnDataType.DOUBLE
    });
    assertEquals(SelectionOperatorUtils.getResultTableColumnIndices(queryContext, dataSchema), new int[]{2, 0, 2});
  }

  @Test
  public void testGetResultTableDataSchema() {
    // Select * without order-by
    QueryContext queryContext = QueryContextConverterUtils.getQueryContext("SELECT * FROM testTable");
    DataSchema dataSchema = new DataSchema(new String[]{"col1", "col2", "col3"}, new ColumnDataType[]{
        ColumnDataType.INT, ColumnDataType.LONG, ColumnDataType.DOUBLE
    });
    assertEquals(SelectionOperatorUtils.getResultTableDataSchema(queryContext, dataSchema),
        new DataSchema(new String[]{"col1", "col2", "col3"}, new ColumnDataType[]{
            ColumnDataType.INT, ColumnDataType.LONG, ColumnDataType.DOUBLE
        }));

    // Select * with all segments pruned
    dataSchema = new DataSchema(new String[]{"*"}, new ColumnDataType[]{ColumnDataType.STRING});
    assertEquals(SelectionOperatorUtils.getResultTableDataSchema(queryContext, dataSchema),
        new DataSchema(new String[]{"*"}, new ColumnDataType[]{ColumnDataType.STRING}));

    // Select * with order-by but LIMIT 0
    queryContext = QueryContextConverterUtils.getQueryContext("SELECT * FROM testTable ORDER BY col1 LIMIT 0");
    dataSchema = new DataSchema(new String[]{"col1", "col2", "col3"}, new ColumnDataType[]{
        ColumnDataType.INT, ColumnDataType.LONG, ColumnDataType.DOUBLE
    });
    assertEquals(SelectionOperatorUtils.getResultTableDataSchema(queryContext, dataSchema),
        new DataSchema(new String[]{"col1", "col2", "col3"}, new ColumnDataType[]{
            ColumnDataType.INT, ColumnDataType.LONG, ColumnDataType.DOUBLE
        }));

    // Select columns without order-by
    queryContext = QueryContextConverterUtils.getQueryContext("SELECT col1 + 1, col2 + 2 FROM testTable");
    // Intentionally make data schema not matching the string representation of the expression
    dataSchema = new DataSchema(new String[]{"add(col1+1)", "add(col2+2)"}, new ColumnDataType[]{
        ColumnDataType.DOUBLE, ColumnDataType.DOUBLE
    });
    assertEquals(SelectionOperatorUtils.getResultTableDataSchema(queryContext, dataSchema),
        new DataSchema(new String[]{"plus(col1,1)", "plus(col2,2)"}, new ColumnDataType[]{
            ColumnDataType.DOUBLE, ColumnDataType.DOUBLE
        }));

    // Select duplicate columns without order-by
    queryContext = QueryContextConverterUtils.getQueryContext("SELECT col1 + 1, col2 + 2, col1 + 1 FROM testTable");
    // Intentionally make data schema not matching the string representation of the expression
    dataSchema = new DataSchema(new String[]{"add(col1+1)", "add(col2+2)"}, new ColumnDataType[]{
        ColumnDataType.DOUBLE, ColumnDataType.DOUBLE
    });
    assertEquals(SelectionOperatorUtils.getResultTableDataSchema(queryContext, dataSchema),
        new DataSchema(new String[]{"plus(col1,1)", "plus(col2,2)", "plus(col1,1)"}, new ColumnDataType[]{
            ColumnDataType.DOUBLE, ColumnDataType.DOUBLE, ColumnDataType.DOUBLE
        }));

    // Select * with order-by
    queryContext = QueryContextConverterUtils.getQueryContext("SELECT * FROM testTable ORDER BY col3");
    dataSchema = new DataSchema(new String[]{"col3", "col1", "col2"}, new ColumnDataType[]{
        ColumnDataType.DOUBLE, ColumnDataType.INT, ColumnDataType.LONG
    });
    assertEquals(SelectionOperatorUtils.getResultTableDataSchema(queryContext, dataSchema),
        new DataSchema(new String[]{"col1", "col2", "col3"}, new ColumnDataType[]{
            ColumnDataType.INT, ColumnDataType.LONG, ColumnDataType.DOUBLE
        }));

    // Select * ordering on function
    queryContext = QueryContextConverterUtils.getQueryContext("SELECT * FROM testTable ORDER BY col1 + col2");
    // Intentionally make data schema not matching the string representation of the expression
    dataSchema = new DataSchema(new String[]{"add(col1+col2)", "col1", "col2", "col3"}, new ColumnDataType[]{
        ColumnDataType.DOUBLE, ColumnDataType.INT, ColumnDataType.LONG, ColumnDataType.DOUBLE
    });
    assertEquals(SelectionOperatorUtils.getResultTableDataSchema(queryContext, dataSchema),
        new DataSchema(new String[]{"col1", "col2", "col3"}, new ColumnDataType[]{
            ColumnDataType.INT, ColumnDataType.LONG, ColumnDataType.DOUBLE
        }));

    // Select * ordering on both column and function
    queryContext = QueryContextConverterUtils.getQueryContext("SELECT * FROM testTable ORDER BY col1 + col2, col2");
    // Intentionally make data schema not matching the string representation of the expression
    dataSchema = new DataSchema(new String[]{"add(col1+col2)", "col2", "col1", "col3"}, new ColumnDataType[]{
        ColumnDataType.DOUBLE, ColumnDataType.LONG, ColumnDataType.INT, ColumnDataType.DOUBLE
    });
    assertEquals(SelectionOperatorUtils.getResultTableDataSchema(queryContext, dataSchema),
        new DataSchema(new String[]{"col1", "col2", "col3"}, new ColumnDataType[]{
            ColumnDataType.INT, ColumnDataType.LONG, ColumnDataType.DOUBLE
        }));

    // Select columns with order-by
    queryContext = QueryContextConverterUtils.getQueryContext(
        "SELECT col1 + 1, col3, col2 + 2 FROM testTable ORDER BY col2 + 2, col4");
    // Intentionally make data schema not matching the string representation of the expression
    dataSchema = new DataSchema(new String[]{"add(col2+2)", "col4", "add(col1+1)", "col3"}, new ColumnDataType[]{
        ColumnDataType.DOUBLE, ColumnDataType.STRING, ColumnDataType.DOUBLE, ColumnDataType.DOUBLE
    });
    assertEquals(SelectionOperatorUtils.getResultTableDataSchema(queryContext, dataSchema),
        new DataSchema(new String[]{"plus(col1,1)", "col3", "plus(col2,2)"}, new ColumnDataType[]{
            ColumnDataType.DOUBLE, ColumnDataType.DOUBLE, ColumnDataType.DOUBLE
        }));

    // Select columns with order-by, all the segments are pruned on the server side
    // Intentionally make data schema not matching the string representation of the expression
    dataSchema = new DataSchema(new String[]{"add(col1+1)", "col3", "add(col2+2)"}, new ColumnDataType[]{
        ColumnDataType.STRING, ColumnDataType.STRING, ColumnDataType.STRING
    });
    assertEquals(SelectionOperatorUtils.getResultTableDataSchema(queryContext, dataSchema),
        new DataSchema(new String[]{"plus(col1,1)", "col3", "plus(col2,2)"}, new ColumnDataType[]{
            ColumnDataType.STRING, ColumnDataType.STRING, ColumnDataType.STRING
        }));

    // Select duplicate columns with order-by
    queryContext = QueryContextConverterUtils.getQueryContext(
        "SELECT col1 + 1, col2 + 2, col1 + 1 FROM testTable ORDER BY col2 + 2, col4");
    // Intentionally make data schema not matching the string representation of the expression
    dataSchema = new DataSchema(new String[]{"add(col2+2)", "col4", "add(col1+1)"}, new ColumnDataType[]{
        ColumnDataType.DOUBLE, ColumnDataType.STRING, ColumnDataType.DOUBLE
    });
    assertEquals(SelectionOperatorUtils.getResultTableDataSchema(queryContext, dataSchema),
        new DataSchema(new String[]{"plus(col1,1)", "plus(col2,2)", "plus(col1,1)"}, new ColumnDataType[]{
            ColumnDataType.DOUBLE, ColumnDataType.DOUBLE, ColumnDataType.DOUBLE
        }));

    // Select duplicate columns with order-by, all the segments are pruned on the server side
    // Intentionally make data schema not matching the string representation of the expression
    dataSchema = new DataSchema(new String[]{"add(col1+1)", "add(col2+2)", "add(col1+1)"}, new ColumnDataType[]{
        ColumnDataType.STRING, ColumnDataType.STRING, ColumnDataType.STRING
    });
    assertEquals(SelectionOperatorUtils.getResultTableDataSchema(queryContext, dataSchema),
        new DataSchema(new String[]{"plus(col1,1)", "plus(col2,2)", "plus(col1,1)"}, new ColumnDataType[]{
            ColumnDataType.STRING, ColumnDataType.STRING, ColumnDataType.STRING
        }));
  }
}
