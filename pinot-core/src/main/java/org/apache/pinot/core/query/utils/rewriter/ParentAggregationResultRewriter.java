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
package org.apache.pinot.core.query.utils.rewriter;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.core.query.aggregation.utils.ParentAggregationFunctionResultObject;
import org.apache.pinot.spi.utils.CommonConstants;


/**
 * Used in aggregation and group-by queries with aggregation functions.
 * Use the result of parent aggregation functions to populate the result of child aggregation functions.
 * This implementation is based on the column names of the result schema.
 * The result column name of a parent aggregation function has the following format:
 * CommonConstants.RewriterConstants.PARENT_AGGREGATION_NAME_PREFIX + aggregationFunctionType + FunctionID
 * The result column name of corresponding child aggregation function has the following format:
 * CHILD_AGGREGATION_NAME_PREFIX + aggregationFunctionType + operands + CHILD_AGGREGATION_SEPERATOR
 * + aggregationFunctionType + parent FunctionID + CHILD_KEY_SEPERATOR + column key in parent function
 * This approach will not work with `AS` clauses as they alter the column names.
 * TODO: Add support for `AS` clauses.
 */
public class ParentAggregationResultRewriter implements ResultRewriter {
  public ParentAggregationResultRewriter() {
  }

  private static Map<String, ChildFunctionMapping> createChildFunctionMapping(DataSchema schema, Object[] row) {
    Map<String, ChildFunctionMapping> childFunctionMapping = new HashMap<>();
    for (int i = 0; i < schema.size(); i++) {
      String columnName = schema.getColumnName(i);
      if (columnName.startsWith(CommonConstants.RewriterConstants.PARENT_AGGREGATION_NAME_PREFIX)) {
        ParentAggregationFunctionResultObject parent = (ParentAggregationFunctionResultObject) row[i];

        DataSchema nestedSchema = parent.getSchema();
        for (int j = 0; j < nestedSchema.size(); j++) {
          String childColumnKey = nestedSchema.getColumnName(j);
          String originalChildFunctionKey =
              columnName.substring(CommonConstants.RewriterConstants.PARENT_AGGREGATION_NAME_PREFIX.length())
                  + CommonConstants.RewriterConstants.CHILD_KEY_SEPERATOR + childColumnKey;
          // aggregationFunctionType + childFunctionID + CHILD_KEY_SEPERATOR + childFunctionKeyInParent
          childFunctionMapping.put(originalChildFunctionKey, new ChildFunctionMapping(parent, j, i));
        }
      }
    }
    return childFunctionMapping;
  }

  public RewriterResult rewrite(DataSchema dataSchema, List<Object[]> rows) {

    int numParentAggregationFunctions = 0;
    // Count the number of parent aggregation functions
    for (int i = 0; i < dataSchema.size(); i++) {
      if (dataSchema.getColumnName(i).startsWith(CommonConstants.RewriterConstants.PARENT_AGGREGATION_NAME_PREFIX)) {
        numParentAggregationFunctions++;
      }
    }

    if (numParentAggregationFunctions == 0) {
      // no change to the result
      return new RewriterResult(dataSchema, rows);
    }

    Map<String, ChildFunctionMapping> childFunctionMapping = null;
    if (!rows.isEmpty()) {
      // Create a mapping from the child aggregation function name to the child aggregation function
      childFunctionMapping = createChildFunctionMapping(dataSchema, rows.get(0));
    }

    String[] newColumnNames = new String[dataSchema.size() - numParentAggregationFunctions];
    DataSchema.ColumnDataType[] newColumnDataTypes
        = new DataSchema.ColumnDataType[dataSchema.size() - numParentAggregationFunctions];

    // Create a mapping from the function offset in the final aggregation result
    // to its own/parent function offset in the original aggregation result
    Map<Integer, Integer> aggregationFunctionIndexMapping = new HashMap<>();
    // Create a set of the result indices of the child aggregation functions
    Set<Integer> childAggregationFunctionIndices = new HashSet<>();
    // Create a mapping from the result aggregation function index to the nested index of the
    // child aggregation function in the parent aggregation function
    Map<Integer, Integer> childAggregationFunctionNestedIndexMapping = new HashMap<>();
    // Create a set of the result indices of the parent aggregation functions
    Set<Integer> parentAggregationFunctionIndices = new HashSet<>();

    for (int i = 0, j = 0; i < dataSchema.size(); i++) {
      String columnName = dataSchema.getColumnName(i);
      // Skip the parent aggregation functions
      if (columnName.startsWith(CommonConstants.RewriterConstants.PARENT_AGGREGATION_NAME_PREFIX)) {
        parentAggregationFunctionIndices.add(i);
        continue;
      }

      // for child aggregation functions and regular columns in the result
      // create a new schema and populate the new column names and data types
      // also populate the offset mappings used to rewrite the result
      if (columnName.startsWith(CommonConstants.RewriterConstants.CHILD_AGGREGATION_NAME_PREFIX)) {
        // This is a child column of a parent aggregation function
        String childAggregationFunctionNameWithKey =
            columnName.substring(CommonConstants.RewriterConstants.CHILD_AGGREGATION_NAME_PREFIX.length());
        String[] s = childAggregationFunctionNameWithKey
            .split(CommonConstants.RewriterConstants.CHILD_AGGREGATION_SEPERATOR);
        newColumnNames[j] = s[0];

        if (childFunctionMapping == null) {
          newColumnDataTypes[j] = DataSchema.ColumnDataType.STRING;
          j++;
          continue;
        }
        ChildFunctionMapping childFunction = childFunctionMapping.get(s[1]);
        newColumnDataTypes[j] = childFunction.getParent().getSchema()
            .getColumnDataType(childFunction.getNestedOffset());

        childAggregationFunctionNestedIndexMapping.put(j, childFunction.getNestedOffset());
        childAggregationFunctionIndices.add(j);
        aggregationFunctionIndexMapping.put(j, childFunction.getOffset());
      } else {
        // This is a regular column
        newColumnNames[j] = columnName;
        newColumnDataTypes[j] = dataSchema.getColumnDataType(i);

        aggregationFunctionIndexMapping.put(j, i);
      }
      j++;
    }

    DataSchema newDataSchema = new DataSchema(newColumnNames, newColumnDataTypes);
    List<Object[]> newRows = new ArrayList<>();

    for (Object[] row : rows) {
      int maxRows = parentAggregationFunctionIndices.stream().map(k -> {
        ParentAggregationFunctionResultObject parentAggregationFunctionResultObject =
            (ParentAggregationFunctionResultObject) row[k];
        return parentAggregationFunctionResultObject.getNumberOfRows();
      }).max(Integer::compareTo).orElse(0);
      maxRows = maxRows == 0 ? 1 : maxRows;

      List<Object[]> newRowsBuffer = new ArrayList<>();
      for (int rowIter = 0; rowIter < maxRows; rowIter++) {
        Object[] newRow = new Object[newDataSchema.size()];
        for (int fieldIter = 0; fieldIter < newDataSchema.size(); fieldIter++) {
          // If the field is a child aggregation function, extract the value from the parent result
          if (childAggregationFunctionIndices.contains(fieldIter)) {
            int offset = aggregationFunctionIndexMapping.get(fieldIter);
            int nestedOffset = childAggregationFunctionNestedIndexMapping.get(fieldIter);
            ParentAggregationFunctionResultObject parentAggregationFunctionResultObject =
                (ParentAggregationFunctionResultObject) row[offset];
            // If the parent result has more rows than the current row, extract the value from the row
            if (rowIter < parentAggregationFunctionResultObject.getNumberOfRows()) {
              newRow[fieldIter] = parentAggregationFunctionResultObject.getField(rowIter, nestedOffset);
            } else {
              newRow[fieldIter] = null;
            }
          } else { // If the field is a regular column, extract the value from the row, only the first row has value
            newRow[fieldIter] = row[aggregationFunctionIndexMapping.get(fieldIter)];
          }
        }
        newRowsBuffer.add(newRow);
      }
      newRows.addAll(newRowsBuffer);
    }
    return new RewriterResult(newDataSchema, newRows);
  }

  /**
   * Mapping from child function key to
   * 1. the parent result object,
   * 2. offset of the parent result column in original result row,
   * 3. the nested offset of the child function result in the parent data block
   *
   * For example, for a list of aggregation functions result:
   *            0                      1                    2                   3
   *            |                      |                    |                   |
   * "child_exprmin(a, b, x) ,child_exprmin(a, b, y), child_exprmin(a, b, z), parent_exprmin(a, b, x, y, z)"
   *                                                                                           |  |  |
   *                                                                                           0  1  2
   * offset of the parent of child_exprmin(a, b, y) is 3
   * nested offset is child_exprmin(a, b, y) is 1
   */
  private static class ChildFunctionMapping {
    private final ParentAggregationFunctionResultObject _parent;
    private final int _nestedOffset;
    private final int _offset;

    public ChildFunctionMapping(ParentAggregationFunctionResultObject parent, int nestedOffset, int offset) {
      _parent = parent;
      _nestedOffset = nestedOffset;
      _offset = offset;
    }

    public int getOffset() {
      return _offset;
    }

    public ParentAggregationFunctionResultObject getParent() {
      return _parent;
    }

    public int getNestedOffset() {
      return _nestedOffset;
    }
  }
}
