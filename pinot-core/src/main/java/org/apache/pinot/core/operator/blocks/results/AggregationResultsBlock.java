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
package org.apache.pinot.core.operator.blocks.results;

import it.unimi.dsi.fastutil.doubles.DoubleArrayList;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.List;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.common.utils.DataTable;
import org.apache.pinot.core.common.datatable.DataTableBuilder;
import org.apache.pinot.core.common.datatable.DataTableFactory;
import org.apache.pinot.core.query.aggregation.function.AggregationFunction;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.spi.utils.ByteArray;
import org.apache.pinot.spi.utils.NullValueUtils;
import org.roaringbitmap.RoaringBitmap;


/**
 * Results block for aggregation queries.
 */
@SuppressWarnings({"rawtypes", "unchecked"})
public class AggregationResultsBlock extends BaseResultsBlock {
  private final AggregationFunction[] _aggregationFunctions;
  private final List<Object> _results;

  public AggregationResultsBlock(AggregationFunction[] aggregationFunctions, List<Object> results) {
    _aggregationFunctions = aggregationFunctions;
    _results = results;
  }

  public AggregationFunction[] getAggregationFunctions() {
    return _aggregationFunctions;
  }

  public List<Object> getResults() {
    return _results;
  }

  @Override
  public DataTable getDataTable(QueryContext queryContext)
      throws Exception {
    boolean returnFinalResult = queryContext.isServerReturnFinalResult();

    // Extract result column name and type from each aggregation function
    int numColumns = _aggregationFunctions.length;
    String[] columnNames = new String[numColumns];
    ColumnDataType[] columnDataTypes = new ColumnDataType[numColumns];
    for (int i = 0; i < numColumns; i++) {
      AggregationFunction aggregationFunction = _aggregationFunctions[i];
      columnNames[i] = aggregationFunction.getColumnName();
      columnDataTypes[i] = returnFinalResult ? aggregationFunction.getFinalResultColumnType()
          : aggregationFunction.getIntermediateResultColumnType();
    }

    // Build the data table.
    DataTableBuilder dataTableBuilder =
        DataTableFactory.getDataTableBuilder(new DataSchema(columnNames, columnDataTypes));
    if (queryContext.isNullHandlingEnabled()) {
      RoaringBitmap[] nullBitmaps = new RoaringBitmap[numColumns];
      for (int i = 0; i < numColumns; i++) {
        nullBitmaps[i] = new RoaringBitmap();
      }
      dataTableBuilder.startRow();
      for (int i = 0; i < numColumns; i++) {
        Object result = _results.get(i);
        if (!returnFinalResult) {
          if (result == null && columnDataTypes[i] != ColumnDataType.OBJECT) {
            result = NullValueUtils.getDefaultNullValue(columnDataTypes[i].toDataType());
            nullBitmaps[i].add(0);
          }
          setIntermediateResult(dataTableBuilder, columnDataTypes, i, result);
        } else {
          result = _aggregationFunctions[i].extractFinalResult(result);
          if (result == null) {
            result = NullValueUtils.getDefaultNullValue(columnDataTypes[i].toDataType());
            nullBitmaps[i].add(0);
          }
          setFinalResult(dataTableBuilder, columnDataTypes, i, result);
        }
      }
      dataTableBuilder.finishRow();
      for (RoaringBitmap nullBitmap : nullBitmaps) {
        dataTableBuilder.setNullRowIds(nullBitmap);
      }
    } else {
      dataTableBuilder.startRow();
      for (int i = 0; i < numColumns; i++) {
        Object result = _results.get(i);
        if (!returnFinalResult) {
          setIntermediateResult(dataTableBuilder, columnDataTypes, i, result);
        } else {
          result = _aggregationFunctions[i].extractFinalResult(result);
          setFinalResult(dataTableBuilder, columnDataTypes, i, result);
        }
      }
      dataTableBuilder.finishRow();
    }

    DataTable dataTable = dataTableBuilder.build();
    attachMetadataToDataTable(dataTable);
    return dataTable;
  }

  private void setIntermediateResult(DataTableBuilder dataTableBuilder, ColumnDataType[] columnDataTypes, int index,
      Object result)
      throws IOException {
    ColumnDataType columnDataType = columnDataTypes[index];
    switch (columnDataType) {
      case LONG:
        dataTableBuilder.setColumn(index, (long) result);
        break;
      case DOUBLE:
        dataTableBuilder.setColumn(index, (double) result);
        break;
      case OBJECT:
        dataTableBuilder.setColumn(index, result);
        break;
      default:
        throw new IllegalStateException("Illegal column data type in intermediate result: " + columnDataType);
    }
  }

  private void setFinalResult(DataTableBuilder dataTableBuilder, ColumnDataType[] columnDataTypes, int index,
      Object result)
      throws IOException {
    ColumnDataType columnDataType = columnDataTypes[index];
    switch (columnDataType) {
      case INT:
        dataTableBuilder.setColumn(index, (int) result);
        break;
      case LONG:
        dataTableBuilder.setColumn(index, (long) result);
        break;
      case FLOAT:
        dataTableBuilder.setColumn(index, (float) result);
        break;
      case DOUBLE:
        dataTableBuilder.setColumn(index, (double) result);
        break;
      case BIG_DECIMAL:
        dataTableBuilder.setColumn(index, (BigDecimal) result);
        break;
      case STRING:
        dataTableBuilder.setColumn(index, result.toString());
        break;
      case BYTES:
        dataTableBuilder.setColumn(index, (ByteArray) result);
        break;
      case DOUBLE_ARRAY:
        dataTableBuilder.setColumn(index, ((DoubleArrayList) result).elements());
        break;
      default:
        throw new IllegalStateException("Illegal column data type in final result: " + columnDataType);
    }
  }
}
