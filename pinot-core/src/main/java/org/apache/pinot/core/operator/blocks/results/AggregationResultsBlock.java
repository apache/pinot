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
import it.unimi.dsi.fastutil.floats.FloatArrayList;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.Collections;
import java.util.List;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pinot.common.datatable.DataTable;
import org.apache.pinot.common.request.context.FilterContext;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.core.common.datatable.DataTableBuilder;
import org.apache.pinot.core.common.datatable.DataTableBuilderFactory;
import org.apache.pinot.core.query.aggregation.function.AggregationFunction;
import org.apache.pinot.core.query.aggregation.function.AggregationFunctionUtils;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.spi.utils.ByteArray;
import org.roaringbitmap.RoaringBitmap;


/**
 * Results block for aggregation queries.
 */
@SuppressWarnings({"rawtypes", "unchecked"})
public class AggregationResultsBlock extends BaseResultsBlock {
  private final AggregationFunction[] _aggregationFunctions;
  private final List<Object> _results;
  private final QueryContext _queryContext;

  public AggregationResultsBlock(AggregationFunction[] aggregationFunctions, List<Object> results,
      QueryContext queryContext) {
    _aggregationFunctions = aggregationFunctions;
    _results = results;
    _queryContext = queryContext;
  }

  public AggregationFunction[] getAggregationFunctions() {
    return _aggregationFunctions;
  }

  public List<Object> getResults() {
    return _results;
  }

  @Override
  public int getNumRows() {
    return 1;
  }

  @Override
  public QueryContext getQueryContext() {
    return _queryContext;
  }

  @Override
  public DataSchema getDataSchema() {
    List<Pair<AggregationFunction, FilterContext>> filteredAggregationFunctions =
        _queryContext.getFilteredAggregationFunctions();
    assert filteredAggregationFunctions != null;
    int numColumns = filteredAggregationFunctions.size();
    String[] columnNames = new String[numColumns];
    ColumnDataType[] columnDataTypes = new ColumnDataType[numColumns];
    boolean returnFinalResult = _queryContext.isServerReturnFinalResult();
    for (int i = 0; i < numColumns; i++) {
      Pair<AggregationFunction, FilterContext> pair = filteredAggregationFunctions.get(i);
      AggregationFunction aggregationFunction = pair.getLeft();
      columnNames[i] = AggregationFunctionUtils.getResultColumnName(aggregationFunction, pair.getRight());
      columnDataTypes[i] = returnFinalResult ? aggregationFunction.getFinalResultColumnType()
          : aggregationFunction.getIntermediateResultColumnType();
    }
    return new DataSchema(columnNames, columnDataTypes);
  }

  @Override
  public List<Object[]> getRows() {
    return Collections.singletonList(_results.toArray());
  }

  @Override
  public DataTable getDataTable()
      throws IOException {
    DataSchema dataSchema = getDataSchema();
    assert dataSchema != null;
    ColumnDataType[] columnDataTypes = dataSchema.getColumnDataTypes();
    int numColumns = columnDataTypes.length;
    DataTableBuilder dataTableBuilder = DataTableBuilderFactory.getDataTableBuilder(dataSchema);
    boolean returnFinalResult = _queryContext.isServerReturnFinalResult();
    if (_queryContext.isNullHandlingEnabled()) {
      RoaringBitmap[] nullBitmaps = new RoaringBitmap[numColumns];
      for (int i = 0; i < numColumns; i++) {
        nullBitmaps[i] = new RoaringBitmap();
      }
      dataTableBuilder.startRow();
      for (int i = 0; i < numColumns; i++) {
        Object result = _results.get(i);
        if (result == null) {
          result = columnDataTypes[i].getNullPlaceholder();
          nullBitmaps[i].add(0);
        }
        if (!returnFinalResult) {
          setIntermediateResult(dataTableBuilder, columnDataTypes, i, result);
        } else {
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
    return dataTableBuilder.build();
  }

  private void setIntermediateResult(DataTableBuilder dataTableBuilder, ColumnDataType[] columnDataTypes, int index,
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
    switch (columnDataType.getStoredType()) {
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
      case INT_ARRAY:
        dataTableBuilder.setColumn(index, ((IntArrayList) result).elements());
        break;
      case LONG_ARRAY:
        dataTableBuilder.setColumn(index, ((LongArrayList) result).elements());
        break;
      case FLOAT_ARRAY:
        dataTableBuilder.setColumn(index, ((FloatArrayList) result).elements());
        break;
      case DOUBLE_ARRAY:
        dataTableBuilder.setColumn(index, ((DoubleArrayList) result).elements());
        break;
      case STRING_ARRAY:
        dataTableBuilder.setColumn(index, ((ObjectArrayList<String>) result).toArray(new String[0]));
        break;
      default:
        throw new IllegalStateException("Illegal column data type in final result: " + columnDataType);
    }
  }
}
