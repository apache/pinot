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

import java.io.IOException;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pinot.common.datatable.DataTable;
import org.apache.pinot.common.datatable.DataTable.MetadataKey;
import org.apache.pinot.common.request.context.FilterContext;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.core.common.datatable.DataTableBuilder;
import org.apache.pinot.core.common.datatable.DataTableBuilderFactory;
import org.apache.pinot.core.common.datatable.DataTableBuilderUtils;
import org.apache.pinot.core.query.aggregation.function.AggregationFunction;
import org.apache.pinot.core.query.aggregation.function.AggregationFunctionUtils;
import org.apache.pinot.core.query.request.context.QueryContext;


/// Results block for aggregation queries.
@SuppressWarnings({"rawtypes", "unchecked"})
public class AggregationResultsBlock extends BaseResultsBlock {
  private final AggregationFunction[] _aggregationFunctions;
  private final List<Object> _results;
  private final QueryContext _queryContext;
  /// The final results, extracted once when the server returns final results. See [#getFinalResults()].
  @Nullable
  private Object[] _finalResults;

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
    return _queryContext.getLimit() == 0 ? 0 : 1;
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
    if (!_queryContext.isServerReturnFinalResult()) {
      return List.<Object[]>of(_results.toArray());
    }
    // When the server is requested to return the final result (e.g. a single-server colocated DIRECT aggregate in the
    // multi-stage engine), getDataSchema() reports the final column types. Finalize the intermediate results here so
    // that the rows are consistent with the schema; otherwise an intermediate object (e.g. a HyperLogLogPlus) would be
    // left in a column typed as its final type (e.g. LONG) and fail when the block is serialized. This mirrors the
    // finalization done in getDataTable() and in GroupByCombineOperator for the group-by case.
    return List.<Object[]>of(getFinalResults().clone());
  }

  /// Extracts the final results once, for [#getResultsMetadata()], [#getRows()] and [#getDataTable()]. Synchronized
  /// because two threads can ask at once: a streamed block reaches the multi-stage leaf, which reads its rows, while
  /// the server thread is still reading its metadata. Extraction can mutate the intermediate results in place, for
  /// example `PERCENTILE` sorts its values, so it must not run twice concurrently.
  private synchronized Object[] getFinalResults() {
    if (_finalResults == null) {
      int numColumns = _results.size();
      Object[] finalResults = new Object[numColumns];
      for (int i = 0; i < numColumns; i++) {
        finalResults[i] = _aggregationFunctions[i].extractFinalResult(_results.get(i));
      }
      _finalResults = finalResults;
    }
    return _finalResults;
  }

  @Override
  public DataTable getDataTable()
      throws IOException {
    DataSchema dataSchema = getDataSchema();
    assert dataSchema != null;
    ColumnDataType[] columnDataTypes = dataSchema.getColumnDataTypes();
    int numColumns = columnDataTypes.length;
    DataTableBuilder dataTableBuilder = DataTableBuilderFactory.getDataTableBuilder(dataSchema);

    // For LIMIT 0 queries
    if (_results.isEmpty()) {
      return dataTableBuilder.build();
    }

    // NOTE: Nulls are serialized through the builder's null-aware path regardless of the query's null-handling
    // option. Aggregation functions whose accumulator has no identity element (MINSTRING, MAXSTRING, ANYVALUE)
    // return a null intermediate result in both modes, and their result column type is not OBJECT.
    dataTableBuilder.startRow();
    if (_queryContext.isServerReturnFinalResult()) {
      Object[] finalResults = getFinalResults();
      for (int i = 0; i < numColumns; i++) {
        Object result = finalResults[i];
        if (result == null) {
          dataTableBuilder.setNull(i);
        } else {
          DataTableBuilderUtils.setColumn(dataTableBuilder, columnDataTypes[i].getStoredType(), i, result);
        }
      }
    } else {
      for (int i = 0; i < numColumns; i++) {
        Object result = _results.get(i);
        if (result == null) {
          dataTableBuilder.setNull(i);
        } else if (columnDataTypes[i] == ColumnDataType.OBJECT) {
          dataTableBuilder.setColumn(i, _aggregationFunctions[i].serializeIntermediateResult(result));
        } else {
          AggregationFunctionUtils.setIntermediateResult(dataTableBuilder, columnDataTypes[i], i, result);
        }
      }
    }
    dataTableBuilder.finishRow();
    return dataTableBuilder.build();
  }

  /// Extracts the final results before the data table is built when the server returns them, because the metadata
  /// is read first, and has to report a function that returned an approximate result.
  @Override
  public Map<String, String> getResultsMetadata() {
    Map<String, String> metadata = super.getResultsMetadata();
    if (_queryContext.isServerReturnFinalResult()) {
      getFinalResults();
      if (AggregationFunctionUtils.hasApproximatedResult(_aggregationFunctions)) {
        metadata.put(MetadataKey.APPROXIMATED_RESULT.getName(), "true");
      }
    }
    return metadata;
  }
}
