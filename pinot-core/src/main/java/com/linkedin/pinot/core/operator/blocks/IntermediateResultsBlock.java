/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.core.operator.blocks;

import com.linkedin.pinot.common.exception.QueryException;
import com.linkedin.pinot.common.response.ProcessingException;
import com.linkedin.pinot.common.utils.DataSchema;
import com.linkedin.pinot.common.utils.DataTable;
import com.linkedin.pinot.core.common.Block;
import com.linkedin.pinot.core.common.BlockDocIdSet;
import com.linkedin.pinot.core.common.BlockDocIdValueSet;
import com.linkedin.pinot.core.common.BlockMetadata;
import com.linkedin.pinot.core.common.BlockValSet;
import com.linkedin.pinot.core.common.datatable.DataTableBuilder;
import com.linkedin.pinot.core.common.datatable.DataTableImplV2;
import com.linkedin.pinot.core.query.aggregation.AggregationFunctionContext;
import com.linkedin.pinot.core.query.aggregation.groupby.AggregationGroupByResult;
import com.linkedin.pinot.core.query.selection.SelectionOperatorUtils;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;


/**
 * The <code>IntermediateResultsBlock</code> class is the holder of the server side inter-segment results.
 */
public class IntermediateResultsBlock implements Block {
  private DataSchema _selectionDataSchema;
  private Collection<Serializable[]> _selectionResult;
  private AggregationFunctionContext[] _aggregationFunctionContexts;
  private List<Object> _aggregationResult;
  private AggregationGroupByResult _aggregationGroupByResult;
  private List<Map<String, Object>> _combinedAggregationGroupByResult;
  private List<ProcessingException> _processingExceptions;
  private long _numDocsScanned;
  private long _numEntriesScannedInFilter;
  private long _numEntriesScannedPostFilter;
  private long _numTotalRawDocs;
  private boolean _numGroupsLimitReached;

  /**
   * Constructor for selection result.
   */
  public IntermediateResultsBlock(@Nonnull DataSchema selectionDataSchema,
      @Nonnull Collection<Serializable[]> selectionResult) {
    _selectionDataSchema = selectionDataSchema;
    _selectionResult = selectionResult;
  }

  /**
   * Constructor for aggregation result.
   * <p>For aggregation only, the result is a list of values.
   * <p>For aggregation group-by, the result is a list of maps from group keys to aggregation values.
   */
  @SuppressWarnings("unchecked")
  public IntermediateResultsBlock(@Nonnull AggregationFunctionContext[] aggregationFunctionContexts,
      @Nonnull List aggregationResult, boolean isGroupBy) {
    _aggregationFunctionContexts = aggregationFunctionContexts;
    if (isGroupBy) {
      _combinedAggregationGroupByResult = aggregationResult;
    } else {
      _aggregationResult = aggregationResult;
    }
  }

  /**
   * Constructor for aggregation group-by result with {@link AggregationGroupByResult}.
   */
  public IntermediateResultsBlock(@Nonnull AggregationFunctionContext[] aggregationFunctionContexts,
      @Nullable AggregationGroupByResult aggregationGroupByResults) {
    _aggregationFunctionContexts = aggregationFunctionContexts;
    _aggregationGroupByResult = aggregationGroupByResults;
  }

  /**
   * Constructor for exception block.
   */
  public IntermediateResultsBlock(@Nonnull ProcessingException processingException, @Nonnull Exception e) {
    _processingExceptions = new ArrayList<>();
    _processingExceptions.add(QueryException.getException(processingException, e));
  }

  /**
   * Constructor for exception block.
   */
  public IntermediateResultsBlock(@Nonnull Exception e) {
    this(QueryException.QUERY_EXECUTION_ERROR, e);
  }

  @Nullable
  public DataSchema getSelectionDataSchema() {
    return _selectionDataSchema;
  }

  public void setSelectionDataSchema(@Nullable DataSchema dataSchema) {
    _selectionDataSchema = dataSchema;
  }

  @Nullable
  public Collection<Serializable[]> getSelectionResult() {
    return _selectionResult;
  }

  public void setSelectionResult(@Nullable Collection<Serializable[]> rowEventsSet) {
    _selectionResult = rowEventsSet;
  }

  @Nullable
  public AggregationFunctionContext[] getAggregationFunctionContexts() {
    return _aggregationFunctionContexts;
  }

  public void setAggregationFunctionContexts(AggregationFunctionContext[] aggregationFunctionContexts) {
    _aggregationFunctionContexts = aggregationFunctionContexts;
  }

  @Nullable
  public List<Object> getAggregationResult() {
    return _aggregationResult;
  }

  public void setAggregationResults(@Nullable List<Object> aggregationResults) {
    _aggregationResult = aggregationResults;
  }

  @Nullable
  public AggregationGroupByResult getAggregationGroupByResult() {
    return _aggregationGroupByResult;
  }

  @Nullable
  public List<ProcessingException> getProcessingExceptions() {
    return _processingExceptions;
  }

  public void setProcessingExceptions(@Nullable List<ProcessingException> processingExceptions) {
    _processingExceptions = processingExceptions;
  }

  public void addToProcessingExceptions(@Nonnull ProcessingException processingException) {
    if (_processingExceptions == null) {
      _processingExceptions = new ArrayList<>();
    }
    _processingExceptions.add(processingException);
  }

  public void setNumDocsScanned(long numDocsScanned) {
    _numDocsScanned = numDocsScanned;
  }

  public void setNumEntriesScannedInFilter(long numEntriesScannedInFilter) {
    _numEntriesScannedInFilter = numEntriesScannedInFilter;
  }

  public void setNumEntriesScannedPostFilter(long numEntriesScannedPostFilter) {
    _numEntriesScannedPostFilter = numEntriesScannedPostFilter;
  }

  public void setNumTotalRawDocs(long numTotalRawDocs) {
    _numTotalRawDocs = numTotalRawDocs;
  }

  public void setNumGroupsLimitReached(boolean numGroupsLimitReached) {
    _numGroupsLimitReached = numGroupsLimitReached;
  }

  @Nonnull
  public DataTable getDataTable()
      throws Exception {
    if (_selectionResult != null) {
      return getSelectionResultDataTable();
    }

    if (_aggregationResult != null) {
      return getAggregationResultDataTable();
    }

    if (_combinedAggregationGroupByResult != null) {
      return getAggregationGroupByResultDataTable();
    }

    if (_processingExceptions != null && _processingExceptions.size() > 0) {
      return getProcessingExceptionsDataTable();
    }

    throw new UnsupportedOperationException("No data inside IntermediateResultsBlock.");
  }

  @Nonnull
  private DataTable getSelectionResultDataTable()
      throws Exception {
    return attachMetadataToDataTable(
        SelectionOperatorUtils.getDataTableFromRows(_selectionResult, _selectionDataSchema));
  }

  @Nonnull
  private DataTable getAggregationResultDataTable()
      throws Exception {
    // Extract each aggregation column name and type from aggregation function context.
    int numAggregationFunctions = _aggregationFunctionContexts.length;
    String[] columnNames = new String[numAggregationFunctions];
    DataSchema.ColumnDataType[] columnDataTypes = new DataSchema.ColumnDataType[numAggregationFunctions];
    for (int i = 0; i < numAggregationFunctions; i++) {
      AggregationFunctionContext aggregationFunctionContext = _aggregationFunctionContexts[i];
      columnNames[i] = aggregationFunctionContext.getAggregationColumnName();
      columnDataTypes[i] = aggregationFunctionContext.getAggregationFunction().getIntermediateResultColumnType();
    }

    // Build the data table.
    DataTableBuilder dataTableBuilder = new DataTableBuilder(new DataSchema(columnNames, columnDataTypes));
    dataTableBuilder.startRow();
    for (int i = 0; i < numAggregationFunctions; i++) {
      switch (columnDataTypes[i]) {
        case LONG:
          dataTableBuilder.setColumn(i, ((Number) _aggregationResult.get(i)).longValue());
          break;
        case DOUBLE:
          dataTableBuilder.setColumn(i, ((Double) _aggregationResult.get(i)).doubleValue());
          break;
        case OBJECT:
          dataTableBuilder.setColumn(i, _aggregationResult.get(i));
          break;
        default:
          throw new UnsupportedOperationException(
              "Unsupported aggregation column data type: " + columnDataTypes[i] + " for column: " + columnNames[i]);
      }
    }
    dataTableBuilder.finishRow();
    DataTable dataTable = dataTableBuilder.build();

    return attachMetadataToDataTable(dataTable);
  }

  @Nonnull
  private DataTable getAggregationGroupByResultDataTable() throws Exception {
    String[] columnNames = new String[]{"functionName", "GroupByResultMap"};
    DataSchema.ColumnDataType[] columnDataTypes =
        new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.STRING, DataSchema.ColumnDataType.OBJECT};

    // Build the data table.
    DataTableBuilder dataTableBuilder = new DataTableBuilder(new DataSchema(columnNames, columnDataTypes));
    int numAggregationFunctions = _aggregationFunctionContexts.length;
    for (int i = 0; i < numAggregationFunctions; i++) {
      dataTableBuilder.startRow();
      AggregationFunctionContext aggregationFunctionContext = _aggregationFunctionContexts[i];
      dataTableBuilder.setColumn(0, aggregationFunctionContext.getAggregationColumnName());
      dataTableBuilder.setColumn(1, _combinedAggregationGroupByResult.get(i));
      dataTableBuilder.finishRow();
    }
    DataTable dataTable = dataTableBuilder.build();

    return attachMetadataToDataTable(dataTable);
  }

  private DataTable getProcessingExceptionsDataTable() {
    return attachMetadataToDataTable(new DataTableImplV2());
  }

  private DataTable attachMetadataToDataTable(DataTable dataTable) {
    dataTable.getMetadata().put(DataTable.NUM_DOCS_SCANNED_METADATA_KEY, String.valueOf(_numDocsScanned));
    dataTable.getMetadata()
        .put(DataTable.NUM_ENTRIES_SCANNED_IN_FILTER_METADATA_KEY, String.valueOf(_numEntriesScannedInFilter));
    dataTable.getMetadata()
        .put(DataTable.NUM_ENTRIES_SCANNED_POST_FILTER_METADATA_KEY, String.valueOf(_numEntriesScannedPostFilter));
    dataTable.getMetadata().put(DataTable.TOTAL_DOCS_METADATA_KEY, String.valueOf(_numTotalRawDocs));
    if (_numGroupsLimitReached) {
      dataTable.getMetadata().put(DataTable.NUM_GROUPS_LIMIT_REACHED_KEY, "true");
    }
    if (_processingExceptions != null && _processingExceptions.size() > 0) {
      for (ProcessingException exception : _processingExceptions) {
        dataTable.addException(exception);
      }
    }
    return dataTable;
  }

  @Override
  public BlockDocIdSet getBlockDocIdSet() {
    throw new UnsupportedOperationException();
  }

  @Override
  public BlockValSet getBlockValueSet() {
    throw new UnsupportedOperationException();
  }

  @Override
  public BlockDocIdValueSet getBlockDocIdValueSet() {
    throw new UnsupportedOperationException();
  }

  @Override
  public BlockMetadata getMetadata() {
    throw new UnsupportedOperationException();
  }
}
