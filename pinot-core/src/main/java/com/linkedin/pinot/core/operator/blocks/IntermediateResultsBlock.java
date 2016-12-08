/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
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

import com.linkedin.pinot.common.data.FieldSpec.DataType;
import com.linkedin.pinot.common.exception.QueryException;
import com.linkedin.pinot.common.response.ProcessingException;
import com.linkedin.pinot.common.utils.DataTable;
import com.linkedin.pinot.common.utils.DataTableBuilder;
import com.linkedin.pinot.common.utils.DataTableBuilder.DataSchema;
import com.linkedin.pinot.core.common.Block;
import com.linkedin.pinot.core.common.BlockDocIdSet;
import com.linkedin.pinot.core.common.BlockDocIdValueSet;
import com.linkedin.pinot.core.common.BlockId;
import com.linkedin.pinot.core.common.BlockMetadata;
import com.linkedin.pinot.core.common.BlockValSet;
import com.linkedin.pinot.core.common.Predicate;
import com.linkedin.pinot.core.operator.aggregation.groupby.AggregationGroupByResult;
import com.linkedin.pinot.core.query.aggregation.AggregationFunction;
import com.linkedin.pinot.core.query.aggregation.AggregationFunctionUtils;
import com.linkedin.pinot.core.query.selection.SelectionOperatorUtils;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;


/**
 * A holder of InstanceResponse components. Easy to do merge.
 *
 *
 */
public class IntermediateResultsBlock implements Block {
  private List<AggregationFunction> _aggregationFunctionList;
  private List<Serializable> _aggregationResultList;
  private List<ProcessingException> _processingExceptions;
  private long _numDocsScanned;
  private long _numEntriesScannedInFilter;
  private long _numEntriesScannedPostFilter;
  private long _totalRawDocs;
  private List<Map<String, Serializable>> _aggregationGroupByOperatorResult;
  private AggregationGroupByResult _aggregationGroupByResult;
  private DataSchema _dataSchema;
  private Collection<Serializable[]> _selectionResult;

  public IntermediateResultsBlock(List<AggregationFunction> aggregationFunctionList,
      List<Serializable> aggregationResult) {
    _aggregationFunctionList = aggregationFunctionList;
    _aggregationResultList = aggregationResult;
  }

  /**
   * Constructor for the class when group-by results are provided in a list of Maps containing
   * group-by keys and aggregation values.
   *
   * @param aggregationFunctionList List of aggregation functions in the query
   * @param aggregationGroupByResults Result of aggregation group-by.
   * @param isGroupByResults
   */
  public IntermediateResultsBlock(List<AggregationFunction> aggregationFunctionList,
      List<Map<String, Serializable>> aggregationGroupByResults, boolean isGroupByResults) {
    _aggregationFunctionList = aggregationFunctionList;
    _aggregationGroupByOperatorResult = aggregationGroupByResults;
    _aggregationGroupByResult = null;
  }

  /**
   * Constructor of the class when group-by results are provided in {@link AggregationGroupByResult}
   *
   * @param aggregationFunctions List of aggregation functions in the query
   * @param aggregationGroupByResults Result of aggregation group-by.
   */
  public IntermediateResultsBlock(List<AggregationFunction> aggregationFunctions,
      AggregationGroupByResult aggregationGroupByResults) {
    _aggregationFunctionList = aggregationFunctions;
    _aggregationGroupByResult = aggregationGroupByResults;
    _aggregationGroupByOperatorResult = null;
  }

  public IntermediateResultsBlock(Exception e) {
    this(QueryException.QUERY_EXECUTION_ERROR, e);
  }

  public IntermediateResultsBlock(ProcessingException processingException, Exception e) {
    _processingExceptions = new ArrayList<>();
    _processingExceptions.add(QueryException.getException(processingException, e));
  }

  public IntermediateResultsBlock() {
    // TODO Auto-generated constructor stub
  }

  @Override
  public boolean applyPredicate(Predicate predicate) {
    throw new UnsupportedOperationException();
  }

  @Override
  public BlockId getId() {
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
  public BlockDocIdSet getBlockDocIdSet() {
    throw new UnsupportedOperationException();
  }

  @Override
  public BlockMetadata getMetadata() {
    throw new UnsupportedOperationException();
  }

  public List<Serializable> getAggregationResult() {
    return _aggregationResultList;
  }

  public DataTable getDataTable() throws Exception {
    if (_aggregationResultList != null) {
      return getAggregationResultDataTable();
    }

    if (_aggregationGroupByOperatorResult != null) {
      return getAggregationGroupByResultDataTable();
    }
    if (_selectionResult != null) {
      return getSelectionResultDataTable();
    }
    if (_processingExceptions != null && _processingExceptions.size() > 0) {
      return getExceptionsDataTable();
    }
    throw new UnsupportedOperationException("Cannot get DataTable from IntermediateResultBlock!");
  }

  public DataTable attachMetadataToDataTable(DataTable dataTable) {
    dataTable.getMetadata().put(DataTable.NUM_DOCS_SCANNED_METADATA_KEY, String.valueOf(_numDocsScanned));
    dataTable.getMetadata()
        .put(DataTable.NUM_ENTRIES_SCANNED_IN_FILTER_METADATA_KEY, String.valueOf(_numEntriesScannedInFilter));
    dataTable.getMetadata()
        .put(DataTable.NUM_ENTRIES_SCANNED_POST_FILTER_METADATA_KEY, String.valueOf(_numEntriesScannedPostFilter));
    dataTable.getMetadata().put(DataTable.TOTAL_DOCS_METADATA_KEY, String.valueOf(_totalRawDocs));
    if (_processingExceptions != null && _processingExceptions.size() > 0) {
      for (ProcessingException exception : _processingExceptions) {
        dataTable.addException(exception);
      }
    }
    return dataTable;
  }

  public DataTable getExceptionsDataTable() {
    return attachMetadataToDataTable(new DataTable());
  }

  private DataTable getSelectionResultDataTable() throws Exception {
    return attachMetadataToDataTable(SelectionOperatorUtils.getDataTableFromRowSet(_selectionResult, _dataSchema));
  }

  public DataTable getAggregationResultDataTable() throws Exception {
    DataSchema schema = AggregationFunctionUtils.getAggregationResultsDataSchema(_aggregationFunctionList);
    DataTableBuilder builder = new DataTableBuilder(schema);
    builder.open();
    builder.startRow();
    for (int i = 0; i < _aggregationResultList.size(); ++i) {
      switch (_aggregationFunctionList.get(i).aggregateResultDataType()) {
        case LONG:
          builder.setColumn(i, ((Number) _aggregationResultList.get(i)).longValue());
          break;
        case DOUBLE:
          builder.setColumn(i, ((Double) _aggregationResultList.get(i)).doubleValue());
          break;
        case OBJECT:
          builder.setColumn(i, _aggregationResultList.get(i));
          break;
        default:
          throw new UnsupportedOperationException("Shouldn't reach here in getAggregationResultsList()");
      }
    }
    builder.finishRow();
    builder.seal();
    return attachMetadataToDataTable(builder.build());
  }

  public void setAggregationResults(List<Serializable> aggregationResults) {
    _aggregationResultList = aggregationResults;
  }

  public List<Map<String, Serializable>> getAggregationGroupByOperatorResult() {
    return _aggregationGroupByOperatorResult;
  }

  public DataTable getAggregationGroupByResultDataTable() throws Exception {

    String[] columnNames = new String[] { "functionName", "GroupByResultMap" };
    DataType[] columnTypes = new DataType[] { DataType.STRING, DataType.OBJECT };
    DataSchema dataSchema = new DataSchema(columnNames, columnTypes);

    DataTableBuilder dataTableBuilder = new DataTableBuilder(dataSchema);
    dataTableBuilder.open();
    for (int i = 0; i < _aggregationGroupByOperatorResult.size(); ++i) {
      dataTableBuilder.startRow();
      dataTableBuilder.setColumn(0, _aggregationFunctionList.get(i).getFunctionName());
      dataTableBuilder.setColumn(1, _aggregationGroupByOperatorResult.get(i));
      dataTableBuilder.finishRow();
    }
    dataTableBuilder.seal();
    return attachMetadataToDataTable(dataTableBuilder.build());
  }

  public List<ProcessingException> getExceptions() {
    return _processingExceptions;
  }

  public long getNumDocsScanned() {
    return _numDocsScanned;
  }

  public long getNumEntriesScannedInFilter() {
    return _numEntriesScannedInFilter;
  }

  public long getNumEntriesScannedPostFilter() {
    return _numEntriesScannedPostFilter;
  }

  public long getTotalRawDocs() {
    return _totalRawDocs;
  }

  public void setExceptionsList(List<ProcessingException> processingExceptions) {
    _processingExceptions = processingExceptions;
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

  public void setTotalRawDocs(long totalRawDocs) {
    _totalRawDocs = totalRawDocs;
  }

  public void setAggregationFunctions(List<AggregationFunction> aggregationFunctions) {
    _aggregationFunctionList = aggregationFunctions;
  }

  public void setSelectionDataSchema(DataSchema dataSchema) {
    _dataSchema = dataSchema;

  }

  public void setSelectionResult(Collection<Serializable[]> rowEventsSet) {
    _selectionResult = rowEventsSet;
  }

  public DataSchema getSelectionDataSchema() {
    return _dataSchema;
  }

  public Collection<Serializable[]> getSelectionResult() {
    return _selectionResult;
  }

  public void setAggregationGroupByResult(List<Map<String, Serializable>> combineAggregationGroupByResults1) {
    _aggregationGroupByOperatorResult = combineAggregationGroupByResults1;

  }

  public AggregationGroupByResult getAggregationGroupByResult() {
    return _aggregationGroupByResult;
  }
}
