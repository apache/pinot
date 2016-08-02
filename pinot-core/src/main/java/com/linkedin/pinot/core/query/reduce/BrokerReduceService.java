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
package com.linkedin.pinot.core.query.reduce;

import com.linkedin.pinot.common.Utils;
import com.linkedin.pinot.common.exception.QueryException;
import com.linkedin.pinot.common.query.ReduceService;
import com.linkedin.pinot.common.request.BrokerRequest;
import com.linkedin.pinot.common.response.InstanceResponse;
import com.linkedin.pinot.common.response.ServerInstance;
import com.linkedin.pinot.common.response.broker.AggregationResult;
import com.linkedin.pinot.common.response.broker.BrokerResponseNative;
import com.linkedin.pinot.common.response.broker.QueryProcessingException;
import com.linkedin.pinot.common.response.broker.SelectionResults;
import com.linkedin.pinot.common.utils.DataTable;
import com.linkedin.pinot.common.utils.DataTableBuilder.DataSchema;
import com.linkedin.pinot.core.query.aggregation.AggregationFunction;
import com.linkedin.pinot.core.query.aggregation.AggregationFunctionFactory;
import com.linkedin.pinot.core.query.aggregation.groupby.AggregationGroupByOperatorService;
import com.linkedin.pinot.core.query.selection.SelectionOperatorService;
import com.linkedin.pinot.core.query.selection.SelectionOperatorUtils;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * BrokerReduceService will reduce DataTables gathered from multiple instances
 * to BrokerResponseNative.
 *
 */
public class BrokerReduceService implements ReduceService<BrokerResponseNative> {
  private static final Logger LOGGER = LoggerFactory.getLogger(BrokerReduceService.class);

  private static String NUM_DOCS_SCANNED = "numDocsScanned";
  private static String TIME_USED_MS = "timeUsedMs";
  private static String TOTAL_DOCS = "totalDocs";

  @Override
  public BrokerResponseNative reduce(BrokerRequest brokerRequest,
      Map<ServerInstance, InstanceResponse> instanceResponseMap) {
    // This methods will be removed from the interface, so not implemented currently.
    throw new RuntimeException("Method 'reduce' not implemented in class BrokerResponseNative");
  }

  @Override
  public BrokerResponseNative reduceOnDataTable(BrokerRequest brokerRequest,
      Map<ServerInstance, DataTable> instanceResponseMap) {
    BrokerResponseNative brokerResponseNative = new BrokerResponseNative();

    if (instanceResponseMap == null || instanceResponseMap.size() == 0) {
      return BrokerResponseNative.EMPTY_RESULT;
    }

    for (ServerInstance serverInstance : instanceResponseMap.keySet()
        .toArray(new ServerInstance[instanceResponseMap.size()])) {

      DataTable instanceResponse = instanceResponseMap.get(serverInstance);
      if (instanceResponse == null) {
        continue;
      }

      // reduceOnTraceInfo (put it here so that trace info can show up even exception happens)
      if (brokerRequest.isEnableTrace() && instanceResponse.getMetadata() != null) {
        brokerResponseNative.getTraceInfo()
            .put(serverInstance.getHostname(), instanceResponse.getMetadata().get("traceInfo"));
      }

      if (instanceResponse.getDataSchema() == null && instanceResponse.getMetadata() != null) {
        for (String key : instanceResponse.getMetadata().keySet()) {
          if (key.startsWith(DataTable.EXCEPTION_METADATA_KEY)) {
            QueryProcessingException processingException = new QueryProcessingException();
            processingException.setErrorCode(Integer.parseInt(key.substring(9)));
            processingException.setMessage(instanceResponse.getMetadata().get(key));
            brokerResponseNative.getProcessingExceptions().add(processingException);
          }
        }
        instanceResponseMap.remove(serverInstance);
        continue;
      }

      // Reduce on numDocsScanned
      brokerResponseNative.setNumDocsScanned(brokerResponseNative.getNumDocsScanned() + Long
          .parseLong(instanceResponse.getMetadata().get(NUM_DOCS_SCANNED)));

      // Reduce on totaDocs
      brokerResponseNative.setTotalDocs(
          brokerResponseNative.getTotalDocs() + Long.parseLong(instanceResponse.getMetadata().get(TOTAL_DOCS)));

      if (Long.parseLong(instanceResponse.getMetadata().get(TIME_USED_MS)) > brokerResponseNative.getTimeUsedMs()) {
        brokerResponseNative.setTimeUsedMs(Long.parseLong(instanceResponse.getMetadata().get(TIME_USED_MS)));
      }
    }

    try {
      if (brokerRequest.isSetSelections() && (brokerRequest.getSelections().getSelectionColumns() != null) && (
          brokerRequest.getSelections().getSelectionColumns().size() >= 0)) {

        // Reduce DataTable for selection query.
        SelectionResults selectionResults = reduceOnSelectionResults(brokerRequest, instanceResponseMap);
        brokerResponseNative.setSelectionResults(selectionResults);
        return brokerResponseNative;
      }

      if (brokerRequest.isSetAggregationsInfo()) {
        if (!brokerRequest.isSetGroupBy()) {
          List<List<Serializable>> aggregationResultsList =
              getShuffledAggregationResults(brokerRequest, instanceResponseMap);
          brokerResponseNative.setAggregationResults(reduceOnAggregationResults(brokerRequest, aggregationResultsList));
        } else {
          AggregationGroupByOperatorService aggregationGroupByOperatorService =
              new AggregationGroupByOperatorService(brokerRequest.getAggregationsInfo(), brokerRequest.getGroupBy());
          brokerResponseNative.setAggregationResults(
              reduceOnAggregationGroupByOperatorResults(aggregationGroupByOperatorService, instanceResponseMap));
        }
        return brokerResponseNative;
      }
    } catch (Exception e) {
      QueryProcessingException processingException = new QueryProcessingException();
      processingException.setMessage(e.getMessage());
      processingException.setErrorCode(QueryException.BROKER_GATHER_ERROR_CODE);
      brokerResponseNative.getProcessingExceptions().add(processingException);
      return brokerResponseNative;
    }

    throw new UnsupportedOperationException(
        "Should not reach here, the query has no attributes of selection or aggregation!");
  }

  /**
   * Reduce selection results from various servers into SelectionResults object, that goes
   * into BrokerResponseNative.
   *
   * @param brokerRequest
   * @param instanceResponseMap
   * @return
   */
  private SelectionResults reduceOnSelectionResults(BrokerRequest brokerRequest,
      Map<ServerInstance, DataTable> instanceResponseMap) {
    try {
      if (instanceResponseMap.size() > 0) {
        DataTable dt = chooseFirstNonEmptySchema(instanceResponseMap.values());
        removeConflictingResponses(dt.getDataSchema(), instanceResponseMap);

        if (brokerRequest.getSelections().isSetSelectionSortSequence()) {
          SelectionOperatorService selectionService =
              new SelectionOperatorService(brokerRequest.getSelections(), dt.getDataSchema());
          return selectionService.renderSelectionResults(selectionService.reduce(instanceResponseMap));
        } else {
          Collection<Serializable[]> reduceResult =
              SelectionOperatorUtils.reduceWithoutOrdering(instanceResponseMap, brokerRequest.getSelections().getSize());

          return SelectionOperatorUtils
              .renderSelectionResultsWithoutOrdering(reduceResult, brokerRequest.getSelections().getSelectionColumns(),
                  dt.getDataSchema());
        }
      } else {
        return null;
      }
    } catch (Exception e) {
      LOGGER.error("Caught exception while reducing results", e);
      Utils.rethrowException(e);
      throw new AssertionError("Should not reach this");
    }
  }

  /**
   * Remove rows for which the schema does not match the provided master schema.
   * @param masterSchema
   * @param instanceResponseMap
   */
  private void removeConflictingResponses(DataSchema masterSchema, Map<ServerInstance, DataTable> instanceResponseMap) {
    Iterator<Map.Entry<ServerInstance, DataTable>> responseIter = instanceResponseMap.entrySet().iterator();
    StringBuilder droppedServersSb = new StringBuilder();

    int droppedCount = 0;
    while (responseIter.hasNext()) {
      Map.Entry<ServerInstance, DataTable> entry = responseIter.next();
      DataTable entryTable = entry.getValue();
      DataSchema entrySchema = entryTable.getDataSchema();

      if (!masterSchema.equals(entrySchema)) {
        if (entryTable.getNumberOfRows() > 0) {
          ++droppedCount;
          droppedServersSb.append(" " + entry.getKey());
        }
        responseIter.remove();
      }
    }

    // To log once per query
    if (droppedCount > 0) {
      LOGGER.error("SCHEMA-MISMATCH: Dropping responses from servers: {}", droppedServersSb.toString());
    }
  }

  /**
   * Given a collection of data tables, pick the first one with non-empty rows.
   * @param dataTables
   * @return
   */
  private DataTable chooseFirstNonEmptySchema(Iterable<DataTable> dataTables) {
    for (DataTable dt : dataTables) {
      if (dt.getNumberOfRows() > 0) {
        return dt;
      }
    }
    Iterator<DataTable> it = dataTables.iterator();
    return it.hasNext() ? it.next() : null;
  }

  /**
   * Reduce the aggregationGroupBy response from various servers, and return a list of
   * AggregationResult objects, that is used to build the BrokerResponseNative object.
   *
   * @param aggregationGroupByOperatorService
   * @param instanceResponseMap
   * @return
   */
  private List<AggregationResult> reduceOnAggregationGroupByOperatorResults(
      AggregationGroupByOperatorService aggregationGroupByOperatorService,
      Map<ServerInstance, DataTable> instanceResponseMap) {

    List<Map<String, Serializable>> reducedGroupByResults =
        aggregationGroupByOperatorService.reduceGroupByOperators(instanceResponseMap);

    return aggregationGroupByOperatorService.renderAggregationGroupByResult(reducedGroupByResults);
  }

  /**
   * Reduce the aggregationGroupBy response from various servers, and return a list of
   * AggregationResult objects, that is used to build the BrokerResponseNative object.
   *
   * @param brokerRequest
   * @param aggregationResultsList
   * @return
   */
  private List<AggregationResult> reduceOnAggregationResults(BrokerRequest brokerRequest,
      List<List<Serializable>> aggregationResultsList) {
    List<AggregationResult> aggregationResults = new ArrayList<AggregationResult>();
    List<AggregationFunction> aggregationFunctions = AggregationFunctionFactory.getAggregationFunction(brokerRequest);

    for (int i = 0; i < aggregationFunctions.size(); ++i) {
      String function = aggregationFunctions.get(i).getFunctionName();
      Serializable value = formatValue(aggregationFunctions.get(i).reduce(aggregationResultsList.get(i)));

      AggregationResult aggregationResult = new AggregationResult(function, value);
      aggregationResults.add(aggregationResult);
    }

    return aggregationResults;
  }

  /**
   * Format the input float/double value to be of the form #####.#####.
   * If the input is not float or double, return the value as is.
   *
   * @param value
   * @return
   */
  private Serializable formatValue(Serializable value) {
    return (value instanceof Float || value instanceof Double) ? String.format(Locale.US, "%1.5f", value)
        : value.toString();
  }

  /**
   * @param brokerRequest
   * @param instanceResponseMap
   * @return
   */
  private List<List<Serializable>> getShuffledAggregationResults(BrokerRequest brokerRequest,
      Map<ServerInstance, DataTable> instanceResponseMap) {
    List<List<Serializable>> aggregationResultsList = new ArrayList<List<Serializable>>();

    for (int i = 0; i < brokerRequest.getAggregationsInfo().size(); ++i) {
      aggregationResultsList.add(new ArrayList<Serializable>());
    }

    DataSchema aggregationResultSchema;
    for (ServerInstance serverInstance : instanceResponseMap.keySet()) {
      DataTable instanceResponse = instanceResponseMap.get(serverInstance);
      aggregationResultSchema = instanceResponse.getDataSchema();

      if (aggregationResultSchema == null) {
        continue;
      }

      // Shuffle AggregationResults
      for (int rowId = 0; rowId < instanceResponse.getNumberOfRows(); ++rowId) {
        for (int colId = 0; colId < brokerRequest.getAggregationsInfoSize(); ++colId) {
          switch (aggregationResultSchema.getColumnType(colId)) {
            case INT:
              aggregationResultsList.get(colId).add(instanceResponse.getInt(rowId, colId));
              break;
            case SHORT:
              aggregationResultsList.get(colId).add(instanceResponse.getShort(rowId, colId));
              break;
            case FLOAT:
              aggregationResultsList.get(colId).add(instanceResponse.getFloat(rowId, colId));
              break;
            case LONG:
              aggregationResultsList.get(colId).add(instanceResponse.getLong(rowId, colId));
              break;
            case DOUBLE:
              aggregationResultsList.get(colId).add(instanceResponse.getDouble(rowId, colId));
              break;
            case STRING:
              aggregationResultsList.get(colId).add(instanceResponse.getString(rowId, colId));
              break;
            default:
              aggregationResultsList.get(colId).add(instanceResponse.getObject(rowId, colId));
              break;
          }
        }
      }
    }
    return aggregationResultsList;
  }
}
