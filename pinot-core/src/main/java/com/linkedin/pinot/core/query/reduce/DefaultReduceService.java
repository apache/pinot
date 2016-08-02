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
import com.linkedin.pinot.common.response.AggregationResult;
import com.linkedin.pinot.common.response.BrokerResponseJSON;
import com.linkedin.pinot.common.response.InstanceResponse;
import com.linkedin.pinot.common.response.ProcessingException;
import com.linkedin.pinot.common.response.ResponseStatistics;
import com.linkedin.pinot.common.response.ServerInstance;
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
import java.util.Map;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * DefaultReduceService will reduce DataTables gathered from multiple instances
 * to BrokerResponse.
 *
 *
 */
public class DefaultReduceService implements ReduceService<BrokerResponseJSON> {
  private static final Logger LOGGER = LoggerFactory.getLogger(DefaultReduceService.class);

  private static String NUM_DOCS_SCANNED = "numDocsScanned";
  private static String TIME_USED_MS = "timeUsedMs";
  private static String TOTAL_DOCS = "totalDocs";

  @Override
  public BrokerResponseJSON reduce(BrokerRequest brokerRequest, Map<ServerInstance, InstanceResponse> instanceResponseMap) {
    BrokerResponseJSON brokerResponse = new BrokerResponseJSON();

    List<List<AggregationResult>> aggregationResultsList = new ArrayList<List<AggregationResult>>();
    for (int i = 0; i < brokerRequest.getAggregationsInfoSize(); ++i) {
      aggregationResultsList.add(new ArrayList<AggregationResult>());
    }

    for (ServerInstance serverInstance : instanceResponseMap.keySet()) {
      InstanceResponse instanceResponse = instanceResponseMap.get(serverInstance);
      // Shuffle AggregationResults
      if (instanceResponse.getAggregationResults() != null) {
        for (int i = 0; i < brokerRequest.getAggregationsInfoSize(); ++i) {
          aggregationResultsList.get(i).add(instanceResponse.getAggregationResults().get(i));
        }
      }
      // reduceOnSelectionResults
      //      reduceOnSelectionResults(brokerResponse.getRowEvents(), serverInstance, instanceResponse.getRowEvents());

      // reduceOnExceptions
      reduceOnExceptions(brokerResponse.getExceptions(), serverInstance, instanceResponse.getExceptions());

      // debug mode enable : reduceOnTraceInfo
      /*if (brokerRequest.isEnableTrace()) {
        reduceOnSegmentStatistics(brokerResponse.getSegmentStatistics(), serverInstance,
            instanceResponse.getSegmentStatistics());
        reduceOnTraceInfos(brokerResponse.getTraceInfo(), serverInstance, instanceResponse.getTraceInfo());
      }*/
      // reduceOnNumDocsScanned
      brokerResponse.setNumDocsScanned(brokerResponse.getNumDocsScanned() + instanceResponse.getNumDocsScanned());
      // reduceOnTotalDocs
      brokerResponse.setTotalDocs(brokerResponse.getTotalDocs() + instanceResponse.getTotalDocs());

    }
    // brokerResponse.setAggregationResults(reduceOnAggregationResults(brokerRequest, aggregationResultsList));
    return brokerResponse;
  }

  private void reduceOnSegmentStatistics(List<ResponseStatistics> brokerSegmentStatistics,
      ServerInstance serverInstance, List<ResponseStatistics> segmentStatisticsToAdd) {
    brokerSegmentStatistics.addAll(segmentStatisticsToAdd);
  }

  private void reduceOnExceptions(List<ProcessingException> brokerExceptions, ServerInstance serverInstance,
      List<ProcessingException> exceptionsToAdd) {
    brokerExceptions.addAll(exceptionsToAdd);
  }

  @Override
  public BrokerResponseJSON reduceOnDataTable(BrokerRequest brokerRequest,
      Map<ServerInstance, DataTable> instanceResponseMap) {
    BrokerResponseJSON brokerResponse = new BrokerResponseJSON();
    if (instanceResponseMap == null || instanceResponseMap.size() == 0) {
      return BrokerResponseJSON.EMPTY_RESULT;
    }
    for (ServerInstance serverInstance : instanceResponseMap.keySet().toArray(new ServerInstance[instanceResponseMap.size()])) {
      DataTable instanceResponse = instanceResponseMap.get(serverInstance);
      if (instanceResponse == null) {
        continue;
      }

      // reduceOnTraceInfo (put it here so that trace info can show up even exception happens)
      if (brokerRequest.isEnableTrace() && instanceResponse.getMetadata() != null) {
        brokerResponse.getTraceInfo().put(serverInstance.getHostname(),
                instanceResponse.getMetadata().get("traceInfo"));
      }

      if (instanceResponse.getDataSchema() == null && instanceResponse.getMetadata() != null) {
        for (String key : instanceResponse.getMetadata().keySet()) {
          if (key.startsWith(DataTable.EXCEPTION_METADATA_KEY)) {
            ProcessingException processingException = new ProcessingException();
            processingException.setErrorCode(Integer.parseInt(key.substring(9)));
            processingException.setMessage(instanceResponse.getMetadata().get(key));
            brokerResponse.addToExceptions(processingException);
          }
        }
        instanceResponseMap.remove(serverInstance);
        continue;
      }

      // reduceOnNumDocsScanned
      brokerResponse.setNumDocsScanned(brokerResponse.getNumDocsScanned()
          + Long.parseLong(instanceResponse.getMetadata().get(NUM_DOCS_SCANNED)));
      // reduceOnTotalDocs
      brokerResponse.setTotalDocs(brokerResponse.getTotalDocs()
              + Long.parseLong(instanceResponse.getMetadata().get(TOTAL_DOCS)));
      if (Long.parseLong(instanceResponse.getMetadata().get(TIME_USED_MS)) > brokerResponse.getTimeUsedMs()) {
        brokerResponse.setTimeUsedMs(Long.parseLong(instanceResponse.getMetadata().get(TIME_USED_MS)));
      }
    }
    try {

      if (brokerRequest.isSetSelections() && (brokerRequest.getSelections().getSelectionColumns() != null)
          && (brokerRequest.getSelections().getSelectionColumns().size() >= 0)) {
        // Reduce DataTable for selection query.
        JSONObject selectionRet = reduceOnSelectionResults(brokerRequest, instanceResponseMap);
        brokerResponse.setSelectionResults(selectionRet);
        return brokerResponse;
      }
      if (brokerRequest.isSetAggregationsInfo()) {
        if (!brokerRequest.isSetGroupBy()) {
          List<List<Serializable>> aggregationResultsList =
              getShuffledAggregationResults(brokerRequest, instanceResponseMap);
          brokerResponse.setAggregationResults(reduceOnAggregationResults(brokerRequest, aggregationResultsList));
        } else {
          // Reduce DataTable for aggregation groupby query.
          //        GroupByAggregationService groupByAggregationService =
          //            new GroupByAggregationService(brokerRequest.getAggregationsInfo(), brokerRequest.getGroupBy());
          //        brokerResponse.setAggregationResults(reduceOnAggregationGroupByResults(groupByAggregationService,
          //            instanceResponseMap));

          AggregationGroupByOperatorService aggregationGroupByOperatorService =
              new AggregationGroupByOperatorService(brokerRequest.getAggregationsInfo(), brokerRequest.getGroupBy());
          brokerResponse.setAggregationResults(reduceOnAggregationGroupByOperatorResults(
              aggregationGroupByOperatorService, instanceResponseMap));

        }
        return brokerResponse;
      }

    } catch (Exception e) {
      brokerResponse.addToExceptions(QueryException.getException(QueryException.BROKER_GATHER_ERROR, e));
      return brokerResponse;
    }
    throw new UnsupportedOperationException(
        "Should not reach here, the query has no attributes of selection or aggregation!");
  }

  private JSONObject reduceOnSelectionResults(BrokerRequest brokerRequest,
      Map<ServerInstance, DataTable> instanceResponseMap) {
    try {
      if (instanceResponseMap.size() > 0) {
        DataTable dt = chooseFirstNonEmptySchema(instanceResponseMap.values());
        // best-effort to respond to the client instead of erroring out all requests
        // if there are errors
        removeConflictingResponses(dt.getDataSchema(), instanceResponseMap);
        if (brokerRequest.getSelections().isSetSelectionSortSequence()) {
          SelectionOperatorService selectionService =
              new SelectionOperatorService(brokerRequest.getSelections(), dt.getDataSchema());
          return selectionService.render(selectionService.reduce(instanceResponseMap));
        } else {
          Collection<Serializable[]> reduceResult = SelectionOperatorUtils.reduceWithoutOrdering(instanceResponseMap, brokerRequest.getSelections().getSize());
          return SelectionOperatorUtils.renderWithoutOrdering(reduceResult, brokerRequest.getSelections().getSelectionColumns(), dt.getDataSchema());
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

  private void removeConflictingResponses(DataSchema masterSchema, Map<ServerInstance, DataTable> instanceResponseMap) {
    Iterator<Map.Entry<ServerInstance, DataTable>> responseIter = instanceResponseMap.entrySet().iterator();
    StringBuilder droppedServersSb = new StringBuilder();
    int droppedCount = 0;
    while (responseIter.hasNext()) {
      Map.Entry<ServerInstance, DataTable> entry = responseIter.next();
      DataTable entryTable = entry.getValue();
      DataSchema entrySchema = entryTable.getDataSchema();
      if (! masterSchema.equals(entrySchema)) {
        if (entryTable.getNumberOfRows() > 0) {
          ++droppedCount;
          droppedServersSb.append(" " + entry.getKey());
        }
        responseIter.remove();
      }
    }
    // to log once per query
    if (droppedCount > 0) {
      LOGGER.error("SCHEMA-MISMATCH: Dropping responses from servers: {}", droppedServersSb.toString());
    }
  }

  private DataTable chooseFirstNonEmptySchema(Iterable<DataTable> dataTables) {
    for (DataTable dt : dataTables) {
      if (dt.getNumberOfRows() > 0) {
        return dt;
      }
    }
    Iterator<DataTable> it = dataTables.iterator();
    return  it.hasNext() ? it.next() : null;
  }

  private List<JSONObject> reduceOnAggregationGroupByOperatorResults(
      AggregationGroupByOperatorService aggregationGroupByOperatorService,
      Map<ServerInstance, DataTable> instanceResponseMap) {
    return aggregationGroupByOperatorService.renderGroupByOperators(
        aggregationGroupByOperatorService.reduceGroupByOperators(instanceResponseMap));
  }

  private List<JSONObject> reduceOnAggregationResults(BrokerRequest brokerRequest,
      List<List<Serializable>> aggregationResultsList) {
    List<JSONObject> retAggregationResults = new ArrayList<JSONObject>();
    List<AggregationFunction> aggregationFunctions = AggregationFunctionFactory.getAggregationFunction(brokerRequest);
    for (int i = 0; i < aggregationFunctions.size(); ++i) {
      Serializable retResult = aggregationFunctions.get(i).reduce(aggregationResultsList.get(i));
      try {
        retAggregationResults.add(aggregationFunctions.get(i).render(retResult)
            .put("function", aggregationFunctions.get(i).getFunctionName()));
      } catch (JSONException e) {
        LOGGER.error("Caught exception while reducing aggregation results", e);
        Utils.rethrowException(e);
        throw new AssertionError("Should not reach this");
      }
    }
    return retAggregationResults;
  }

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
