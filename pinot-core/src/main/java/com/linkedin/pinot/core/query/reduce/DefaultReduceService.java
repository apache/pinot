package com.linkedin.pinot.core.query.reduce;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.json.JSONException;
import org.json.JSONObject;

import com.linkedin.pinot.common.query.ReduceService;
import com.linkedin.pinot.common.request.BrokerRequest;
import com.linkedin.pinot.common.response.AggregationResult;
import com.linkedin.pinot.common.response.BrokerResponse;
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


/**
 * DefaultReduceService will reduce DataTables gathered from multiple instances
 * to BrokerResponse.
 * 
 * @author xiafu
 *
 */
public class DefaultReduceService implements ReduceService {

  private static String NUM_DOCS_SCANNED = "numDocsScanned";
  private static String TIME_USED_MS = "timeUsedMs";
  private static String TOTAL_DOCS = "totalDocs";

  @Override
  public BrokerResponse reduce(BrokerRequest brokerRequest, Map<ServerInstance, InstanceResponse> instanceResponseMap) {
    BrokerResponse brokerResponse = new BrokerResponse();

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
      if (brokerRequest.isEnableTrace()) {
        reduceOnSegmentStatistics(brokerResponse.getSegmentStatistics(), serverInstance,
            instanceResponse.getSegmentStatistics());
        reduceOnTraceInfos(brokerResponse.getTraceInfo(), serverInstance, instanceResponse.getTraceInfo());
      }
      // reduceOnNumDocsScanned
      brokerResponse.setNumDocsScanned(brokerResponse.getNumDocsScanned() + instanceResponse.getNumDocsScanned());
      // reduceOnTotalDocs
      brokerResponse.setTotalDocs(brokerResponse.getTotalDocs() + instanceResponse.getTotalDocs());

    }
    // brokerResponse.setAggregationResults(reduceOnAggregationResults(brokerRequest, aggregationResultsList));
    return brokerResponse;
  }

  private void reduceOnTraceInfos(Map<String, String> brokerTraceInfo, ServerInstance serverInstance,
      Map<String, String> traceInfoToAdd) {
    for (String key : traceInfoToAdd.keySet()) {
      brokerTraceInfo.put(serverInstance.getHostname() + " : " + key, traceInfoToAdd.get(key));
    }
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
  public BrokerResponse reduceOnDataTable(BrokerRequest brokerRequest,
      Map<ServerInstance, DataTable> instanceResponseMap) {
    BrokerResponse brokerResponse = new BrokerResponse();
    if (instanceResponseMap == null || instanceResponseMap.size() == 0) {
      return BrokerResponse.EMPTY_RESULT;
    }
    for (ServerInstance serverInstance : instanceResponseMap.keySet()) {
      DataTable instanceResponse = instanceResponseMap.get(serverInstance);
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

    throw new UnsupportedOperationException(
        "Should not reach here, the query has no attributes of selection or aggregation!");
  }

  private JSONObject reduceOnSelectionResults(BrokerRequest brokerRequest,
      Map<ServerInstance, DataTable> instanceResponseMap) {
    try {
      if (instanceResponseMap.size() > 0) {
        DataTable dt = instanceResponseMap.values().iterator().next();
        SelectionOperatorService selectionService =
            new SelectionOperatorService(brokerRequest.getSelections(), dt.getDataSchema());
        return selectionService.render(selectionService.reduce(instanceResponseMap));
      } else {
        return null;
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private List<JSONObject> reduceOnAggregationGroupByOperatorResults(
      AggregationGroupByOperatorService aggregationGroupByOperatorService,
      Map<ServerInstance, DataTable> instanceResponseMap) {
    return aggregationGroupByOperatorService.renderGroupByOperators(aggregationGroupByOperatorService
        .reduceGroupByOperators(instanceResponseMap));
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
        throw new RuntimeException(e);
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
