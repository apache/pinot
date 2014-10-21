package com.linkedin.pinot.core.query.aggregation.groupby;

import it.unimi.dsi.fastutil.PriorityQueue;
import it.unimi.dsi.fastutil.objects.ObjectArrayPriorityQueue;

import java.io.Serializable;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import com.linkedin.pinot.common.data.FieldSpec.DataType;
import com.linkedin.pinot.common.request.AggregationInfo;
import com.linkedin.pinot.common.request.GroupBy;
import com.linkedin.pinot.common.response.ServerInstance;
import com.linkedin.pinot.common.utils.DataTable;
import com.linkedin.pinot.core.query.aggregation.AggregationFunction;
import com.linkedin.pinot.core.query.aggregation.AggregationFunctionFactory;
import com.linkedin.pinot.core.query.aggregation.function.AvgAggregationFunction.AvgPair;
import com.linkedin.pinot.core.query.utils.Pair;


/**
 * GroupByAggregationService is initialized by aggregation functions and groupBys.
 *  
 * @author xiafu
 *
 */
public class AggregationGroupByOperatorService {
  private final List<String> _groupByColumns;
  private final int _groupByTopN;
  private final List<AggregationFunction> _aggregationFunctionList;

  public static final Map<DataType, DecimalFormat> DEFAULT_FORMAT_STRING_MAP = new HashMap<DataType, DecimalFormat>();
  static {
    DEFAULT_FORMAT_STRING_MAP.put(DataType.INT, new DecimalFormat("##########"));
    DEFAULT_FORMAT_STRING_MAP.put(DataType.LONG, new DecimalFormat("####################"));
    DEFAULT_FORMAT_STRING_MAP.put(DataType.FLOAT, new DecimalFormat("##########.#####"));
    DEFAULT_FORMAT_STRING_MAP.put(DataType.DOUBLE, new DecimalFormat("####################.##########"));
  }

  public AggregationGroupByOperatorService(List<AggregationInfo> aggregationInfos, GroupBy groupByQuery) {
    _aggregationFunctionList = AggregationFunctionFactory.getAggregationFunction(aggregationInfos);
    _groupByColumns = groupByQuery.getColumns();
    _groupByTopN = (int) groupByQuery.getTopN();
  }

  public static List<Map<String, Serializable>> transformDataTableToGroupByResult(DataTable dataTable) {
    List<Map<String, Serializable>> aggregationGroupByResults = new ArrayList<Map<String, Serializable>>();
    for (int i = 0; i < dataTable.getNumberOfRows(); i++) {
      String key = dataTable.getString(i, 0);
      Map<String, Serializable> hashMap = (Map<String, Serializable>) dataTable.getObject(i, 1);
      aggregationGroupByResults.add(hashMap);
    }
    return aggregationGroupByResults;
  }

  public List<AggregationFunction> getAggregationFunctionList() {
    return _aggregationFunctionList;
  }

  public List<Map<String, Serializable>> reduceGroupByOperators(Map<ServerInstance, DataTable> instanceResponseMap) {
    if ((instanceResponseMap == null) || instanceResponseMap.isEmpty()) {
      return null;
    }
    List<Map<String, Serializable>> reducedResult = null;
    for (DataTable toBeReducedGroupByResults : instanceResponseMap.values()) {
      if (reducedResult == null) {
        reducedResult = transformDataTableToGroupByResult(toBeReducedGroupByResults);
      } else {
        List<Map<String, Serializable>> toBeReducedResult =
            transformDataTableToGroupByResult(toBeReducedGroupByResults);
        for (int i = 0; i < reducedResult.size(); ++i) {
          for (String key : toBeReducedResult.get(i).keySet()) {
            if (reducedResult.get(i).containsKey(key)) {
              reducedResult.get(i).put(
                  key,
                  _aggregationFunctionList.get(i).combineTwoValues(reducedResult.get(i).get(key),
                      toBeReducedResult.get(i).get(key)));
            } else {
              reducedResult.get(i).put(key, toBeReducedResult.get(i).get(key));
            }
          }
        }
      }
    }
    return reducedResult;
  }

  public List<JSONObject> renderGroupByOperators(List<Map<String, Serializable>> finalAggregationResult) {
    try {

      List<JSONObject> retJsonResultList = new ArrayList<JSONObject>();
      for (int i = 0; i < _aggregationFunctionList.size(); ++i) {
        DecimalFormat df = DEFAULT_FORMAT_STRING_MAP.get(_aggregationFunctionList.get(i).aggregateResultDataType());
        JSONArray groupByResultsArray = new JSONArray();
        PriorityQueue priorityQueue = getPriorityQueue(_aggregationFunctionList.get(i));

        int groupSize = _groupByColumns.size();
        Map<String, Serializable> reducedGroupByResult = finalAggregationResult.get(i);

        for (String groupedKey : reducedGroupByResult.keySet()) {
          priorityQueue.enqueue(new Pair(reducedGroupByResult.get(groupedKey), groupedKey));
          if (priorityQueue.size() == (_groupByTopN + 1)) {
            priorityQueue.dequeue();
          }
        }

        int realGroupSize = _groupByTopN;
        if (priorityQueue.size() < _groupByTopN) {
          realGroupSize = priorityQueue.size();
        }
        for (int j = 0; j < realGroupSize; ++j) {
          JSONObject groupByResultObject = new JSONObject();
          Pair res = (Pair) priorityQueue.dequeue();
          groupByResultObject.put(
              "group",
              new JSONArray(((String) res.getSecond()).split(
                  GroupByConstants.GroupByDelimiter.groupByMultiDelimeter.toString(), groupSize)));
          //          if (res.getFirst() instanceof Number) {
          //            groupByResultObject.put("value", df.format(res.getFirst()));
          //          } else {
          //            groupByResultObject.put("value", res.getFirst());
          //          }
          //          groupByResultsArray.put(realGroupSize - 1 - j, groupByResultObject);
          groupByResultObject.put("value",
              _aggregationFunctionList.get(i).render((Serializable) res.getFirst()).get("value"));
          groupByResultsArray.put(realGroupSize - 1 - j, groupByResultObject);
        }

        JSONObject result = new JSONObject();
        result.put("function", _aggregationFunctionList.get(i).getFunctionName());
        result.put("groupByResult", groupByResultsArray);
        result.put("groupByColumns", new JSONArray(_groupByColumns));
        retJsonResultList.add(result);
      }
      return retJsonResultList;
    } catch (JSONException e) {
      throw new RuntimeException(e);
    }
  }

  public void trimToSize(List<Map<String, Serializable>> aggregationGroupByResultList) {
    if (aggregationGroupByResultList == null) {
      return;
    }

    for (int i = 0; i < aggregationGroupByResultList.size(); ++i) {
      if (aggregationGroupByResultList.get(i).size() > (_groupByTopN * 20)) {
        trimToSize(_aggregationFunctionList.get(i), aggregationGroupByResultList.get(i), _groupByTopN * 5);
      }
    }
  }

  private void trimToSize(AggregationFunction aggregationFunction, Map<String, Serializable> aggregationGroupByResult,
      int trimSize) {
    PriorityQueue priorityQueue = getPriorityQueue(aggregationFunction);
    for (String groupedKey : aggregationGroupByResult.keySet()) {
      priorityQueue.enqueue(new Pair(aggregationGroupByResult.get(groupedKey), groupedKey));
      if (priorityQueue.size() == (_groupByTopN + 1)) {
        priorityQueue.dequeue();
      }
    }

    for (int i = 0; i < (priorityQueue.size() - trimSize); ++i) {
      Pair res = (Pair) priorityQueue.dequeue();
      aggregationGroupByResult.remove(res.getSecond());
    }
  }

  private PriorityQueue getPriorityQueue(AggregationFunction aggregationFunction) {

    switch (aggregationFunction.aggregateResultDataType()) {
      case LONG:
        if (aggregationFunction.getFunctionName().startsWith("min_")) {
          return getLongGroupedValuePairPriorityQueueForMinFunction();
        } else {
          return getLongGroupedValuePairPriorityQueue();
        }

      case DOUBLE:
        if (aggregationFunction.getFunctionName().startsWith("min_")) {
          return getDoubleGroupedValuePairPriorityQueueForMinFunction();
        } else {
          return getDoubleGroupedValuePairPriorityQueue();
        }
      case OBJECT:
        if (aggregationFunction.getFunctionName().startsWith("avg_")) {
          return new customPriorityQueue<AvgPair>().getGroupedValuePairPriorityQueue();
        }
      default:
        throw new UnsupportedOperationException("AggregationFunction DataType is not supported in GroupBy Query");
    }
  }

  class customPriorityQueue<T extends Comparable> {
    private PriorityQueue getGroupedValuePairPriorityQueue() {
      return new ObjectArrayPriorityQueue<Pair<T, String>>(_groupByTopN + 1, new Comparator() {
        @Override
        public int compare(Object o1, Object o2) {
          if (((Pair<T, String>) o1).getFirst().compareTo(((Pair<T, String>) o2).getFirst()) < 0) {
            return -1;
          } else {
            if (((Pair<T, String>) o1).getFirst().compareTo(((Pair<T, String>) o2).getFirst()) > 0) {
              return 1;
            }
          }
          return 0;
        }
      });
    }
  }

  private PriorityQueue getLongGroupedValuePairPriorityQueue() {
    return new ObjectArrayPriorityQueue<Pair<Long, String>>(_groupByTopN + 1, new Comparator() {
      @Override
      public int compare(Object o1, Object o2) {
        if (((Pair<Long, String>) o1).getFirst() < ((Pair<Long, String>) o2).getFirst()) {
          return -1;
        } else {
          if (((Pair<Long, String>) o1).getFirst() > ((Pair<Long, String>) o2).getFirst()) {
            return 1;
          }
        }
        return 0;
      }
    });
  }

  private PriorityQueue getDoubleGroupedValuePairPriorityQueue() {
    return new ObjectArrayPriorityQueue<Pair<Double, String>>(_groupByTopN + 1, new Comparator() {
      @Override
      public int compare(Object o1, Object o2) {
        if (((Pair<Double, String>) o1).getFirst() < ((Pair<Double, String>) o2).getFirst()) {
          return -1;
        } else {
          if (((Pair<Double, String>) o1).getFirst() > ((Pair<Double, String>) o2).getFirst()) {
            return 1;
          }
        }
        return 0;
      }
    });
  }

  private PriorityQueue getLongGroupedValuePairPriorityQueueForMinFunction() {
    return new ObjectArrayPriorityQueue<Pair<Long, String>>(_groupByTopN + 1, new Comparator() {
      @Override
      public int compare(Object o1, Object o2) {
        if (((Pair<Long, String>) o1).getFirst() < ((Pair<Long, String>) o2).getFirst()) {
          return 1;
        } else {
          if (((Pair<Long, String>) o1).getFirst() > ((Pair<Long, String>) o2).getFirst()) {
            return -1;
          }
        }
        return 0;
      }
    });
  }

  private PriorityQueue getDoubleGroupedValuePairPriorityQueueForMinFunction() {
    return new ObjectArrayPriorityQueue<Pair<Double, String>>(_groupByTopN + 1, new Comparator() {
      @Override
      public int compare(Object o1, Object o2) {
        if (((Pair<Double, String>) o1).getFirst() < ((Pair<Double, String>) o2).getFirst()) {
          return 1;
        } else {
          if (((Pair<Double, String>) o1).getFirst() > ((Pair<Double, String>) o2).getFirst()) {
            return -1;
          }
        }
        return 0;
      }
    });
  }
}
