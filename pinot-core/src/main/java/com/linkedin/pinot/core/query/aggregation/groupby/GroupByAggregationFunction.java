package com.linkedin.pinot.core.query.aggregation.groupby;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import com.linkedin.pinot.common.data.FieldSpec.DataType;
import com.linkedin.pinot.common.request.AggregationInfo;
import com.linkedin.pinot.common.request.GroupBy;
import com.linkedin.pinot.core.indexsegment.IndexSegment;
import com.linkedin.pinot.core.indexsegment.columnar.readers.ColumnarReader;
import com.linkedin.pinot.core.query.aggregation.AggregationFunction;
import com.linkedin.pinot.core.query.aggregation.AggregationFunctionFactory;
import com.linkedin.pinot.core.query.aggregation.CombineLevel;
import com.linkedin.pinot.core.query.utils.IntArray;


class GroupedValue implements Serializable {

}

public class GroupByAggregationFunction implements
    AggregationFunction<HashMap<String, List<Serializable>>, HashMap<String, List<Serializable>>> {
  private GroupBy _groupByQuery;
  private List<String> _groupByColumns;
  private int _groupByTopN;
  private List<AggregationFunction> _aggregationFunctionList;
  private Map<String, AggregationFunction> _aggregationFunctionMap;

  public GroupByAggregationFunction() {
  }

  @Override
  public void init(AggregationInfo aggregationInfo) {
  }

  public void init(List<AggregationInfo> aggregationInfos, GroupBy groupByQuery) {
    this._aggregationFunctionList = AggregationFunctionFactory.getAggregationFunction(aggregationInfos);
    _groupByQuery = groupByQuery;
    _groupByColumns = groupByQuery.getColumns();
    _groupByTopN = (int) groupByQuery.getTopN();
  }

  @Override
  public HashMap<String, List<Serializable>> aggregate(IntArray docIds, int docIdCount, IndexSegment indexSegment) {
    HashMap<String, List<Serializable>> aggregateGroupedValue = new HashMap<String, List<Serializable>>();
    List<Serializable> groupedValues = null;
    for (int i = 0; i < docIdCount; ++i) {
      String groupedKey = getGroupedKey(docIds.get(i), indexSegment);
      if (aggregateGroupedValue.containsKey(groupedKey)) {
        groupedValues = aggregateGroupedValue.get(groupedKey);
        for (int j = 0; j < _aggregationFunctionList.size(); ++j) {
          AggregationFunction aggregationFunction = _aggregationFunctionList.get(j);
          String functionName = aggregationFunction.getFunctionName();
          aggregationFunction.aggregate(groupedValues.get(j), i, indexSegment);
        }
      } else {
        groupedValues = new ArrayList<Serializable>();
        for (int j = 0; j < _aggregationFunctionList.size(); ++j) {
          AggregationFunction aggregationFunction = _aggregationFunctionList.get(j);
          groupedValues.add(aggregationFunction.aggregate((String) null, i, indexSegment));
        }
      }
      aggregateGroupedValue.put(groupedKey, groupedValues);
    }
    return aggregateGroupedValue;
  }

  @Override
  public List<HashMap<String, List<Serializable>>> combine(
      List<HashMap<String, List<Serializable>>> aggregationResultList, CombineLevel combineLevel) {
    if (aggregationResultList == null || aggregationResultList.isEmpty()) {
      return null;
    }
    HashMap<String, List<Serializable>> combinedGroupByResults = aggregationResultList.get(0);
    for (int i = 0; i < aggregationResultList.size(); ++i) {
      HashMap<String, List<Serializable>> toBeCombinedGroupByResults = aggregationResultList.get(i);
      for (String groupedKey : toBeCombinedGroupByResults.keySet()) {
        if (combinedGroupByResults.containsKey(groupedKey)) {
          combinedGroupByResults.put(
              groupedKey,
              combineTwoGroupByValues(combinedGroupByResults.get(groupedKey),
                  toBeCombinedGroupByResults.get(groupedKey)));
        } else {
          combinedGroupByResults.put(groupedKey, toBeCombinedGroupByResults.get(groupedKey));
        }
      }
    }
    return Arrays.asList(combinedGroupByResults);
  }

  List<Serializable> combineTwoGroupByValues(List<Serializable> combinedResults, List<Serializable> toBeCombinedResults) {
    for (int j = 0; j < combinedResults.size(); ++j) {
      List<Serializable> currentResult = Arrays.asList(combinedResults.get(j));
      currentResult.add(toBeCombinedResults.get(j));
      combinedResults.set(j,
          (Serializable) _aggregationFunctionList.get(j).combine(currentResult, CombineLevel.PARTITION).get(0));
    }
    return combinedResults;
  }

  @Override
  public HashMap<String, List<Serializable>> reduce(List<HashMap<String, List<Serializable>>> combinedResultList) {
    if (combinedResultList == null || combinedResultList.isEmpty()) {
      return null;
    }
    HashMap<String, List<Serializable>> reducedGroupByResults = combinedResultList.get(0);
    for (int i = 0; i < combinedResultList.size(); ++i) {
      HashMap<String, List<Serializable>> toBeReducedGroupByResults = combinedResultList.get(i);
      for (String groupedKey : toBeReducedGroupByResults.keySet()) {
        if (reducedGroupByResults.containsKey(groupedKey)) {
          reducedGroupByResults
              .put(
                  groupedKey,
                  combineTwoGroupByValues(reducedGroupByResults.get(groupedKey),
                      toBeReducedGroupByResults.get(groupedKey)));
        } else {
          reducedGroupByResults.put(groupedKey, toBeReducedGroupByResults.get(groupedKey));
        }
      }
    }
    return reducedGroupByResults;
  }

  @Override
  public JSONObject render(HashMap<String, List<Serializable>> finalAggregationResult) {
    try {
      JSONArray resultArray = new JSONArray();
      for (int i = 0; i < _aggregationFunctionList.size(); ++i) {
        JSONArray groupByResultsArray = new JSONArray();
        for (String groupedKey : finalAggregationResult.keySet()) {
          JSONObject groupByResultObject = new JSONObject();
          groupByResultObject.put("group", new JSONArray(groupedKey.split("\t")));
          groupByResultObject.put("value", finalAggregationResult.get(groupedKey).get(i));
          groupByResultsArray.put(groupByResultObject);
        }

        for (String groupedKey : finalAggregationResult.keySet()) {
          JSONObject groupByResultObject = new JSONObject();
          groupByResultObject.put("group", new JSONArray(groupedKey.split("\t")));
          groupByResultObject.put("value", finalAggregationResult.get(groupedKey).get(i));
          groupByResultsArray.put(groupByResultObject);
        }
        JSONObject result = new JSONObject();
        result.put("function", _aggregationFunctionList.get(i).getFunctionName());
        result.put("groupByResult", groupByResultsArray);
        resultArray.put(result);
      }
      return new JSONObject().put("groupByAggregationResults", resultArray).put("groupByColumns",
          new JSONArray(_groupByColumns));
    } catch (JSONException e) {
      throw new RuntimeException(e);
    }
  }

  private String getGroupedKey(int docId, IndexSegment indexSegment) {
    String groupedKey = indexSegment.getColumnarReader(_groupByColumns.get(0)).getRawValue(docId).toString();
    for (int i = 1; i < _groupByColumns.size(); ++i) {
      ColumnarReader reader = indexSegment.getColumnarReader(_groupByColumns.get(i));
      groupedKey += '\t' + reader.getStringValue(docId);
    }
    return groupedKey;
  }

  @Override
  public String getFunctionName() {
    return "GroupBY";
  }

  @Override
  public DataType aggregateResultDataType() {
    return DataType.OBJECT;
  }

  @Override
  public HashMap<String, List<Serializable>> aggregate(HashMap<String, List<Serializable>> currentResult, int docId,
      IndexSegment indexSegment) {
    throw new UnsupportedOperationException("GroupBy query doesn't need Aggregate on one value");
  }

  @Override
  public HashMap<String, List<Serializable>> combineTwoValues(HashMap<String, List<Serializable>> aggregationResult0,
      HashMap<String, List<Serializable>> aggregationResult1) {
    // TODO Auto-generated method stub
    return null;
  }

}
