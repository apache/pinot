package com.linkedin.pinot.core.query.aggregation.groupby;

import it.unimi.dsi.fastutil.PriorityQueue;
import it.unimi.dsi.fastutil.objects.ObjectArrayPriorityQueue;

import java.io.Serializable;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Arrays;
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
import com.linkedin.pinot.common.utils.DataTableBuilder;
import com.linkedin.pinot.common.utils.DataTableBuilder.DataSchema;
import com.linkedin.pinot.core.indexsegment.ColumnarReader;
import com.linkedin.pinot.core.indexsegment.IndexSegment;
import com.linkedin.pinot.core.query.aggregation.AggregationFunction;
import com.linkedin.pinot.core.query.aggregation.AggregationFunctionFactory;
import com.linkedin.pinot.core.query.aggregation.CombineLevel;
import com.linkedin.pinot.core.query.utils.IntArray;
import com.linkedin.pinot.core.query.utils.Pair;


public class GroupByAggregationService {
  private GroupBy _groupByQuery;
  private List<String> _groupByColumns;
  private int _groupByTopN;
  private List<AggregationFunction> _aggregationFunctionList;
  private long _numDocsScanned = 0;
  private HashMap<String, List<Serializable>> _aggregateGroupedValue = new HashMap<String, List<Serializable>>();

  public void init(List<AggregationInfo> aggregationInfos, GroupBy groupByQuery) {
    this._aggregationFunctionList = AggregationFunctionFactory.getAggregationFunction(aggregationInfos);
    _groupByQuery = groupByQuery;
    _groupByColumns = groupByQuery.getColumns();
    _groupByTopN = (int) groupByQuery.getTopN();
  }

  public void aggregate(int docId, IndexSegment indexSegment) {
    List<Serializable> groupedValues = null;
    _numDocsScanned++;

    String groupedKey = getGroupedKey(docId, indexSegment);
    if (_aggregateGroupedValue.containsKey(groupedKey)) {
      groupedValues = _aggregateGroupedValue.get(groupedKey);
      for (int j = 0; j < _aggregationFunctionList.size(); ++j) {
        AggregationFunction aggregationFunction = _aggregationFunctionList.get(j);
        String functionName = aggregationFunction.getFunctionName();
        groupedValues.set(j, aggregationFunction.aggregate(groupedValues.get(j), docId, indexSegment));
      }
    } else {
      groupedValues = new ArrayList<Serializable>();
      for (int j = 0; j < _aggregationFunctionList.size(); ++j) {
        AggregationFunction aggregationFunction = _aggregationFunctionList.get(j);
        groupedValues.add(aggregationFunction.aggregate((String) null, docId, indexSegment));
      }
    }
    _aggregateGroupedValue.put(groupedKey, groupedValues);
  }

  public HashMap<String, List<Serializable>> getAggregationGroupByResult() {
    return _aggregateGroupedValue;
  }

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
          groupedValues.set(j, aggregationFunction.aggregate(groupedValues.get(j), i, indexSegment));
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

  public List<HashMap<String, List<Serializable>>> combine(
      List<HashMap<String, List<Serializable>>> aggregationResultList, CombineLevel combineLevel) {
    if (aggregationResultList == null || aggregationResultList.isEmpty()) {
      return null;
    }
    HashMap<String, List<Serializable>> combinedGroupByResults = aggregationResultList.get(0);
    for (int i = 1; i < aggregationResultList.size(); ++i) {
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

  private List<Serializable> combineTwoGroupByValues(List<Serializable> combinedResults,
      List<Serializable> toBeCombinedResults) {
    for (int j = 0; j < combinedResults.size(); ++j) {
      combinedResults.set(j,
          _aggregationFunctionList.get(j).combineTwoValues(combinedResults.get(j), toBeCombinedResults.get(j)));
    }
    return combinedResults;
  }

  //  public HashMap<String, List<Serializable>> reduce(List<HashMap<String, List<Serializable>>> combinedResultList) {
  //    if (combinedResultList == null || combinedResultList.isEmpty()) {
  //      return null;
  //    }
  //    HashMap<String, List<Serializable>> reducedGroupByResults = combinedResultList.get(0);
  //    List<Serializable> currentResult = new ArrayList<Serializable>();
  //    for (int i = 0; i < combinedResultList.size(); ++i) {
  //      HashMap<String, List<Serializable>> toBeReducedGroupByResults = combinedResultList.get(i);
  //      for (String groupedKey : toBeReducedGroupByResults.keySet()) {
  //        if (reducedGroupByResults.containsKey(groupedKey)) {
  //          reducedGroupByResults
  //              .put(
  //                  groupedKey,
  //                  combineTwoGroupByValues(reducedGroupByResults.get(groupedKey),
  //                      toBeReducedGroupByResults.get(groupedKey), currentResult));
  //        } else {
  //          reducedGroupByResults.put(groupedKey, toBeReducedGroupByResults.get(groupedKey));
  //        }
  //      }
  //    }
  //    return reducedGroupByResults;
  //  }

  public Map<String, List<Serializable>> reduce(List<DataTable> combinedResultList) {
    if (combinedResultList == null || combinedResultList.isEmpty()) {
      return null;
    }
    Map<String, List<Serializable>> reducedResult = new HashMap<String, List<Serializable>>();

    for (int i = 0; i < combinedResultList.size(); ++i) {
      DataTable toBeReducedGroupByResults = combinedResultList.get(i);
      for (int rowId = 0; rowId < toBeReducedGroupByResults.getNumberOfRows(); ++rowId) {
        String groupedKey = toBeReducedGroupByResults.getString(rowId, 0);
        if (!reducedResult.containsKey(groupedKey)) {
          List<Serializable> groupedValues = constructGroupedValuesFromDataTableRow(toBeReducedGroupByResults, rowId);
          reducedResult.put(groupedKey, groupedValues);
        } else {
          List<Serializable> groupedValues = constructGroupedValuesFromDataTableRow(toBeReducedGroupByResults, rowId);
          reducedResult.put(groupedKey, combineTwoGroupByValues(reducedResult.get(groupedKey), groupedValues));
        }
      }
    }
    return reducedResult;
  }

  private List<Serializable> constructGroupedValuesFromDataTableRow(DataTable toBeReducedGroupByResults, int rowId) {
    List<Serializable> groupedValues = new ArrayList<Serializable>();
    DataSchema dataSchema = toBeReducedGroupByResults.getDataSchema();
    for (int colId = 1; colId < toBeReducedGroupByResults.getNumberOfCols(); ++colId) {
      switch (dataSchema.getColumnType(colId)) {
        case LONG:
          groupedValues.add(toBeReducedGroupByResults.getLong(rowId, colId));
          break;
        case DOUBLE:
          groupedValues.add(toBeReducedGroupByResults.getDouble(rowId, colId));
          break;
        case INT:
          groupedValues.add(toBeReducedGroupByResults.getInt(rowId, colId));
          break;
        case FLOAT:
          groupedValues.add(toBeReducedGroupByResults.getFloat(rowId, colId));
          break;
        case STRING:
          groupedValues.add(toBeReducedGroupByResults.getString(rowId, colId));
          break;
        case OBJECT:
          groupedValues.add(toBeReducedGroupByResults.getObject(rowId, colId));
          break;

        default:
          break;
      }

    }
    return groupedValues;
  }

  public List<JSONObject> render(Map<String, List<Serializable>> finalAggregationResult) {
    try {
      DecimalFormat df = new DecimalFormat("#");
      List<JSONObject> retJsonResultList = new ArrayList<JSONObject>();
      for (int i = 0; i < _aggregationFunctionList.size(); ++i) {
        JSONArray groupByResultsArray = new JSONArray();
        PriorityQueue priorityQueue = null;

        switch (_aggregationFunctionList.get(i).aggregateResultDataType()) {
          case LONG:
            priorityQueue = getLongGroupedValuePairPriorityQueue();
            break;
          case DOUBLE:
            priorityQueue = getDoubleGroupedValuePairPriorityQueue();
            break;

          default:
            break;
        }
        if (priorityQueue == null) {
          for (String groupedKey : finalAggregationResult.keySet()) {
            JSONObject groupByResultObject = new JSONObject();
            groupByResultObject.put("group", new JSONArray(groupedKey.split("\t")));
            groupByResultObject.put("value", df.format(finalAggregationResult.get(groupedKey).get(i)));
            groupByResultsArray.put(groupByResultObject);
          }
        } else {
          for (String groupedKey : finalAggregationResult.keySet()) {
            priorityQueue.enqueue(new Pair(finalAggregationResult.get(groupedKey).get(i), groupedKey));
            if (priorityQueue.size() == _groupByTopN + 1) {
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
            groupByResultObject.put("group", new JSONArray(((String) res.getSecond()).split("\t")));

            groupByResultObject.put("value", df.format(res.getFirst()));
            groupByResultsArray.put(realGroupSize - 1 - j, groupByResultObject);
          }
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

  private String getGroupedKey(int docId, IndexSegment indexSegment) {
    String groupedKey = indexSegment.getColumnarReader(_groupByColumns.get(0)).getRawValue(docId).toString();
    for (int i = 1; i < _groupByColumns.size(); ++i) {
      ColumnarReader reader = indexSegment.getColumnarReader(_groupByColumns.get(i));
      groupedKey += '\t' + reader.getStringValue(docId);
    }
    return groupedKey;
  }

  public static DataTable transformGroupByResultToDataTable(Map<String, List<Serializable>> groupByResult,
      List<AggregationFunction> aggregationFunctionList) throws Exception {
    DataSchema dataSchema = buildDataSchema(aggregationFunctionList);
    DataTableBuilder dataTableBuilder = new DataTableBuilder(dataSchema);
    dataTableBuilder.open();
    for (String groupedKey : groupByResult.keySet()) {
      dataTableBuilder.startRow();
      List<Serializable> row = groupByResult.get(groupedKey);
      dataTableBuilder.setColumn(0, groupedKey);
      for (int i = 0; i < row.size(); ++i) {
        switch (dataSchema.getColumnType(i + 1)) {
          case DOUBLE:
            dataTableBuilder.setColumn(i + 1, ((Double) row.get(i)).doubleValue());
            break;
          case LONG:
            dataTableBuilder.setColumn(i + 1, ((Long) row.get(i)).longValue());
            break;
          default:
            dataTableBuilder.setColumn(i + 1, row.get(i));
            break;
        }
      }

      dataTableBuilder.finishRow();
    }
    dataTableBuilder.seal();
    return dataTableBuilder.build();
  }

  public static DataSchema buildDataSchema(List<AggregationFunction> aggregationFunctionList) {
    int size = aggregationFunctionList.size() + 1;
    String[] columnNames = new String[size];
    DataType[] columnTypes = new DataType[size];
    columnNames[0] = "groupedKeys";
    columnTypes[0] = DataType.STRING;
    for (int i = 1; i < size; ++i) {
      columnNames[i] = aggregationFunctionList.get(i - 1).getFunctionName();
      columnTypes[i] = aggregationFunctionList.get(i - 1).aggregateResultDataType();
    }
    return new DataSchema(columnNames, columnTypes);
  }

  public List<AggregationFunction> getAggregationFunctionList() {
    return _aggregationFunctionList;
  }

  public long getNumDocsScanned() {
    return _numDocsScanned;
  }

  public HashMap<String, List<Serializable>> combine(HashMap<String, List<Serializable>> combinedGroupByResults,
      HashMap<String, List<Serializable>> toBeCombinedGroupByResults) {
    if (combinedGroupByResults == null) {
      return toBeCombinedGroupByResults;
    }
    if (toBeCombinedGroupByResults == null) {
      return combinedGroupByResults;
    }

    for (String groupedKey : toBeCombinedGroupByResults.keySet()) {
      if (combinedGroupByResults.containsKey(groupedKey)) {
        combinedGroupByResults
            .put(
                groupedKey,
                combineTwoGroupByValues(combinedGroupByResults.get(groupedKey),
                    toBeCombinedGroupByResults.get(groupedKey)));
      } else {
        combinedGroupByResults.put(groupedKey, toBeCombinedGroupByResults.get(groupedKey));
      }
    }

    return combinedGroupByResults;
  }

  public Map<String, List<Serializable>> reduce(Map<ServerInstance, DataTable> instanceResponseMap) {
    if (instanceResponseMap == null || instanceResponseMap.isEmpty()) {
      return null;
    }
    Map<String, List<Serializable>> reducedResult = new HashMap<String, List<Serializable>>();

    for (DataTable toBeReducedGroupByResults : instanceResponseMap.values()) {
      for (int rowId = 0; rowId < toBeReducedGroupByResults.getNumberOfRows(); ++rowId) {
        String groupedKey = toBeReducedGroupByResults.getString(rowId, 0);
        if (!reducedResult.containsKey(groupedKey)) {
          List<Serializable> groupedValues = constructGroupedValuesFromDataTableRow(toBeReducedGroupByResults, rowId);
          reducedResult.put(groupedKey, groupedValues);
        } else {
          List<Serializable> groupedValues = constructGroupedValuesFromDataTableRow(toBeReducedGroupByResults, rowId);
          reducedResult.put(groupedKey, combineTwoGroupByValues(reducedResult.get(groupedKey), groupedValues));
        }
      }
    }
    return reducedResult;
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
}
