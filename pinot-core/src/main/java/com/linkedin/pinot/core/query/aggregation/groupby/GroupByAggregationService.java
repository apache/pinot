package com.linkedin.pinot.core.query.aggregation.groupby;

import it.unimi.dsi.fastutil.PriorityQueue;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
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
import com.linkedin.pinot.common.utils.DataTableBuilder;
import com.linkedin.pinot.common.utils.DataTableBuilder.DataSchema;
import com.linkedin.pinot.core.indexsegment.IndexSegment;
import com.linkedin.pinot.core.query.aggregation.AggregationFunction;
import com.linkedin.pinot.core.query.aggregation.AggregationFunctionFactory;
import com.linkedin.pinot.core.query.utils.Pair;
import com.linkedin.pinot.core.query.utils.TrieNode;


/**
 * GroupByAggregationService is initialized by aggregation functions and groupBys.
 * aggregate(..) function will create a tree for each group, the routing is the groupKeys.
 * Each leaf node will contain the intermediate aggregation result for this group.
 * When aggregate a new docId, first go through the tree to find if the group is already there.
 * If not, will create all the routing nodes and insert the aggregated result at leaf.
 * If already there, then just do merge with existed aggregation result. 
 *  
 * @author xiafu
 *
 */
public class GroupByAggregationService {
  private final List<String> _groupByColumns;
  private final int _groupByTopN;
  private final List<AggregationFunction> _aggregationFunctionList;
  private long _numDocsScanned = 0;
  private HashMap<String, List<Serializable>> _aggregateGroupedValue = new HashMap<String, List<Serializable>>();

  private TrieNode _rootNode = null;

  public GroupByAggregationService(List<AggregationInfo> aggregationInfos, GroupBy groupByQuery) {
    _aggregationFunctionList = AggregationFunctionFactory.getAggregationFunction(aggregationInfos);
    _groupByColumns = groupByQuery.getColumns();
    _groupByTopN = (int) groupByQuery.getTopN();
    _rootNode = new TrieNode();
  }

  public void aggregate(int docId, IndexSegment indexSegment) {
    List<Serializable> groupedValues = null;
    _numDocsScanned++;

    TrieNode currentNode = _rootNode;
    for (String groupByColumn : _groupByColumns) {
      if (currentNode.getNextGroupedColumnValues() == null) {
        currentNode.setNextGroupedColumnValues(new Int2ObjectOpenHashMap<TrieNode>());
      }
      int groupKey = indexSegment.getColumnarReader(groupByColumn).getDictionaryId(docId);
      if (!currentNode.getNextGroupedColumnValues().containsKey(groupKey)) {
        currentNode.getNextGroupedColumnValues().put(groupKey, new TrieNode());
      }
      currentNode = currentNode.getNextGroupedColumnValues().get(groupKey);
    }
    if (currentNode.getAggregationResults() == null) {
      groupedValues = new ArrayList<Serializable>();
      for (int j = 0; j < _aggregationFunctionList.size(); ++j) {
        AggregationFunction aggregationFunction = _aggregationFunctionList.get(j);
        groupedValues.add(aggregationFunction.aggregate((String) null, docId, indexSegment));
      }
      currentNode.setAggregationResults(groupedValues);
    } else {
      groupedValues = currentNode.getAggregationResults();
      for (int j = 0; j < _aggregationFunctionList.size(); ++j) {
        AggregationFunction aggregationFunction = _aggregationFunctionList.get(j);
        groupedValues.set(j, aggregationFunction.aggregate(groupedValues.get(j), docId, indexSegment));
      }
    }
  }

  public HashMap<String, List<Serializable>> getAggregationGroupByResult(IndexSegment indexSegment) {
    _aggregateGroupedValue.clear();
    List<Integer> groupedKey = new ArrayList<Integer>();
    traverseTrieTree(_rootNode, groupedKey, indexSegment);
    return _aggregateGroupedValue;
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

  public Map<String, List<Serializable>> reduce(List<DataTable> combinedResultList) {
    if ((combinedResultList == null) || combinedResultList.isEmpty()) {
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

  private void traverseTrieTree(TrieNode rootNode, List<Integer> groupedKey, IndexSegment indexSegment) {
    if (rootNode.getNextGroupedColumnValues() != null) {
      for (int key : rootNode.getNextGroupedColumnValues().keySet()) {
        groupedKey.add(key);
        traverseTrieTree(rootNode.getNextGroupedColumnValues().get(key), groupedKey, indexSegment);
        groupedKey.remove(groupedKey.size() - 1);
      }
    } else {
      _aggregateGroupedValue.put(getGroupedKey(groupedKey, indexSegment), rootNode.getAggregationResults());
    }
  }

  private String getGroupedKey(List<Integer> groupedKey, IndexSegment indexSegment) {
    StringBuilder sb = new StringBuilder();
    sb.append(indexSegment.getColumnarReader(_groupByColumns.get(0)).getStringValueFromDictId(groupedKey.get(0)));
    for (int i = 1; i < groupedKey.size(); ++i) {
      sb.append('\t' + indexSegment.getColumnarReader(_groupByColumns.get(i)).getStringValueFromDictId(
          groupedKey.get(i)));
    }
    return sb.toString();
  }

  private List<Serializable> combineTwoGroupByValues(List<Serializable> combinedResults,
      List<Serializable> toBeCombinedResults) {
    for (int j = 0; j < combinedResults.size(); ++j) {
      combinedResults.set(j,
          _aggregationFunctionList.get(j).combineTwoValues(combinedResults.get(j), toBeCombinedResults.get(j)));
    }
    return combinedResults;
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

  public Map<String, List<Serializable>> reduce(Map<ServerInstance, DataTable> instanceResponseMap) {
    if ((instanceResponseMap == null) || instanceResponseMap.isEmpty()) {
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
