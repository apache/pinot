package com.linkedin.pinot.broker.broker.request;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import com.linkedin.pinot.broker.broker.request.filter.FilterQueryTreeConstructor;
import com.linkedin.pinot.common.request.AggregationInfo;
import com.linkedin.pinot.common.request.BrokerRequest;
import com.linkedin.pinot.common.request.QuerySource;
import com.linkedin.pinot.common.request.Selection;
import com.linkedin.pinot.common.request.SelectionSort;
import com.linkedin.pinot.common.utils.request.FilterQueryTree;
import com.linkedin.pinot.common.utils.request.RequestUtils;


public class RequestConverter {

  public static final String COLLECTION = "collection";

  /*
   * 
   * Paging
   */
  public static final String PAGING_SIZE = "size";
  public static final String PAGING_FROM = "from";

  /*
   * 
   * Group By
   */
  public static final String GROUPBY = "groupBy";
  public static final String GROUPBY_COLUMN = "column";
  public static final String GROUPBY_TOP = "top";

  /*
   * 
   * Selections
   */

  public static final String META = "meta";
  public static final String SELECT_LIST = "select_list";
  public static final String STAR = "*";

  public static final String SELECTIONS = "selections";
  public static final String SELECTIONS_TERM = "term";
  public static final String SELECTIONS_TERM_VALUE = "value";
  public static final String SELECTIONS_TERMS = "terms";
  public static final String SELECTIONS_TERMS_VALUES = "values";
  public static final String SELECTIONS_TERMS_EXCLUDES = "excludes";
  public static final String SELECTIONS_TERMS_OPERATOR = "operator";
  public static final String SELECTIONS_TERMS_OPERATOR_OR = "or";
  public static final String SELECTIONS_TERMS_OPERATOR_AND = "and";
  public static final String SELECTIONS_RANGE = "range";
  public static final String SELECTIONS_RANGE_FROM = "from";
  public static final String SELECTIONS_RANGE_TO = "to";
  public static final String SELECTIONS_RANGE_INCLUDE_LOWER = "include_lower";
  public static final String SELECTIONS_RANGE_INCLUDE_UPPER = "include_upper";
  public static final String SELECTIONS_PATH = "path";
  public static final String SELECTIONS_PATH_VALUE = "value";
  public static final String SELECTIONS_PATH_STRICT = "strict";
  public static final String SELECTIONS_PATH_DEPTH = "depth";
  public static final String SELECTIONS_CUSTOM = "custom";
  public static final String SELECTIONS_DEFAULT = "default";

  /*
   * 
   * Sort
   */
  public static final String SORT = "sort";
  public static final String SORT_ASC = "asc";
  public static final String SORT_DESC = "desc";

  /*
   * 
   * Aggregation Functions
   */
  public static final String MAP_REDUCE = "mapReduce";
  public static final String MAP_REDUCE_FUNCTION = "function";
  public static final String MAP_REDUCE_PARAMETERS = "parameters";
  public static final String COMPOSITE_MR = "sensei.composite";

  public static BrokerRequest fromJSON(JSONObject requestJSON) throws Exception {
    BrokerRequest req = new BrokerRequest();

    /*
     * lets set query source
     */

    QuerySource source = new QuerySource();
    if (requestJSON.has(COLLECTION)) {
      String collection = requestJSON.getString(COLLECTION);
      if (collection.contains(".")) {
        source.setResourceName(collection.split(".")[0]);
        source.setTableName(collection.split(".")[1]);
      } else {
        source.setResourceName(collection);
        source.setTableName(collection);
      }
    }

    req.setQuerySource(source);

    /*
     * Lets handle the selections first
     */
    Selection selection = new Selection();

    if (requestJSON.has(META)) {
      if (requestJSON.has(SELECT_LIST)) {
        JSONArray selectionsArr = requestJSON.getJSONObject(META).getJSONArray(SELECT_LIST);
        List<String> columns = new ArrayList<String>();

        for (int i = 0; i < selectionsArr.length(); i++) {
          String s = selectionsArr.getString(i);
          if (!s.trim().equals(STAR)) {
            columns.add(s.trim());
          }
        }

        selection.setSelectionColumns(columns);
      }

      if (requestJSON.has(PAGING_FROM)) {
        selection.setOffset(requestJSON.getInt(PAGING_FROM));
      }

      if (requestJSON.has(PAGING_SIZE)) {
        selection.setSize(requestJSON.getInt(PAGING_SIZE));
      }

      if (requestJSON.has(SORT)) {
        List<SelectionSort> selectionsSorts = new ArrayList<SelectionSort>();
        JSONArray sorts = requestJSON.getJSONArray(SORT);
        for (int i = 0; i < sorts.length(); i++) {
          String key = (String) sorts.getJSONObject(i).keys().next();
          String val = sorts.getJSONObject(i).getString(key);
          SelectionSort sort = new SelectionSort();
          sort.setColumn(key);
          sort.setIsAsc(val.equals("asc"));
          selectionsSorts.add(sort);
        }
        selection.setSelectionSortSequence(selectionsSorts);
      }
    }
    req.setSelections(selection);

    /*
     * Lets handle the agg functions now
     */

    if (requestJSON.has(GROUPBY)) {
      throw new UnsupportedOperationException("no group by support yet");
    }

    List<AggregationInfo> aggInfos = new ArrayList<AggregationInfo>();

    if (requestJSON.has(MAP_REDUCE)) {
      if (requestJSON.getJSONObject(MAP_REDUCE).getString(MAP_REDUCE_FUNCTION).equals(COMPOSITE_MR)) {
        JSONArray aggs = requestJSON.getJSONObject(MAP_REDUCE).getJSONObject("parameters").getJSONArray("array");
        for (int i = 0; i < aggs.length(); i++) {
          AggregationInfo inf = new AggregationInfo();

          inf.setAggregationType(aggs.getJSONObject(i).getString(MAP_REDUCE));
          Map<String, String> params = new HashMap<String, String>();
          params.put("column", aggs.getJSONObject(i).getString("column"));
          inf.setAggregationParams(params);

          aggInfos.add(inf);
        }
      } else {
        AggregationInfo inf = new AggregationInfo();

        inf.setAggregationType(requestJSON.getJSONObject(MAP_REDUCE).getJSONObject("parameters").getString(MAP_REDUCE));
        Map<String, String> params = new HashMap<String, String>();
        params.put("column", requestJSON.getJSONObject(MAP_REDUCE).getJSONObject("parameters").getString("column"));
        inf.setAggregationParams(params);

        aggInfos.add(inf);
      }
    }

    req.setAggregationsInfo(aggInfos); 

    FilterQueryTree filterQuery = FilterQueryTreeConstructor.constructFilter(requestJSON.getJSONObject("filter"));
    RequestUtils.generateFilterFromTree(filterQuery, req);
    return req;
  }

}
