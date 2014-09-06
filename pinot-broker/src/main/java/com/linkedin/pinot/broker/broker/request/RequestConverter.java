package com.linkedin.pinot.broker.broker.request;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import org.antlr.runtime.RecognitionException;
import org.json.JSONArray;
import org.json.JSONObject;

import com.linkedin.pinot.broker.broker.request.filter.FilterQueryTreeConstructor;
import com.linkedin.pinot.common.request.AggregationInfo;
import com.linkedin.pinot.common.request.BrokerRequest;
import com.linkedin.pinot.common.request.GroupBy;
import com.linkedin.pinot.common.request.QuerySource;
import com.linkedin.pinot.common.request.Selection;
import com.linkedin.pinot.common.request.SelectionSort;
import com.linkedin.pinot.common.utils.request.FilterQueryTree;
import com.linkedin.pinot.common.utils.request.RequestUtils;
import com.linkedin.pinot.pql.parsers.PQLCompiler;


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
    final BrokerRequest req = new BrokerRequest();

    /*
     * lets set query source
     */

    final QuerySource source = new QuerySource();
    if (requestJSON.has(COLLECTION)) {
      final String collection = requestJSON.getString(COLLECTION);
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
    boolean setSelection = false;

    if (requestJSON.has(META)) {
      final Selection selection = new Selection();
      if (requestJSON.getJSONObject(META).has(SELECT_LIST)) {
        final JSONArray selectionsArr = requestJSON.getJSONObject(META).getJSONArray(SELECT_LIST);

        final List<String> columns = new ArrayList<String>();

        for (int i = 0; i < selectionsArr.length(); i++) {
          final String s = selectionsArr.getString(i);
          if (!s.trim().equals(STAR)) {
            setSelection = true;
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
        final List<SelectionSort> selectionsSorts = new ArrayList<SelectionSort>();
        final JSONArray sorts = requestJSON.getJSONArray(SORT);
        for (int i = 0; i < sorts.length(); i++) {
          final String key = (String) sorts.getJSONObject(i).keys().next();
          final String val = sorts.getJSONObject(i).getString(key);
          final SelectionSort sort = new SelectionSort();
          sort.setColumn(key);
          sort.setIsAsc(val.equals("asc"));
          selectionsSorts.add(sort);
        }
        selection.setSelectionSortSequence(selectionsSorts);
      }
      if (setSelection) {
        req.setSelections(selection);
      }
    }

    /*
     * Lets handle the agg functions now
     */

    final List<AggregationInfo> aggInfos = new ArrayList<AggregationInfo>();

    if (requestJSON.has(MAP_REDUCE)) {
      if (requestJSON.getJSONObject(MAP_REDUCE).getString(MAP_REDUCE_FUNCTION).equals(COMPOSITE_MR)) {
        final JSONArray aggs = requestJSON.getJSONObject(MAP_REDUCE).getJSONObject("parameters").getJSONArray("array");

        if (aggs.getJSONObject(0).getString("mapReduce").equals("sensei.groupBy")) {
          final GroupBy groupBy = new GroupBy();
          final HashSet<String> groupByColumns = new HashSet<String>();
          for (int i = 0; i < aggs.length(); i++) {
            final AggregationInfo inf = new AggregationInfo();
            final JSONArray columns = aggs.getJSONObject(i).getJSONArray("columns");
            for (int j = 0; j < columns.length(); j++) {
              groupByColumns.add(columns.getString(j));
            }
            inf.setAggregationType(aggs.getJSONObject(i).getString("function"));
            final Map<String, String> params = new HashMap<String, String>();
            params.put("column", aggs.getJSONObject(i).getString("metric"));
            inf.setAggregationParams(params);
            aggInfos.add(inf);
          }
          groupBy.setTopN(requestJSON.getJSONObject("groupBy").getLong("top"));
          final String[] a = new String[groupByColumns.size()];
          groupBy.setColumns(Arrays.asList(groupByColumns.toArray(a)));
          req.setGroupBy(groupBy);
        } else {
          for (int i = 0; i < aggs.length(); i++) {
            final AggregationInfo inf = new AggregationInfo();

            inf.setAggregationType(aggs.getJSONObject(i).getString(MAP_REDUCE));
            final Map<String, String> params = new HashMap<String, String>();
            params.put("column", aggs.getJSONObject(i).getString("column"));
            inf.setAggregationParams(params);

            aggInfos.add(inf);
          }
        }
      } else if (requestJSON.getJSONObject(MAP_REDUCE).getString(MAP_REDUCE_FUNCTION).equals("sensei.groupBy")) {
        final JSONObject parameters = requestJSON.getJSONObject(MAP_REDUCE).getJSONObject("parameters");
        final List<String> cols = new ArrayList<String>();
        final JSONArray columns = parameters.getJSONArray("columns");
        for (int i = 0; i < columns.length(); i++) {
          cols.add(columns.getString(i));
        }

        final GroupBy gBy = new GroupBy();
        gBy.setTopN(requestJSON.getJSONObject("groupBy").getLong("top"));
        gBy.setColumns(cols);
        req.setGroupBy(gBy);

        final AggregationInfo inf = new AggregationInfo();
        inf.setAggregationType(parameters.getString("function"));
        final Map<String, String> params = new HashMap<String, String>();
        params.put("column", parameters.getString("metric"));
        inf.setAggregationParams(params);
        aggInfos.add(inf);
      } else {

        final AggregationInfo inf = new AggregationInfo();

        inf.setAggregationType(requestJSON.getJSONObject(MAP_REDUCE).getJSONObject("parameters").getString(MAP_REDUCE));
        final Map<String, String> params = new HashMap<String, String>();
        params.put("column", requestJSON.getJSONObject(MAP_REDUCE).getJSONObject("parameters").getString("column"));
        inf.setAggregationParams(params);

        aggInfos.add(inf);
      }
    }

    if (aggInfos.size() > 0) {
      req.setAggregationsInfo(aggInfos);
    }

    if (requestJSON.has("filter")) {
      final FilterQueryTree filterQuery =
          FilterQueryTreeConstructor.constructFilter(requestJSON.getJSONObject("filter"));
      RequestUtils.generateFilterFromTree(filterQuery, req);
    }
    return req;
  }

  public static void main(String[] args) throws RecognitionException {
    final PQLCompiler requestCompiler = new PQLCompiler(new HashMap<String, String[]>());
    System.out.println(requestCompiler
        .compile("select count('1'),sum('column') from x where y='ew1' group by c1,c2 top 10 limit 0"));
    System.out.println(requestCompiler.compile("select count(*) from x where y='ew1' group by c1,c2"));
    System.out.println(requestCompiler.compile("select count(*) from x where y='ew1' "));
  }
}
