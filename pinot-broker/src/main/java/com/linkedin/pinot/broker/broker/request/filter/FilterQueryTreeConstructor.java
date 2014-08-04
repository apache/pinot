package com.linkedin.pinot.broker.broker.request.filter;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import com.linkedin.pinot.common.request.FilterOperator;
import com.linkedin.pinot.common.request.FilterQuery;
import com.linkedin.pinot.common.utils.request.FilterQueryTree;


public abstract class FilterQueryTreeConstructor {

  public static final String VALUES_PARAM = "values";
  public static final String VALUE_PARAM = "value";
  public static final String EXCLUDES_PARAM = "excludes";
  public static final String OPERATOR_PARAM = "operator";
  public static final String PARAMS_PARAM = "params";
  public static final String MUST_PARAM = "must";
  public static final String MUST_NOT_PARAM = "must_not";
  public static final String SHOULD_PARAM = "should";
  public static final String FROM_PARAM = "from";
  public static final String TO_PARAM = "to";
  public static final String NOOPTIMIZE_PARAM = "_noOptimize";
  public static final String RANGE_FIELD_TYPE = "_type";
  public static final String RANGE_DATE_FORMAT = "_date_format";
  public static final String QUERY_PARAM = "query";
  public static final String OR_PARAM = "or";
  public static final String DEPTH_PARAM = "depth";
  public static final String STRICT_PARAM = "strict";
  public static final String INCLUDE_LOWER_PARAM = "include_lower";
  public static final String INCLUDE_UPPER_PARAM = "include_upper";
  public static final String GT_PARAM = "gt";
  public static final String GTE_PARAM = "gte";
  public static final String LT_PARAM = "lt";
  public static final String LTE_PARAM = "lte";
  public static final String CLASS_PARAM = "class";

  private static final Map<String, FilterQueryTreeConstructor> FILTER_CONSTRUCTOR_MAP = new HashMap<String, FilterQueryTreeConstructor>();

  static {
    FILTER_CONSTRUCTOR_MAP.put(AndFilterQueryTreeConstructor.FILTER_TYPE, new AndFilterQueryTreeConstructor());
    FILTER_CONSTRUCTOR_MAP.put(TermFilterConstructor.FILTER_TYPE, new TermFilterConstructor());
  }

  public static FilterQueryTreeConstructor getFilterConstructor(String type) {
    return FILTER_CONSTRUCTOR_MAP.get(type);
  }

  public static FilterQueryTree constructFilter(JSONObject json) throws JSONException, Exception {
    if (json == null)
      return null;

    Iterator<String> iter = json.keys();
    if (!iter.hasNext())
      throw new IllegalArgumentException("Filter type not specified: " + json);
    
    String type = iter.next();
    
    FilterQueryTreeConstructor filterConstructor = FilterQueryTreeConstructor.getFilterConstructor(type);
    if (filterConstructor == null)
      throw new IllegalArgumentException("Filter type '" + type + "' not supported");

    return filterConstructor.doConstructFilter(json.get(type));
  }

  abstract protected FilterQueryTree doConstructFilter(Object json/* JSONObject or JSONArray */) throws Exception;
}
