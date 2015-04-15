/**
 * Copyright (C) 2014-2015 LinkedIn Corp. (pinot-core@linkedin.com)
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
package com.linkedin.pinot.common.client.request.filter;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.json.JSONException;
import org.json.JSONObject;

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
    FILTER_CONSTRUCTOR_MAP.put(OrFilterQueryTreeConstructor.FILTER_TYPE, new OrFilterQueryTreeConstructor());
    FILTER_CONSTRUCTOR_MAP.put(RangeFilterQueryTreeConstructor.FILTER_TYPE, new RangeFilterQueryTreeConstructor());
    FILTER_CONSTRUCTOR_MAP.put(TermsFilterQueryTreeConstructor.FILTER_TYPE, new TermsFilterQueryTreeConstructor());
  }

  public static FilterQueryTreeConstructor getFilterConstructor(String type) {
    return FILTER_CONSTRUCTOR_MAP.get(type);
  }

  public static FilterQueryTree constructFilter(JSONObject json) throws JSONException, Exception {
    if (json == null) {
      return null;
    }

    final Iterator<String> iter = json.keys();
    if (!iter.hasNext()) {
      throw new IllegalArgumentException("Filter type not specified: " + json);
    }

    final String type = iter.next();

    final FilterQueryTreeConstructor filterConstructor = FilterQueryTreeConstructor.getFilterConstructor(type);
    if (filterConstructor == null) {
      throw new IllegalArgumentException("Filter type '" + type + "' not supported");
    }

    return filterConstructor.doConstructFilter(json.get(type));
  }

  abstract protected FilterQueryTree doConstructFilter(Object json/* JSONObject or JSONArray */) throws Exception;
}
