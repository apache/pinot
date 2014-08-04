package com.linkedin.pinot.broker.broker.request.filter;

import java.util.ArrayList;
import java.util.List;

import org.json.JSONArray;

import com.linkedin.pinot.common.request.FilterOperator;
import com.linkedin.pinot.common.utils.request.FilterQueryTree;


public class AndFilterQueryTreeConstructor extends FilterQueryTreeConstructor {
  public static final String FILTER_TYPE = "and";

  @Override
  protected FilterQueryTree doConstructFilter(Object json) throws Exception {
    JSONArray filterArray = (JSONArray) json;
    List<FilterQueryTree> filters = new ArrayList<FilterQueryTree>(filterArray.length());
    for (int i = 0; i < filterArray.length(); ++i) {
      FilterQueryTree filter = FilterQueryTreeConstructor.constructFilter(filterArray.getJSONObject(i));
      if (filter != null)
        filters.add(filter);
    }
    FilterQueryTree filter = new FilterQueryTree(this.hashCode(), null, null, FilterOperator.AND, filters);
    return filter;
  }

}
