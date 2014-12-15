package com.linkedin.pinot.common.client.request.filter;

import java.util.ArrayList;
import java.util.List;

import org.json.JSONArray;

import com.linkedin.pinot.common.request.FilterOperator;
import com.linkedin.pinot.common.utils.request.FilterQueryTree;


/**
* @author Dhaval Patel<dpatel@linkedin.com>
* Aug 28, 2014
*/

public class OrFilterQueryTreeConstructor extends FilterQueryTreeConstructor {
  
  public static final String FILTER_TYPE = "or";
  
  @Override
  protected FilterQueryTree doConstructFilter(Object json) throws Exception {
    JSONArray filterArray = (JSONArray) json;
    List<FilterQueryTree> filters = new ArrayList<FilterQueryTree>(filterArray.length());
    for (int i = 0; i < filterArray.length(); ++i) {
      FilterQueryTree filter = FilterQueryTreeConstructor.constructFilter(filterArray.getJSONObject(i));
      if (filter != null)
        filters.add(filter);
    }
    FilterQueryTree filter = new FilterQueryTree(this.hashCode(), null, null, FilterOperator.OR, filters);
    return filter;
  }

}
