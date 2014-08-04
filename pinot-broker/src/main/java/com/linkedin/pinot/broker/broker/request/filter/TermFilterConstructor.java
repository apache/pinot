package com.linkedin.pinot.broker.broker.request.filter;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.json.JSONObject;

import com.linkedin.pinot.common.request.FilterOperator;
import com.linkedin.pinot.common.utils.request.FilterQueryTree;


public class TermFilterConstructor extends FilterQueryTreeConstructor {
  public static final String FILTER_TYPE = "term";

  @Override
  protected FilterQueryTree doConstructFilter(Object param) throws Exception {
    JSONObject json = (JSONObject) param;

    Iterator<String> iter = json.keys();
    if (!iter.hasNext())
      return null;

    String field = iter.next();
    String text = json.getJSONObject(field).getString("value");
    List<String> vals = new ArrayList<String>();
    vals.add(text);
    return new FilterQueryTree(this.hashCode(), field, vals, FilterOperator.EQUALITY, null);
  }

}
