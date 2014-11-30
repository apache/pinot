package com.linkedin.pinot.common.client.request.filter;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.json.JSONObject;

import com.linkedin.pinot.common.client.request.RequestConverter;
import com.linkedin.pinot.common.request.FilterOperator;
import com.linkedin.pinot.common.utils.request.FilterQueryTree;


/**
 * @author Dhaval Patel<dpatel@linkedin.com>
 * Nov 29, 2014
 */

public class TermsFilterQueryTreeConstructor extends FilterQueryTreeConstructor {

  public static final String FILTER_TYPE = "terms";

  @Override
  protected FilterQueryTree doConstructFilter(Object obj) throws Exception {
    final JSONObject json = (JSONObject) obj;

    final Iterator<String> iter = json.keys();
    if (!iter.hasNext()) {
      return null;
    }

    final String field = iter.next();

    final JSONObject jsonObj = json.getJSONObject(field);
    System.out.println(field + " : " + jsonObj.toString());
    final String[] includes = RequestConverter.getStrings(jsonObj.getJSONArray("values"));
    final String[] excludes = RequestConverter.getStrings(jsonObj.getJSONArray("excludes"));

    if (includes.length > 0) {
      Arrays.sort(includes);
      final List<String> rhs = new ArrayList<String>();
      rhs.add(StringUtils.join(includes));
      return new FilterQueryTree(this.hashCode(), field, rhs, FilterOperator.IN, null);
    } else {
      Arrays.sort(excludes);
      final List<String> rhs = new ArrayList<String>();
      rhs.add(StringUtils.join(excludes));
      return new FilterQueryTree(this.hashCode(), field, rhs, FilterOperator.NOT_IN, null);
    }
  }
}
