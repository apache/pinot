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
    return new FilterQueryTree(field, vals, FilterOperator.EQUALITY, null);
  }

}
