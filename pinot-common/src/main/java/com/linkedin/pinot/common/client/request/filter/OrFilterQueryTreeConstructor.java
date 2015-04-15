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
    return new FilterQueryTree(this.hashCode(), null, null, FilterOperator.OR, filters);
  }

}
