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
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.json.JSONObject;

import com.linkedin.pinot.common.client.request.RequestConverter;
import com.linkedin.pinot.common.request.FilterOperator;
import com.linkedin.pinot.common.utils.request.FilterQueryTree;


/**
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

    final String[] includes = RequestConverter.getStrings(jsonObj.getJSONArray("values"));
    final String[] excludes = RequestConverter.getStrings(jsonObj.getJSONArray("excludes"));

    if (includes.length > 0) {
      Arrays.sort(includes);
      final List<String> rhs = new ArrayList<String>();
      rhs.add(StringUtils.join(includes, "\t\t"));
      return new FilterQueryTree(field, rhs, FilterOperator.IN, null);
    } else {
      Arrays.sort(excludes);
      final List<String> rhs = new ArrayList<String>();
      rhs.add(StringUtils.join(excludes, "\t\t"));
      if (excludes.length == 1) {
        return new FilterQueryTree(field, rhs, FilterOperator.NOT, null);
      } else {
        return new FilterQueryTree(field, rhs, FilterOperator.NOT_IN, null);
      }
    }
  }
}
