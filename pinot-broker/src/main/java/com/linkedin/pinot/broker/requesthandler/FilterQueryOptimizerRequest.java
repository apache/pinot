/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
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
package com.linkedin.pinot.broker.requesthandler;

import com.linkedin.pinot.common.utils.request.FilterQueryTree;


public class FilterQueryOptimizerRequest {

  private FilterQueryTree _filterQueryTree = null;
  private String _timeColumn = null;

  public FilterQueryOptimizerRequest(FilterQueryOptimizerRequestBuilder builder) {
    _filterQueryTree = builder._filterQueryTree;
    _timeColumn = builder._timeColumn;
  }

  public FilterQueryTree getFilterQueryTree() {
    return _filterQueryTree;
  }

  public String getTimeColumn() {
    return _timeColumn;
  }

  public void setFilterQueryTree(FilterQueryTree filterQueryTree) {
    _filterQueryTree = filterQueryTree;
  }

  public static class FilterQueryOptimizerRequestBuilder {
    private FilterQueryTree _filterQueryTree;
    private String _timeColumn;

    public FilterQueryOptimizerRequestBuilder setFilterQueryTree(FilterQueryTree filterQueryTree) {
      _filterQueryTree = filterQueryTree;
      return this;
    }

    public FilterQueryOptimizerRequestBuilder setTimeColumn(String timeColumn) {
      _timeColumn = timeColumn;
      return this;
    }

    public FilterQueryOptimizerRequest build() {
      return new FilterQueryOptimizerRequest(this);
    }
  }
}
