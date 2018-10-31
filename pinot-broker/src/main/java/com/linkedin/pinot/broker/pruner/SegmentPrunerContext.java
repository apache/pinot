/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
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
package com.linkedin.pinot.broker.pruner;

import com.linkedin.pinot.common.request.BrokerRequest;
import com.linkedin.pinot.common.utils.request.FilterQueryTree;
import com.linkedin.pinot.common.utils.request.RequestUtils;


/**
 * Class to represent context information for pruner. This is a place holder for
 * information that does not need to be recomputed over and over again for each segment
 * for a given query. For example, we don't want to re-generate the same filter query tree for
 * each segment being pruned.
 */
public class SegmentPrunerContext {
  BrokerRequest _brokerRequest;
  FilterQueryTree _filterQueryTree;

  public SegmentPrunerContext(BrokerRequest brokerRequest) {
    _brokerRequest = brokerRequest;
    _filterQueryTree = RequestUtils.generateFilterQueryTree(brokerRequest);
  }

  public BrokerRequest getBrokerRequest() {
    return _brokerRequest;
  }

  public FilterQueryTree getFilterQueryTree() {
    return _filterQueryTree;
  }
}
