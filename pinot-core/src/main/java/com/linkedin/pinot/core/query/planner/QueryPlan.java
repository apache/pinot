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
package com.linkedin.pinot.core.query.planner;

import com.linkedin.pinot.common.request.BrokerRequest;


/**
 * QueryPlan is a DAG to represent the sequence to process segments.
 * The DAG related operation is not thread safe. Only one PlanExecutor will process QueryPlan.
 *
 */
public class QueryPlan {

  private BrokerRequest _brokerRequest = null;
  private JobVertex _virtualRoot = null;

  private void setVirtualRoot(JobVertex virtualRoot) {
    _virtualRoot = virtualRoot;
    _virtualRoot.setIndexSegmentList(null);

  }

  public QueryPlan() {
    setVirtualRoot(new JobVertex(null));
  }

  public QueryPlan(BrokerRequest brokerRequest, JobVertex root) {
    setBrokerRequest(brokerRequest);
    setVirtualRoot(root);
  }

  public JobVertex getVirtualRoot() {
    return _virtualRoot;
  }

  public BrokerRequest getBrokerRequest() {
    return _brokerRequest;
  }

  public void setBrokerRequest(BrokerRequest brokerRequest) {
    _brokerRequest = brokerRequest;
  }

}
