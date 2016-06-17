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
 * QueryPlanCreator will take the dependency relationship for JobVertexes then create an immutable DAG to represent the
 * sequence to process segments.
 *
 */
public class QueryPlanCreator {

  private JobVertex _virtualRoot = null;

  private BrokerRequest _brokerRequest = null;

  public BrokerRequest getQuery() {
    return _brokerRequest;
  }

  public void setQuery(BrokerRequest brokerRequest) {
    _brokerRequest = brokerRequest;
  }

  private void setVirtualRoot(JobVertex virtualRoot) {
    _virtualRoot = virtualRoot;
    _virtualRoot.setIndexSegmentList(null);
  }

  public QueryPlanCreator(BrokerRequest brokerRequest) {
    setVirtualRoot(new JobVertex(null));
    setQuery(brokerRequest);
  }

  public void addJobVertexWithDependency(JobVertex jobVertexFrom, JobVertex jobVertexTo) {
    if (jobVertexFrom == null) {
      _virtualRoot.addSuccessor(jobVertexTo);
      jobVertexTo.addParent(_virtualRoot);
    } else {
      jobVertexFrom.addSuccessor(jobVertexTo);
      jobVertexTo.addParent(jobVertexFrom);
    }
  }

  public QueryPlan buildQueryPlan() {
    return new QueryPlan(_brokerRequest, _virtualRoot);
  }

}
