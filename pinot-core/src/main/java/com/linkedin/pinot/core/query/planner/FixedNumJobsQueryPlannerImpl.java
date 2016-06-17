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

import java.util.ArrayList;
import java.util.List;

import com.linkedin.pinot.common.request.BrokerRequest;
import com.linkedin.pinot.core.indexsegment.IndexSegment;


/**
 * An implementation of QueryPlanner, will evenly distributed segments to a given number of jobs.
 *
 */
public class FixedNumJobsQueryPlannerImpl implements QueryPlanner {

  int _numJobs = 0;

  public FixedNumJobsQueryPlannerImpl() {
    super();
  }

  public FixedNumJobsQueryPlannerImpl(int numJobs) {
    super();
    setNumJobs(numJobs);
  }

  public void setNumJobs(int numJobs) {
    _numJobs = numJobs;
  }

  @Override
  public QueryPlan computeQueryPlan(BrokerRequest brokerRequest, List<IndexSegment> indexSegmentList) {
    QueryPlanCreator queryPlanCreator = new QueryPlanCreator(brokerRequest);
    List<JobVertex> jobVertexList = new ArrayList<JobVertex>();
    for (int i = 0; i < _numJobs; ++i) {
      jobVertexList.add(new JobVertex(new ArrayList<IndexSegment>()));
    }
    int i = 0;
    for (IndexSegment indexSegment : indexSegmentList) {
      (jobVertexList.get(i++).getIndexSegmentList()).add(indexSegment);
      if (i == _numJobs) {
        i = 0;
      }
    }
    for (JobVertex jobVertex : jobVertexList) {
      queryPlanCreator.addJobVertexWithDependency(null, jobVertex);
    }
    return queryPlanCreator.buildQueryPlan();
  }
}
