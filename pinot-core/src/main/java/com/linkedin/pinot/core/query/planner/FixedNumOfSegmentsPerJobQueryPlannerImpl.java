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
 * An implementation of QueryPlanner, will mark all the segments with 0 degree in the graph.
 *
 */
public class FixedNumOfSegmentsPerJobQueryPlannerImpl implements QueryPlanner {

  int _numSegmentsPerJob = 0;

  public FixedNumOfSegmentsPerJobQueryPlannerImpl() {
    super();
  }

  public FixedNumOfSegmentsPerJobQueryPlannerImpl(int numJobs) {
    super();
    setNumJobs(numJobs);
  }

  public void setNumJobs(int numSegmentsPerJob) {
    _numSegmentsPerJob = numSegmentsPerJob;
  }

  @Override
  public QueryPlan computeQueryPlan(BrokerRequest brokerRequest, List<IndexSegment> indexSegmentList) {
    QueryPlanCreator queryPlanCreator = new QueryPlanCreator(brokerRequest);
    List<IndexSegment> vertexSegmentList = new ArrayList<IndexSegment>();
    int i = 0;
    for (IndexSegment indexSegment : indexSegmentList) {

      vertexSegmentList.add(indexSegment);

      if ((++i) % _numSegmentsPerJob == 0) {
        queryPlanCreator.addJobVertexWithDependency(null, new JobVertex(vertexSegmentList));
        vertexSegmentList = new ArrayList<IndexSegment>();
      }
    }
    if (vertexSegmentList.size() > 0) {
      queryPlanCreator.addJobVertexWithDependency(null, new JobVertex(vertexSegmentList));
    }
    return queryPlanCreator.buildQueryPlan();
  }
}
