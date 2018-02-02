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
package com.linkedin.pinot.query.planner;

import com.linkedin.pinot.core.indexsegment.IndexSegment;
import com.linkedin.pinot.core.query.planner.FixedNumJobsQueryPlannerImpl;
import com.linkedin.pinot.core.query.planner.FixedNumOfSegmentsPerJobQueryPlannerImpl;
import com.linkedin.pinot.core.query.planner.JobVertex;
import com.linkedin.pinot.core.query.planner.ParallelQueryPlannerImpl;
import com.linkedin.pinot.core.query.planner.QueryPlan;
import com.linkedin.pinot.core.query.planner.QueryPlanner;
import com.linkedin.pinot.core.query.planner.SequentialQueryPlannerImpl;
import java.util.ArrayList;
import java.util.List;
import org.mockito.Mockito;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.testng.Assert.*;


public class QueryPlannerTest {
  private static final int NUM_SEGMENTS = 100;

  private List<IndexSegment> _indexSegments;

  @BeforeMethod
  public void init() {
    _indexSegments = new ArrayList<>(NUM_SEGMENTS);
    IndexSegment indexSegment = Mockito.mock(IndexSegment.class);
    for (int i = 0; i < NUM_SEGMENTS; i++) {
      _indexSegments.add(indexSegment);
    }
  }

  @Test
  public void testParallelQueryPlanner() {
    QueryPlanner queryPlanner = new ParallelQueryPlannerImpl();
    QueryPlan plan = queryPlanner.computeQueryPlan(null, _indexSegments);
    List<JobVertex> roots = plan.getVirtualRoot().getSuccessors();
    assertEquals(roots.size(), NUM_SEGMENTS);
  }

  @Test
  public void testSequentialQueryPlanner() {
    QueryPlanner queryPlanner = new SequentialQueryPlannerImpl();
    QueryPlan plan = queryPlanner.computeQueryPlan(null, _indexSegments);
    List<JobVertex> roots = plan.getVirtualRoot().getSuccessors();
    assertEquals(roots.size(), 1);
    assertEquals(roots.get(0).getIndexSegmentList().size(), NUM_SEGMENTS);
  }

  @Test
  public void testFixedJobsQueryPlanner() {
    for (int numJobs = 1; numJobs <= NUM_SEGMENTS; ++numJobs) {
      int totalSegments = 0;
      final QueryPlanner queryPlanner = new FixedNumJobsQueryPlannerImpl(numJobs);
      final QueryPlan plan = queryPlanner.computeQueryPlan(null, _indexSegments);
      final List<JobVertex> roots = plan.getVirtualRoot().getSuccessors();
      assertEquals(roots.size(), numJobs);

      for (int i = numJobs - 1; i >= 0; --i) {
        totalSegments += roots.get(0).getIndexSegmentList().size();
        roots.remove(0);
        assertEquals(roots.size(), i);
      }
      assertEquals(totalSegments, NUM_SEGMENTS);
    }
  }

  @Test
  public void testFixedSegmentsPerJobQueryPlanner() {
    for (int numSegment = 1; numSegment <= NUM_SEGMENTS; ++numSegment) {
      int totalSegments = 0;
      final int numJobs = (int) Math.ceil((double) NUM_SEGMENTS / (double) numSegment);
      final QueryPlanner queryPlanner = new FixedNumOfSegmentsPerJobQueryPlannerImpl(numSegment);
      final QueryPlan plan = queryPlanner.computeQueryPlan(null, _indexSegments);
      final List<JobVertex> roots = plan.getVirtualRoot().getSuccessors();
      assertEquals(roots.size(), numJobs);
      for (int i = numJobs - 1; i >= 0; --i) {
        totalSegments += roots.get(0).getIndexSegmentList().size();
        roots.remove(0);
        assertEquals(roots.size(), i);
      }
      assertEquals(totalSegments, NUM_SEGMENTS);
    }
  }
}
