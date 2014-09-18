package com.linkedin.pinot.query.planner;

import static org.testng.AssertJUnit.assertEquals;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.linkedin.pinot.common.request.BrokerRequest;
import com.linkedin.pinot.common.segment.SegmentMetadata;
import com.linkedin.pinot.core.common.Operator;
import com.linkedin.pinot.core.common.Predicate;
import com.linkedin.pinot.core.indexsegment.IndexSegment;
import com.linkedin.pinot.core.indexsegment.IndexType;
import com.linkedin.pinot.core.indexsegment.columnar.readers.ColumnarReader;
import com.linkedin.pinot.core.operator.DataSource;
import com.linkedin.pinot.core.query.planner.FixedNumJobsQueryPlannerImpl;
import com.linkedin.pinot.core.query.planner.FixedNumOfSegmentsPerJobQueryPlannerImpl;
import com.linkedin.pinot.core.query.planner.JobVertex;
import com.linkedin.pinot.core.query.planner.ParallelQueryPlannerImpl;
import com.linkedin.pinot.core.query.planner.QueryPlan;
import com.linkedin.pinot.core.query.planner.QueryPlanner;
import com.linkedin.pinot.core.query.planner.SequentialQueryPlannerImpl;


public class TestQueryPlanner {

  public List<IndexSegment> _indexSegmentList = null;
  public int _numOfSegmentDataManagers = 100;

  @BeforeMethod
  public void init() {
    _indexSegmentList = new ArrayList<IndexSegment>();
    for (int i = 0; i < _numOfSegmentDataManagers; ++i) {
      _indexSegmentList.add(new IndexSegment() {

        @Override
        public String getSegmentName() {
          // TODO Auto-generated method stub
          return null;
        }

        @Override
        public SegmentMetadata getSegmentMetadata() {
          // TODO Auto-generated method stub
          return null;
        }

        @Override
        public IndexType getIndexType() {
          // TODO Auto-generated method stub
          return null;
        }

        @Override
        public DataSource getDataSource(String columnName, Predicate p) {
          // TODO Auto-generated method stub
          return null;
        }

        @Override
        public DataSource getDataSource(String columnName) {
          // TODO Auto-generated method stub
          return null;
        }

        @Override
        public ColumnarReader getColumnarReader(String column) {
          // TODO Auto-generated method stub
          return null;
        }

        @Override
        public String getAssociatedDirectory() {
          // TODO Auto-generated method stub
          return null;
        }

        @Override
        public Iterator<Integer> getDocIdIterator(BrokerRequest brokerRequest) {
          // TODO Auto-generated method stub
          return null;
        }

        @Override
        public DataSource getDataSource(String columnName, Operator op) {
          // TODO Auto-generated method stub
          return null;
        }

        @Override
        public String[] getColumnNames() {
          // TODO Auto-generated method stub
          return null;
        }
      });
    }
  }

  @Test
  public void testParallelQueryPlanner() {
    QueryPlanner queryPlanner = new ParallelQueryPlannerImpl();
    QueryPlan plan = queryPlanner.computeQueryPlan(null, _indexSegmentList);
    List<JobVertex> roots = plan.getVirtualRoot().getSuccessors();
    assertEquals(_numOfSegmentDataManagers, roots.size());
  }

  @Test
  public void testSequentialQueryPlanner() {
    QueryPlanner queryPlanner = new SequentialQueryPlannerImpl();
    QueryPlan plan = queryPlanner.computeQueryPlan(null, _indexSegmentList);
    List<JobVertex> roots = plan.getVirtualRoot().getSuccessors();
    assertEquals(1, roots.size());
    assertEquals(_numOfSegmentDataManagers, roots.get(0).getIndexSegmentList().size());
  }

  @Test
  public void testFixedJobsQueryPlanner() {
    for (int numJobs = 1; numJobs <= _numOfSegmentDataManagers; ++numJobs) {
      int totalSegments = 0;
      QueryPlanner queryPlanner = new FixedNumJobsQueryPlannerImpl(numJobs);
      QueryPlan plan = queryPlanner.computeQueryPlan(null, _indexSegmentList);
      List<JobVertex> roots = plan.getVirtualRoot().getSuccessors();
      assertEquals(roots.size(), numJobs);

      for (int i = numJobs - 1; i >= 0; --i) {
        totalSegments += roots.get(0).getIndexSegmentList().size();
        roots.remove(0);
        assertEquals(roots.size(), i);
      }
      assertEquals(_numOfSegmentDataManagers, totalSegments);
    }
  }

  @Test
  public void testFixedSegmentsPerJobQueryPlanner() {
    for (int numSegment = 1; numSegment <= _numOfSegmentDataManagers; ++numSegment) {
      int totalSegments = 0;
      int numJobs = (int) Math.ceil((double) _numOfSegmentDataManagers / (double) numSegment);
      QueryPlanner queryPlanner = new FixedNumOfSegmentsPerJobQueryPlannerImpl(numSegment);
      QueryPlan plan = queryPlanner.computeQueryPlan(null, _indexSegmentList);
      List<JobVertex> roots = plan.getVirtualRoot().getSuccessors();
      assertEquals(roots.size(), numJobs);
      for (int i = numJobs - 1; i >= 0; --i) {
        totalSegments += roots.get(0).getIndexSegmentList().size();
        roots.remove(0);
        assertEquals(roots.size(), i);
      }
      assertEquals(_numOfSegmentDataManagers, totalSegments);
    }
  }
}
