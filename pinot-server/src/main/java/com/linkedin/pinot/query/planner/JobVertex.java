package com.linkedin.pinot.query.planner;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import com.linkedin.pinot.core.indexsegment.IndexSegment;


/**
 * JobVertex is a vertex in the graph, and will hold a list of segments to be processed in sequential.
 * Each Jobvertex stores its parents and successors, all of them will form a DAG.
 * 
 */
public class JobVertex {
  private List<JobVertex> _parents = new ArrayList<JobVertex>();
  private List<JobVertex> _successors = new ArrayList<JobVertex>();
  private List<IndexSegment> _indexSegmentList = null;
  private Properties _metadata = null;

  public JobVertex(List<IndexSegment> indexSegmentList) {
    _indexSegmentList = indexSegmentList;
  }

  public void setParents(List<JobVertex> parents) {
    this._parents = parents;
  }

  public void setSuccessors(List<JobVertex> successors) {
    this._successors = successors;
  }

  public void setIndexSegmentList(List<IndexSegment> indexSegmentList) {
    this._indexSegmentList = indexSegmentList;
  }

  public void addParent(JobVertex parent) {
    if (parent != null) {
      _parents.add(parent);
    }
  }

  public void addSuccessor(JobVertex successor) {
    if (successor != null) {
      _successors.add(successor);
    }
  }

  public boolean removeParent(JobVertex parent) {
    return _parents.remove(parent);
  }

  public boolean removeSuccessor(JobVertex successor) {
    return _successors.remove(successor);
  }

  public List<JobVertex> getParents() {
    return _parents;
  }

  public List<JobVertex> getSuccessors() {
    return _successors;
  }

  public List<IndexSegment> getIndexSegmentList() {
    return _indexSegmentList;
  }

  public Properties getMetadata() {
    return _metadata;
  }

  public void setMetadata(Properties metadata) {
    _metadata = metadata;
  }

}
