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
