/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.query.planner;

import java.util.List;
import org.apache.pinot.query.planner.plannode.PlanNode;


/**
 * The {@code PlanFragment} is the logical sub query plan that should be scheduled together from the result of
 * {@link org.apache.pinot.query.planner.logical.PlanFragmenter}.
 *
 */
public class PlanFragment {

  private final int _fragmentId;
  private final PlanNode _fragmentRoot;
  private final PlanFragmentMetadata _fragmentMetadata;

  private final List<PlanFragment> _children;

  public PlanFragment(int fragmentId, PlanNode fragmentRoot, PlanFragmentMetadata fragmentMetadata,
      List<PlanFragment> children) {
    _fragmentId = fragmentId;
    _fragmentRoot = fragmentRoot;
    _fragmentMetadata = fragmentMetadata;
    _children = children;
  }

  public int getFragmentId() {
    return _fragmentId;
  }

  public PlanNode getFragmentRoot() {
    return _fragmentRoot;
  }

  public PlanFragmentMetadata getFragmentMetadata() {
    return _fragmentMetadata;
  }

  public List<PlanFragment> getChildren() {
    return _children;
  }
}
