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
package org.apache.pinot.segment.spi.index.startree;

import java.util.Iterator;


/**
 * Interface for Star Tree Node.
 */
public interface StarTreeNode {

  int ALL = -1;

  /**
   * Get the index of the dimension.
   */
  int getDimensionId();

  /**
   * Get the value (dictionary id) of the dimension.
   */
  int getDimensionValue();

  /**
   * Get the child dimension id.
   */
  int getChildDimensionId();

  /**
   * Get the index of the start document.
   */
  int getStartDocId();

  /**
   * Get the index of the end document (exclusive).
   */
  int getEndDocId();

  /**
   * Get the index of the aggregated document.
   */
  int getAggregatedDocId();

  /**
   * Get the number of children nodes.
   */
  int getNumChildren();

  /**
   * Return true if the node is a leaf node, false otherwise.
   */
  boolean isLeaf();

  /**
   * Get the child node corresponding to the given dimension value (dictionary id), or null if such child does not
   * exist.
   */
  StarTreeNode getChildForDimensionValue(int dimensionValue);

  /**
   * Get the iterator over all children nodes.
   */
  Iterator<? extends StarTreeNode> getChildrenIterator();
}
