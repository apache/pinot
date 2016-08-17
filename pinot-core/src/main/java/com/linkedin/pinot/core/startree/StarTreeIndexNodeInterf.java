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
package com.linkedin.pinot.core.startree;

import java.util.Iterator;


/**
 * Interface for Star Tree Node.
 */
public interface StarTreeIndexNodeInterf {

  static final int ALL = -1;

  /**
   * Returns the index for the dimension for this node.
   * @return
   */
  int getDimensionName();

  /**
   * Set the id for dimension name.
   * @param dimension
   */
  void setDimensionName(int dimension);

  /**
   * Return the dictionary id for the dimension value.
   * @return
   */
  int getDimensionValue();

  /**
   * Get the dimension id for children.
   * @return
   */
  int getChildDimensionName();

  /**
   * Sets the dimension value (dictionary id) for the node.
   * @param dimensionValue
   */
  void setDimensionValue(int dimensionValue);

  /**
   * Returns the 'start' index in the raw documents for this node.
   * @return
   */
  int getStartDocumentId();

  /**
   * Sets the 'start' index in the raw documents for this node.
   * @param startDocumentId
   */
  void setStartDocumentId(int startDocumentId);

  /**
   * Returns the 'end' index in the raw documents for this node.
   * @return
   */
  int getEndDocumentId();

  /**
   * Sets the 'end' index in the raw documents for this node.
   * @param endDocumentId
   */
  void setEndDocumentId(int endDocumentId);

  /**
   * Returns the id for the aggregated document for this node.
   * @return
   */
  int getAggregatedDocumentId();

  /**
   * Sets the id for the aggregated document for this node.
   * @param aggregatedDocumentId
   */
  void setAggregatedDocumentId(int aggregatedDocumentId);

  /**
   * Returns the number of children for this node.
   * @return
   */
  int getNumChildren();

  /**
   * Returns true if the node is a leaf (ie has no children), false otherwise.
   * @return
   */
  boolean isLeaf();

  /**
   * Adds the provided node as a child of this node, for the given dimension value.
   * @param child
   * @param dimensionValue
   */
  void addChild(StarTreeIndexNodeInterf child, int dimensionValue);

  /**
   * Returns a child corresponding to the dimension value (dictionary id) for this node.
   * If no such child exists, returns null.
   * @param dimensionValue
   * @return
   */
  StarTreeIndexNodeInterf getChildForDimensionValue(int dimensionValue);

  /**
   * Returns iterator over children of this node.
   * @return
   */
  Iterator<? extends StarTreeIndexNodeInterf> getChildrenIterator();
}
