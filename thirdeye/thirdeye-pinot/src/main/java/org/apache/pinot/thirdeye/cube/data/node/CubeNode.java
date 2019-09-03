/*
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

package org.apache.pinot.thirdeye.cube.data.node;

import com.fasterxml.jackson.annotation.JsonIgnore;
import java.util.List;
import org.apache.pinot.thirdeye.cube.data.dbrow.DimensionValues;
import org.apache.pinot.thirdeye.cube.data.dbrow.Dimensions;


/**
 * Defines the operations that are used by the cube algorithm.
 *
 * @param <N> the class of the actual cube node. For example, ratio cube algorithm will use RatioCubeNode.
 */
public interface CubeNode<N extends CubeNode> {

  /**
   * Returns the level of this node in the cube; level 0 the topmost level.
   *
   * @return the level of this node in the cube.
   */
  int getLevel();

  /**
   * Resets all values (e.g., baseline and current value) of this node.
   */
  void resetValues();

  /**
   * Updates all values when the child node is extracted from this node.
   *
   * @param node the child node to be extracted.
   */
  void removeNodeValues(N node);

  /**
   * Updates all values when an extracted child node is added back to this node.
   *
   * @param node the child node to be added back.
   */
  void addNodeValues(N node);

  /**
   * Returns the latest node size of baseline time period.
   *
   * @return the latest node size of baseline time period.
   */
  double getBaselineSize();

  /**
   * Returns the latest node size of current time period.
   *
   * @return the latest node size of current time period.
   */
  double getCurrentSize();

  /**
   * Returns the original node size of baseline time period.
   *
   * @return the original node size of baseline time period.
   */
  double getOriginalBaselineSize();

  /**
   * Returns the original node size of current time period.
   *
   * @return the original node size of current time period.
   */
  double getOriginalCurrentSize();

  /**
   * Returns the latest baseline value.
   *
   * @return the latest baseline value.
   */
  double getBaselineValue();

  /**
   * Returns the latest current value.
   *
   * @return the latest current value.
   */
  double getCurrentValue();

  /**
   * Returns the original baseline value.
   *
   * @return the original baseline value.
   */
  @JsonIgnore
  double getOriginalBaselineValue();

  /**
   * Return the original current value.
   *
   * @return the original current value.
   */
  @JsonIgnore
  double getOriginalCurrentValue();

  /**
   * Returns the latest cost of this node.
   *
   * @return the latest cost of this node.
   */
  double getCost();

  /**
   * Sets the latest cost of this node.
   *
   * @param cost the latest cost of this node.
   */
  void setCost(double cost);

  /**
   * Returns the dimension names of this node, e.g., ["country", "page key"]
   *
   * @return the dimension names of this node.
   */
  @JsonIgnore
  Dimensions getDimensions();

  /**
   * Returns the dimension values of this node, e.g., ["US", "linkedin.com"]
   *
   * @return the dimension values of this node.
   */
  @JsonIgnore
  DimensionValues getDimensionValues();

  /**
   * Returns the parent of this node.
   *
   * @return the parent of this node.
   */
  N getParent();

  /**
   * Returns the number of children of this node.
   *
   * @return the number of children of this node.
   */
  int childrenSize();

  /**
   * Returns the children list of this node.
   *
   * @return the children list of this node.
   */
  List<N> getChildren();

  /**
   * Returns the changeRatio that is calculated by the aggregate current and aggregate baseline values of all children node.
   *
   * @return aggregated current value of all children / aggregated baseline value of all children;
   */
  double originalChangeRatio();

  /**
   * Returns the changeRatio that is calculated by currentValue and baselineValue.
   * @return currentValue / baselineValue;
   */
  double changeRatio();

  /**
   * Returns the change ratio of the node if it is a finite number; otherwise, provide an alternative change ratio.
   * @see BaseCubeNode for the basic implementation.
   */
  double bootStrapChangeRatio();

  /**
   * Returns the current changeRatio of this node is increased or decreased, i.e., returns true if changeRatio of the node >= 1.0.
   * If the current changeRatio is NAN, then the changeRatio of the aggregated values is used.
   *
   * Precondition: the aggregated baseline and current values cannot both be zero.
   */
  boolean side();

  /**
   * Returns if the data of current node equals to the data of other node. The parent and children nodes are not
   * compared due to the cyclic references between parent and children nodes.
   *
   * @param o the other node.
   *
   * @return true if the data of current node equals to the data of other node.
   */
  @Override
  boolean equals(Object o);

  /**
   * Returns the hash code that is generated base on the data of this node.
   *
   * @return the hash code that is generated base on the data of this node.
   */
  @Override
  int hashCode();
}
