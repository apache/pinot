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

package org.apache.pinot.thirdeye.cube.data.dbrow;

import org.apache.pinot.thirdeye.cube.data.node.CubeNode;


public interface Row {

  /**
   * Returns the dimension names of this row, such as ["country", "page key"].
   *
   * @return the dimension names of this row.
   */
  Dimensions getDimensions();

  /**
   * Sets dimension names of this row, such as ["country", "page key"].
   *
   * @param dimensions the dimension names for this row.
   */
  void setDimensions(Dimensions dimensions);

  /**
   * Returns dimension values of this row, such as ["US", "linkedin.com"]
   *
   * @return dimension values of this row, such as ["US", "linkedin.com"]
   */
  DimensionValues getDimensionValues();

  /**
   * Sets dimension values of this row, such as ["US", "linkedin.com"]
   *
   * @param dimensionValues the dimension values for this row.
   */
  void setDimensionValues(DimensionValues dimensionValues);

  /**
   * Converts current row to a CubeNode.
   *
   * @return a CubeNode of this row.
   */
  CubeNode toNode();

  /**
   * Converts current row to a CubeNode.
   *
   * @param level the level of this node; 0 is the top level.
   * @param index the index of this node, which is used for speeding up algorithm speed.
   * @param parent the parent of this node.
   *
   * @return a CubeNode of this row.
   */
  CubeNode toNode(int level, int index, CubeNode parent);

  @Override
  boolean equals(Object o);

  @Override
  int hashCode();
}
