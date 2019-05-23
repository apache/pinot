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
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.pinot.thirdeye.cube.data.cube.CubeUtils;
import org.apache.pinot.thirdeye.cube.data.dbrow.DimensionValues;
import org.apache.pinot.thirdeye.cube.data.dbrow.Dimensions;
import org.apache.pinot.thirdeye.cube.data.dbrow.Row;


/**
 * Provides basic implementation for hierarchical cube nodes.
 *
 * @param <N> the class of the inherited cube node.
 * @param <R> the Row class of the inherited cube node.
 */
public abstract class BaseCubeNode<N extends BaseCubeNode, R extends Row> implements CubeNode<N> {
  protected int level;
  protected int index;
  protected double cost;
  protected R data;
  protected N parent;
  protected List<N> children = new ArrayList<>();

  /**
   * Constructs a CubeNode with the given data, i.e., a Row. The level, index, cost will be set to 0; parent will
   * be set to null.
   *
   * @param data the data of this cube node.
   */
  public BaseCubeNode(R data) {
    this.data = Preconditions.checkNotNull(data);
  }

  /**
   * Constructs a CubeNode with the given data and specified information.
   *
   * @param level the level of this node; level 0 the topmost level.
   * @param index the index of this node, which is used to speed up algorithm.
   * @param data the data of this cube node.
   * @param parent the parent of this cube node.
   */
  public BaseCubeNode(int level, int index, R data, N parent) {
    this(data);
    this.level = level;
    this.index = index;
    Preconditions.checkArgument((level != 0 && parent != null) || (level == 0 && parent == null));
    this.parent = parent;
    if (parent != null) { // non root node
      Dimensions parentDimension = new Dimensions(parent.getDimensions().namesToDepth(parent.getLevel()));
      Dimensions childDimension = new Dimensions(data.getDimensions().namesToDepth(level));
      Preconditions.checkState(parentDimension.isParentOf(childDimension),
          "Current node is not a child node of the given parent node. Current and parent dimensions: ",
          data.getDimensions(), parent.getDimensions());
      parent.children.add(this);
    }
  }

  @Override
  public int getLevel() {
    return level;
  }

  @Override
  public double getCost() {
    return cost;
  }

  @Override
  public void setCost(double cost) {
    this.cost = cost;
  }

  @Override
  @JsonIgnore
  public Dimensions getDimensions() {
    return data.getDimensions();
  }

  @Override
  @JsonIgnore
  public DimensionValues getDimensionValues() {
    return data.getDimensionValues();
  }

  @Override
  public N getParent() {
    return parent;
  }

  @Override
  public int childrenSize() {
    return children.size();
  }

  @Override
  public List<N> getChildren() {
    return Collections.unmodifiableList(children);
  }

  /**
   * Returns the change ratio of the node if it is a finite number; otherwise, returns an alternative ratio as follows:
   * 1. If originalChangeRatio is a finite number, return it;
   * 2. otherwise, get the ratio from its parent.
   * 3. If none is available, return 1.0.
   */
  @Override
  public double bootStrapChangeRatio() {
    double ratio = changeRatio();
    if (Double.isFinite(ratio) && Double.compare(ratio, 0d) != 0) {
      return ratio;
    } else {
      ratio = originalChangeRatio();
      if (Double.isFinite(ratio) && Double.compare(ratio, 0d) != 0) {
        return CubeUtils.ensureChangeRatioDirection(getBaselineValue(), getCurrentValue(), ratio);
      } else {
        if (parent != null) {
          return CubeUtils.ensureChangeRatioDirection(getBaselineValue(), getCurrentValue(), parent.bootStrapChangeRatio());
        } else {
          return 1.;
        }
      }
    }
  }

  @Override
  public boolean side() {
    double ratio = changeRatio();
    if (!Double.isNaN(ratio)) {
      return Double.compare(1., changeRatio()) <= 0;
    } else {
      return Double.compare(1., originalChangeRatio()) <= 0;
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof BaseCubeNode)) {
      return false;
    }
    BaseCubeNode<?, ?> that = (BaseCubeNode<?, ?>) o;
    return level == that.level && index == that.index && Double.compare(that.cost, cost) == 0 && Objects.equal(data,
        that.data);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(level, index, cost, data);
  }
}
