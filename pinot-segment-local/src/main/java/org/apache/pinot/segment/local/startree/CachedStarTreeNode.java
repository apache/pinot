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
package org.apache.pinot.segment.local.startree;

import java.util.Iterator;
import org.apache.pinot.segment.spi.index.startree.StarTreeNode;


/**
 * Getting all fields in current StarTree node immediately and cache them, instead of jumping back and forth inside the
 * data buffer to read fields from parent and children nodes while doing BFS over the StarTree index tree, making the
 * buffer reads a lot more sequential.
 */
public class CachedStarTreeNode implements StarTreeNode {
  private final StarTreeNode _delegate;
  private final int _dimensionId;
  private final int _dimensionValue;
  private final int _startDocId;
  private final int _endDocId;
  private final int _aggregatedDocId;
  private final int _numChildren;

  public CachedStarTreeNode(StarTreeNode starTreeNode) {
    _delegate = starTreeNode;
    _dimensionId = _delegate.getDimensionId();
    _dimensionValue = _delegate.getDimensionValue();
    _startDocId = _delegate.getStartDocId();
    _endDocId = _delegate.getEndDocId();
    _aggregatedDocId = _delegate.getAggregatedDocId();
    _numChildren = _delegate.getNumChildren();
  }

  @Override
  public int getDimensionId() {
    return _dimensionId;
  }

  @Override
  public int getDimensionValue() {
    return _dimensionValue;
  }

  @Override
  public int getChildDimensionId() {
    return _delegate.getChildDimensionId();
  }

  @Override
  public int getStartDocId() {
    return _startDocId;
  }

  @Override
  public int getEndDocId() {
    return _endDocId;
  }

  @Override
  public int getAggregatedDocId() {
    return _aggregatedDocId;
  }

  @Override
  public int getNumChildren() {
    return _numChildren;
  }

  @Override
  public boolean isLeaf() {
    return _delegate.isLeaf();
  }

  @Override
  public StarTreeNode getChildForDimensionValue(int dimensionValue) {
    return new CachedStarTreeNode(_delegate.getChildForDimensionValue(dimensionValue));
  }

  @Override
  public Iterator<? extends StarTreeNode> getChildrenIterator() {
    Iterator<? extends StarTreeNode> delegateIterator = _delegate.getChildrenIterator();
    return new Iterator<>() {
      @Override
      public boolean hasNext() {
        return delegateIterator.hasNext();
      }

      @Override
      public CachedStarTreeNode next() {
        return new CachedStarTreeNode(delegateIterator.next());
      }

      @Override
      public void remove() {
        delegateIterator.remove();
      }
    };
  }
}
