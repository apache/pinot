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

import com.linkedin.pinot.core.segment.creator.impl.V1Constants;
import java.util.Iterator;
import xerial.larray.buffer.LBufferAPI;


public class OffHeapStarTreeNode implements StarTreeNode {
  public static final int INVALID_INDEX = -1;

  // Number of fields and serializable size of the node
  public static final int NUM_SERIALIZABLE_FIELDS = 7;
  public static final int SERIALIZABLE_SIZE_IN_BYTES = V1Constants.Numbers.INTEGER_SIZE * NUM_SERIALIZABLE_FIELDS;

  private final LBufferAPI _dataBuffer;
  private final int _dimensionId;
  private final int _dimensionValue;
  private final int _startDocId;
  private final int _endDocId;
  private final int _aggregatedDocId;
  private final int _childrenStartIndex;
  // Inclusive
  private final int _childrenEndIndex;

  public OffHeapStarTreeNode(LBufferAPI dataBuffer, int nodeId) {
    _dataBuffer = dataBuffer;
    long offset = nodeId * SERIALIZABLE_SIZE_IN_BYTES;

    _dimensionId = dataBuffer.getInt(offset);
    offset += V1Constants.Numbers.INTEGER_SIZE;

    _dimensionValue = dataBuffer.getInt(offset);
    offset += V1Constants.Numbers.INTEGER_SIZE;

    _startDocId = dataBuffer.getInt(offset);
    offset += V1Constants.Numbers.INTEGER_SIZE;

    _endDocId = dataBuffer.getInt(offset);
    offset += V1Constants.Numbers.INTEGER_SIZE;

    _aggregatedDocId = dataBuffer.getInt(offset);
    offset += V1Constants.Numbers.INTEGER_SIZE;

    _childrenStartIndex = dataBuffer.getInt(offset);
    offset += V1Constants.Numbers.INTEGER_SIZE;

    _childrenEndIndex = dataBuffer.getInt(offset);
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
    if (_childrenStartIndex == INVALID_INDEX) {
      return INVALID_INDEX;
    } else {
      return _dataBuffer.getInt(_childrenStartIndex * SERIALIZABLE_SIZE_IN_BYTES);
    }
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
    if (_childrenStartIndex == INVALID_INDEX) {
      return 0;
    } else {
      return _childrenEndIndex - _childrenStartIndex + 1;
    }
  }

  @Override
  public boolean isLeaf() {
    return _childrenStartIndex == INVALID_INDEX;
  }

  @Override
  public StarTreeNode getChildForDimensionValue(int dimensionValue) {
    if (isLeaf()) {
      return null;
    }

    // Specialize star node for performance
    if (dimensionValue == ALL) {
      OffHeapStarTreeNode firstNode = new OffHeapStarTreeNode(_dataBuffer, _childrenStartIndex);
      if (firstNode.getDimensionValue() == ALL) {
        return firstNode;
      } else {
        return null;
      }
    }

    // Binary search
    int low = _childrenStartIndex;
    int high = _childrenEndIndex;

    while (low <= high) {
      int mid = (low + high) / 2;
      OffHeapStarTreeNode midNode = new OffHeapStarTreeNode(_dataBuffer, mid);
      int midValue = midNode.getDimensionValue();

      if (midValue == dimensionValue) {
        return midNode;
      } else if (midValue < dimensionValue) {
        low = mid + 1;
      } else {
        high = mid - 1;
      }
    }
    return null;
  }

  @Override
  public Iterator<OffHeapStarTreeNode> getChildrenIterator() {
    return new Iterator<OffHeapStarTreeNode>() {
      // Inclusive
      private final int _endChildId = _childrenEndIndex;
      private int _currentChildId = _childrenStartIndex;

      @Override
      public boolean hasNext() {
        return _currentChildId <= _endChildId;
      }

      @Override
      public OffHeapStarTreeNode next() {
        return new OffHeapStarTreeNode(_dataBuffer, _currentChildId++);
      }

      @Override
      public void remove() {
        throw new UnsupportedOperationException();
      }
    };
  }
}
