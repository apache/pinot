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
package org.apache.pinot.core.startree;

import java.util.Iterator;
import org.apache.pinot.core.segment.memory.PinotDataBuffer;
import org.apache.pinot.segment.spi.index.startree.StarTreeNode;

import static org.apache.pinot.core.startree.StarTreeBuilderUtils.INVALID_ID;


public class OffHeapStarTreeNode implements StarTreeNode {
  public static final int NUM_SERIALIZABLE_FIELDS = 7;
  public static final int SERIALIZABLE_SIZE_IN_BYTES = Integer.BYTES * NUM_SERIALIZABLE_FIELDS;

  private final PinotDataBuffer _dataBuffer;
  private final int _dimensionId;
  private final int _dimensionValue;
  private final int _startDocId;
  private final int _endDocId;
  private final int _aggregatedDocId;
  private final int _firstChildId;
  private final int _lastChildId;

  public OffHeapStarTreeNode(PinotDataBuffer dataBuffer, int nodeId) {
    _dataBuffer = dataBuffer;
    long offset = (long) nodeId * SERIALIZABLE_SIZE_IN_BYTES;

    _dimensionId = dataBuffer.getInt(offset);
    offset += Integer.BYTES;

    _dimensionValue = dataBuffer.getInt(offset);
    offset += Integer.BYTES;

    _startDocId = dataBuffer.getInt(offset);
    offset += Integer.BYTES;

    _endDocId = dataBuffer.getInt(offset);
    offset += Integer.BYTES;

    _aggregatedDocId = dataBuffer.getInt(offset);
    offset += Integer.BYTES;

    _firstChildId = dataBuffer.getInt(offset);
    offset += Integer.BYTES;

    _lastChildId = dataBuffer.getInt(offset);
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
    if (_firstChildId == INVALID_ID) {
      return INVALID_ID;
    } else {
      return _dataBuffer.getInt((long) _firstChildId * SERIALIZABLE_SIZE_IN_BYTES);
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
    if (_firstChildId == INVALID_ID) {
      return 0;
    } else {
      return _lastChildId - _firstChildId + 1;
    }
  }

  @Override
  public boolean isLeaf() {
    return _firstChildId == INVALID_ID;
  }

  @Override
  public StarTreeNode getChildForDimensionValue(int dimensionValue) {
    if (isLeaf()) {
      return null;
    }

    // Specialize star node for performance
    if (dimensionValue == ALL) {
      OffHeapStarTreeNode firstNode = new OffHeapStarTreeNode(_dataBuffer, _firstChildId);
      if (firstNode.getDimensionValue() == ALL) {
        return firstNode;
      } else {
        return null;
      }
    }

    // Binary search
    int low = _firstChildId;
    int high = _lastChildId;

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
      private int _currentChildId = _firstChildId;

      @Override
      public boolean hasNext() {
        return _currentChildId <= _lastChildId;
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
