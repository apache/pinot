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
import org.apache.pinot.segment.spi.memory.PinotDataBuffer;

import static org.apache.pinot.segment.local.startree.StarTreeBuilderUtils.INVALID_ID;


public class OffHeapStarTreeNode implements StarTreeNode {
  public static final int NUM_SERIALIZABLE_FIELDS = 7;
  public static final long SERIALIZABLE_SIZE_IN_BYTES = Integer.BYTES * NUM_SERIALIZABLE_FIELDS;
  private static final int DIMENSION_ID_OFFSET = 0;
  private static final int DIMENSION_VALUE_OFFSET = DIMENSION_ID_OFFSET + Integer.BYTES;
  private static final int START_DOC_ID_OFFSET = DIMENSION_VALUE_OFFSET + Integer.BYTES;
  private static final int END_DOC_ID_OFFSET = START_DOC_ID_OFFSET + Integer.BYTES;
  private static final int AGGREGATE_DOC_ID_OFFSET = END_DOC_ID_OFFSET + Integer.BYTES;
  private static final int FIRST_CHILD_ID_OFFSET = AGGREGATE_DOC_ID_OFFSET + Integer.BYTES;
  private static final int LAST_CHILD_ID_OFFSET = FIRST_CHILD_ID_OFFSET + Integer.BYTES;

  private final PinotDataBuffer _dataBuffer;
  private final int _nodeId;
  private final int _firstChildId;

  public OffHeapStarTreeNode(PinotDataBuffer dataBuffer, int nodeId) {
    _dataBuffer = dataBuffer;
    _nodeId = nodeId;
    _firstChildId = getInt(FIRST_CHILD_ID_OFFSET);
  }

  private int getInt(int fieldOffset) {
    return _dataBuffer.getInt(_nodeId * SERIALIZABLE_SIZE_IN_BYTES + fieldOffset);
  }

  @Override
  public int getDimensionId() {
    return getInt(DIMENSION_ID_OFFSET);
  }

  @Override
  public int getDimensionValue() {
    return getInt(DIMENSION_VALUE_OFFSET);
  }

  @Override
  public int getChildDimensionId() {
    if (_firstChildId == INVALID_ID) {
      return INVALID_ID;
    } else {
      return _dataBuffer.getInt(_firstChildId * SERIALIZABLE_SIZE_IN_BYTES);
    }
  }

  @Override
  public int getStartDocId() {
    return getInt(START_DOC_ID_OFFSET);
  }

  @Override
  public int getEndDocId() {
    return getInt(END_DOC_ID_OFFSET);
  }

  @Override
  public int getAggregatedDocId() {
    return getInt(AGGREGATE_DOC_ID_OFFSET);
  }

  @Override
  public int getNumChildren() {
    if (_firstChildId == INVALID_ID) {
      return 0;
    } else {
      return getInt(LAST_CHILD_ID_OFFSET) - _firstChildId + 1;
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
    if (dimensionValue == StarTreeNode.ALL) {
      OffHeapStarTreeNode firstNode = new OffHeapStarTreeNode(_dataBuffer, _firstChildId);
      if (firstNode.getDimensionValue() == StarTreeNode.ALL) {
        return firstNode;
      } else {
        return null;
      }
    }

    // Binary search
    int low = _firstChildId;
    int high = getInt(LAST_CHILD_ID_OFFSET);

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
      private final int _lastChildId = getInt(LAST_CHILD_ID_OFFSET);

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
