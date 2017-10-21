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


public class StarTreeIndexNodeOffHeap implements StarTreeIndexNodeInterf {
  public static final int INVALID_INDEX = -1;

  // Number of fields of this class that will be serialized.
  private static final int NUMBER_OF_SERIALIZABLE_FIELDS = 7;

  // Hard-coded the serializable size of StarTreeIndexNodeOffHeap object.
  private static final int NODE_SERIALIZABLE_SIZE = V1Constants.Numbers.INTEGER_SIZE * NUMBER_OF_SERIALIZABLE_FIELDS;

  private static final long DIMENSION_NAME_OFFSET = 0;
  private static final long DIMENSION_VALUE_OFFSET = DIMENSION_NAME_OFFSET + V1Constants.Numbers.INTEGER_SIZE;

  private static final long START_DOCUMENT_ID_OFFSET = DIMENSION_VALUE_OFFSET + V1Constants.Numbers.INTEGER_SIZE;
  private static final long END_DOCUMENT_ID_OFFSET = START_DOCUMENT_ID_OFFSET + V1Constants.Numbers.INTEGER_SIZE;
  private static final long AGGREGATED_DOCUMENT_ID_OFFSET = END_DOCUMENT_ID_OFFSET + V1Constants.Numbers.INTEGER_SIZE;

  private static final long CHILDREN_START_INDEX_OFFSET =
      AGGREGATED_DOCUMENT_ID_OFFSET + V1Constants.Numbers.INTEGER_SIZE;
  private static final long CHILDREN_END_INDEX_OFFSET = CHILDREN_START_INDEX_OFFSET + V1Constants.Numbers.INTEGER_SIZE;

  private final LBufferAPI dataBuffer;

  private int dimensionName;
  private int dimensionValue;
  private int startDocumentId;
  private int endDocumentId;
  private int aggregatedDocumentId;
  private int childrenStartIndex;
  private int childrenEndIndex;

  /**
   * Constructor for the class.
   * - Reads all fields from the data buffer.
   *
   * @param dataBuffer
   * @param nodeId
   */
  public StarTreeIndexNodeOffHeap(LBufferAPI dataBuffer, int nodeId) {
    this.dataBuffer = dataBuffer;
    long offset = nodeId * NODE_SERIALIZABLE_SIZE;

    dimensionName = dataBuffer.getInt(offset);
    offset += V1Constants.Numbers.INTEGER_SIZE;

    dimensionValue = dataBuffer.getInt(offset);
    offset += V1Constants.Numbers.INTEGER_SIZE;

    startDocumentId = dataBuffer.getInt(offset);
    offset += V1Constants.Numbers.INTEGER_SIZE;

    endDocumentId = dataBuffer.getInt(offset);
    offset += V1Constants.Numbers.INTEGER_SIZE;

    aggregatedDocumentId = dataBuffer.getInt(offset);
    offset += V1Constants.Numbers.INTEGER_SIZE;

    childrenStartIndex = dataBuffer.getInt(offset);
    offset += V1Constants.Numbers.INTEGER_SIZE;

    childrenEndIndex = dataBuffer.getInt(offset);
  }

  /**
   * {@inheritDoc}
   * @return
   */
  @Override
  public int getDimensionName() {
    return dimensionName;
  }

  /**
   * {@inheritDoc}
   * @param dimensionName
   */
  @Override
  public void setDimensionName(int dimensionName) {
    this.dimensionName = dimensionName;
  }

  /**
   * {@inheritDoc}
   * @return
   */
  @Override
  public int getDimensionValue() {
    return dimensionValue;
  }

  /**
   * {@inheritDoc}
   * @param dimensionValue
   */
  @Override
  public void setDimensionValue(int dimensionValue) {
    this.dimensionValue = dimensionValue;
  }

  /**
   * {@inheritDoc}
   * @return
   */
  @Override
  public int getStartDocumentId() {
    return startDocumentId;
  }

  /**
   * {@inheritDoc}
   * @param startDocumentId
   */
  @Override
  public void setStartDocumentId(int startDocumentId) {
    this.startDocumentId = startDocumentId;
  }

  /**
   * {@inheritDoc}
   * @return
   */
  @Override
  public int getEndDocumentId() {
    return endDocumentId;
  }

  /**
   * {@inheritDoc}
   * @param endDocumentId
   */
  @Override
  public void setEndDocumentId(int endDocumentId) {
    this.endDocumentId = endDocumentId;
  }

  /**
   * {@inheritDoc}
   * @return
   */
  @Override
  public int getAggregatedDocumentId() {
    return aggregatedDocumentId;
  }

  /**
   * {@inheritDoc}
   * @param aggregatedDocumentId
   */
  @Override
  public void setAggregatedDocumentId(int aggregatedDocumentId) {
    this.aggregatedDocumentId = aggregatedDocumentId;
  }

  /**
   * {@inheritDoc}
   * @return
   */
  @Override
  public int getNumChildren() {
    return (childrenStartIndex != INVALID_INDEX) ? (childrenEndIndex - childrenStartIndex + 1) : 0;
  }

  /**
   * {@inheritDoc}
   * @return
   */
  @Override
  public boolean isLeaf() {
    return (childrenStartIndex == INVALID_INDEX);
  }

  /**
   * {@inheritDoc}
   * Performs binary search over children (that are sorted by dimension value)
   * and returns child that matches the dimension value.
   * Returns NULL if no child was found for the specified value.
   *
   * @param dimensionValue
   * @return
   */
  @Override
  public StarTreeIndexNodeInterf getChildForDimensionValue(int dimensionValue) {
    if (isLeaf()) {
      return null;
    }

    // Specialize star node for performance
    if (dimensionValue == ALL) {
      StarTreeIndexNodeOffHeap firstNode = new StarTreeIndexNodeOffHeap(dataBuffer, childrenStartIndex);
      if (firstNode.getDimensionValue() == ALL) {
        return firstNode;
      } else {
        return null;
      }
    }

    int lo = childrenStartIndex;
    int hi = childrenEndIndex;

    while (lo <= hi) {
      int mid = lo + ((hi - lo) >>> 1);
      StarTreeIndexNodeOffHeap midNode = new StarTreeIndexNodeOffHeap(dataBuffer, mid);
      int midValue = midNode.getDimensionValue();

      if (midValue == dimensionValue) {
        return midNode;
      } else if (midValue < dimensionValue) {
        lo = mid + 1;
      } else {
        hi = mid - 1;
      }
    }
    return null;
  }

  /**
   * {@inheritDoc}
   * @return
   */
  @Override
  public Iterator<? extends StarTreeIndexNodeInterf> getChildrenIterator() {
    return new ChildIterator();
  }

  /**
   * {@inheritDoc}
   * @return
   */
  @Override
  public int getChildDimensionName() {
    long childDimensNameOffset = (childrenStartIndex * NODE_SERIALIZABLE_SIZE) + DIMENSION_NAME_OFFSET;
    return (!isLeaf()) ? dataBuffer.getInt(childDimensNameOffset) : INVALID_INDEX;
  }

  /**
   * Get the start index for children of this node.
   * @return
   */
  public int getChildrenStartIndex() {
    return childrenStartIndex;
  }

  /**
   * Set the start index for children of this node.
   * @param childrenStartIndex
   */
  public void setChildrenStartIndex(int childrenStartIndex) {
    this.childrenStartIndex = childrenStartIndex;
  }

  /**
   * Get the end index for children of this node.
   * @return
   */
  public int getChildrenEndIndex() {
    return childrenEndIndex;
  }

  /**
   * Get Set the end index for children of this node.
   * @param childrenEndIndex
   */
  public void setChildrenEndIndex(int childrenEndIndex) {
    this.childrenEndIndex = childrenEndIndex;
  }

  /**
   * Return the total size in bytes of fields that will be serialized.
   * @return
   */
  public static int getSerializableSize() {
    return NODE_SERIALIZABLE_SIZE;
  }

  /**
   * Iterator over children nodes.
   */
  private class ChildIterator implements Iterator<StarTreeIndexNodeOffHeap> {
    private int curChildId;
    private int endChildId;
    private StarTreeIndexNodeOffHeap child;

    public ChildIterator() {
      curChildId = getChildrenStartIndex();
      endChildId = getChildrenEndIndex();
    }

    @Override
    public boolean hasNext() {
      return ((curChildId != INVALID_INDEX) && (curChildId <= endChildId));
    }

    @Override
    public StarTreeIndexNodeOffHeap next() {
      StarTreeIndexNodeOffHeap child = new StarTreeIndexNodeOffHeap(dataBuffer, curChildId);
      curChildId++;
      return child;
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException("Operation 'remove' not supported in class " + getClass().getName());
    }
  }
}
