/**
 * Copyright (C) 2014-2015 LinkedIn Corp. (pinot-core@linkedin.com)
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
package com.linkedin.pinot.core.operator.docidsets;

import java.util.Arrays;

import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.roaringbitmap.buffer.MutableRoaringBitmap;

import com.linkedin.pinot.core.common.BlockDocIdIterator;
import com.linkedin.pinot.core.common.BlockMetadata;
import com.linkedin.pinot.core.common.FilterBlockDocIdSet;
import com.linkedin.pinot.core.operator.dociditerators.BitmapDocIdIterator;

public class BitmapDocIdSet implements FilterBlockDocIdSet {

  final private ImmutableRoaringBitmap[] bitmaps;

  private BlockMetadata blockMetadata;
  BitmapDocIdIterator bitmapBasedBlockIdIterator;

  private boolean exclusion;

  private int startDocId;

  private int endDocId;

  private MutableRoaringBitmap answer;

  public BitmapDocIdSet(String datasourceName, BlockMetadata blockMetadata,
      ImmutableRoaringBitmap... bitmaps) {
    this(datasourceName, blockMetadata, bitmaps, false);
  }

  public BitmapDocIdSet(String datasourceName, BlockMetadata blockMetadata,
      ImmutableRoaringBitmap[] bitmaps, boolean exclusion) {
    this.blockMetadata = blockMetadata;
    this.bitmaps = bitmaps;
    this.exclusion = exclusion;
    setStartDocId(blockMetadata.getStartDocId());
    setEndDocId(blockMetadata.getEndDocId());
    answer = MutableRoaringBitmap.or(bitmaps);
    if (exclusion) {
      answer.flip(startDocId, endDocId + 1); // end is exclusive
    }
  }

  @Override
  public int getMinDocId() {
    return blockMetadata.getStartDocId();
  }

  @Override
  public int getMaxDocId() {
    return blockMetadata.getEndDocId();
  }

  /**
   * After setting the startDocId, next calls will always return from &gt;=startDocId
   * @param startDocId
   */
  @Override
  public void setStartDocId(int startDocId) {
    this.startDocId = startDocId;
  }

  /**
   * After setting the endDocId, next call will return Constants.EOF after currentDocId exceeds
   * endDocId
   * @param endDocId
   */
  @Override
  public void setEndDocId(int endDocId) {
    this.endDocId = endDocId;
  }

  @Override
  public BlockDocIdIterator iterator() {
    bitmapBasedBlockIdIterator = new BitmapDocIdIterator(answer.getIntIterator());
    bitmapBasedBlockIdIterator.setStartDocId(blockMetadata.getStartDocId());
    bitmapBasedBlockIdIterator.setEndDocId(blockMetadata.getEndDocId());
    return bitmapBasedBlockIdIterator;
  }

  @SuppressWarnings("unchecked")
  @Override
  public <T extends Object> T getRaw() {
    return (T) answer;
  }

  @Override
  public String toString() {
    return Arrays.toString(bitmaps);
  }
}
