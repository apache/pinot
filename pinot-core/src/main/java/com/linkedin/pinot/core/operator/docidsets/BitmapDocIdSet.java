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
package com.linkedin.pinot.core.operator.docidsets;

import com.linkedin.pinot.core.common.BlockDocIdIterator;
import com.linkedin.pinot.core.common.BlockMetadata;
import com.linkedin.pinot.core.operator.dociditerators.BitmapDocIdIterator;
import java.util.Arrays;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.roaringbitmap.buffer.MutableRoaringBitmap;


public class BitmapDocIdSet implements FilterBlockDocIdSet {

  final private ImmutableRoaringBitmap[] bitmaps;

  BitmapDocIdIterator bitmapBasedBlockIdIterator;

  private int startDocId;

  private int endDocId;

  private ImmutableRoaringBitmap answer;

  /**
   *
   * @param datasourceName
   * @param blockMetadata
   * @param startDocId inclusive
   * @param endDocId inclusive
   * @param bitmaps
   */
  public BitmapDocIdSet(String datasourceName, BlockMetadata blockMetadata, int startDocId, int endDocId,
      ImmutableRoaringBitmap... bitmaps) {
    this(datasourceName, blockMetadata, startDocId, endDocId, bitmaps, false);
  }

  /**
   *
   * @param datasourceName
   * @param blockMetadata
   * @param startDocId inclusive
   * @param endDocId inclusive
   * @param bitmaps
   * @param exclusion
   */
  public BitmapDocIdSet(String datasourceName, BlockMetadata blockMetadata, int startDocId, int endDocId,
      ImmutableRoaringBitmap[] bitmaps, boolean exclusion) {
    this.bitmaps = bitmaps;
    setStartDocId(startDocId);
    setEndDocId(endDocId);
    // or() operation below can be expensive for large segment sizes
    // We avoid that for simple '=' queries
    if (bitmaps.length > 1 || exclusion) {
      MutableRoaringBitmap orBitmap = MutableRoaringBitmap.or(bitmaps);
      if (exclusion) {
        orBitmap.flip(startDocId, endDocId + 1); // end is exclusive
      }
      answer = orBitmap;
    } else if (bitmaps.length == 1) {
      answer = bitmaps[0];
    } else {
      answer = new MutableRoaringBitmap();
    }

    //by default bitmap is created for the all documents (raw docs + agg docs of star tree).
    //we need to consider only bits between start/end docId
    //startDocId/endDocId is decided by the filter plan node based on starTree vs raw data
    //TODO:check the performance penalty of removing this at runtime v/s <br/>
    //changing the bitmap index creation (i.e create two separate bitmaps for raw docs and materialized docs)
    //this should be a no-op when we don't have star tree
    if (blockMetadata.getStartDocId() != startDocId) {
      int start = Math.min(startDocId, blockMetadata.getStartDocId());
      int end = Math.max(startDocId, blockMetadata.getStartDocId());
      // TODO/atumbde: Removed to address [PINOT-2806]
      //answer.remove(start, end + 1);//end is exclusive
    }
    if (blockMetadata.getEndDocId() != endDocId) {
      int start = Math.min(endDocId, blockMetadata.getEndDocId());
      int end = Math.max(endDocId, blockMetadata.getEndDocId());
      // TODO/atumbde: Removed to address [PINOT-2806]
      //answer.remove(start, end + 1);//end is exclusive
    }
  }

  @Override
  public int getMinDocId() {
    return startDocId;
  }

  @Override
  public int getMaxDocId() {
    return endDocId;
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
  public long getNumEntriesScannedInFilter() {
    return 0L;
  }

  @Override
  public BlockDocIdIterator iterator() {
    bitmapBasedBlockIdIterator = new BitmapDocIdIterator(answer.getIntIterator());
    bitmapBasedBlockIdIterator.setStartDocId(startDocId);
    bitmapBasedBlockIdIterator.setEndDocId(endDocId);
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
