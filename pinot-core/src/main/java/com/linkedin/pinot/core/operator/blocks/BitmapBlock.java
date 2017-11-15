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
package com.linkedin.pinot.core.operator.blocks;

import com.linkedin.pinot.core.common.BlockDocIdValueSet;
import com.linkedin.pinot.core.common.BlockMetadata;
import com.linkedin.pinot.core.common.BlockValSet;
import com.linkedin.pinot.core.operator.docidsets.BitmapDocIdSet;
import com.linkedin.pinot.core.operator.docidsets.FilterBlockDocIdSet;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;

public class BitmapBlock extends BaseFilterBlock {

  private final ImmutableRoaringBitmap[] bitmaps;
  BitmapDocIdSet bitmapDocIdSet;
  private BlockMetadata blockMetadata;
  private boolean exclusion;
  private String datasourceName;
  private int startDocId;
  private int endDocId;

  public BitmapBlock(String datasourceName, BlockMetadata blockMetadata, int startDocId, int endDocId, ImmutableRoaringBitmap[] bitmaps) {
    this(datasourceName, blockMetadata, startDocId, endDocId, bitmaps, false);
  }

  public BitmapBlock(String datasourceName, BlockMetadata blockMetadata, int startDocId, int endDocId, ImmutableRoaringBitmap[] bitmaps, boolean exclusion) {
    this.datasourceName = datasourceName;
    this.blockMetadata = blockMetadata;
    this.startDocId = startDocId;
    this.endDocId = endDocId;
    this.bitmaps = bitmaps;
    this.exclusion = exclusion;
  }

  @Override
  public FilterBlockDocIdSet getFilteredBlockDocIdSet() {
    bitmapDocIdSet = new BitmapDocIdSet(datasourceName, blockMetadata, startDocId, endDocId, bitmaps, exclusion);
    return bitmapDocIdSet;
  }

  @Override
  public BlockValSet getBlockValueSet() {
    throw new UnsupportedOperationException("getBlockValueSet not supported in " + this.getClass());
  }

  @Override
  public BlockDocIdValueSet getBlockDocIdValueSet() {
    throw new UnsupportedOperationException("getBlockDocIdValueSet not supported in " + this.getClass());
  }

  @Override
  public BlockMetadata getMetadata() {
    throw new UnsupportedOperationException("getMetadata not supported in " + this.getClass());
  }

}