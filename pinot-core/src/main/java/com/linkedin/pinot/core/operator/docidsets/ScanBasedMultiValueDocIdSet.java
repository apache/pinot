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

import com.linkedin.pinot.core.common.BlockDocIdIterator;
import com.linkedin.pinot.core.common.BlockMetadata;
import com.linkedin.pinot.core.common.BlockValSet;
import com.linkedin.pinot.core.common.FilterBlockDocIdSet;
import com.linkedin.pinot.core.operator.dociditerators.MVScanDocIdIterator;
import com.linkedin.pinot.core.operator.filter.predicate.PredicateEvaluator;

import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;


public class ScanBasedMultiValueDocIdSet implements FilterBlockDocIdSet {
  private final BlockValSet blockValSet;
  private BlockMetadata blockMetadata;
  private MVScanDocIdIterator blockValSetBlockDocIdIterator;

  public ScanBasedMultiValueDocIdSet(BlockValSet blockValSet, BlockMetadata blockMetadata,
      PredicateEvaluator evaluator) {
    this.blockValSet = blockValSet;
    this.blockMetadata = blockMetadata;
    blockValSetBlockDocIdIterator = new MVScanDocIdIterator(blockValSet, blockMetadata, evaluator);
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
    blockValSetBlockDocIdIterator.setStartDocId(startDocId);
  }

  /**
   * After setting the endDocId, next call will return Constants.EOF after currentDocId exceeds endDocId
   * @param endDocId
   */
  @Override
  public void setEndDocId(int endDocId) {
    blockValSetBlockDocIdIterator.setEndDocId(endDocId);
  }

  @Override
  public BlockDocIdIterator iterator() {
    return blockValSetBlockDocIdIterator;
  }

  @Override
  public <T> T getRaw() {
    throw new UnsupportedOperationException("getRaw not supported for ScanBasedDocIdSet");
  }
}
