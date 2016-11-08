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

import com.linkedin.pinot.core.common.BlockMetadata;
import com.linkedin.pinot.core.common.BlockValSet;
import com.linkedin.pinot.core.operator.dociditerators.SVScanDocIdIterator;
import com.linkedin.pinot.core.operator.dociditerators.ScanBasedDocIdIterator;
import com.linkedin.pinot.core.operator.filter.predicate.PredicateEvaluator;


public class ScanBasedSingleValueDocIdSet implements FilterBlockDocIdSet {
  private final BlockValSet blockValSet;
  private SVScanDocIdIterator blockValSetBlockDocIdIterator;
  private String datasourceName;
  int startDocId;
  int endDocId;

  public ScanBasedSingleValueDocIdSet(String datasourceName, BlockValSet blockValSet, BlockMetadata blockMetadata, PredicateEvaluator evaluator) {
    this.datasourceName = datasourceName;
    this.blockValSet = blockValSet;
    blockValSetBlockDocIdIterator = new SVScanDocIdIterator(datasourceName, blockValSet, blockMetadata, evaluator);
    setStartDocId(blockMetadata.getStartDocId());
    setEndDocId(blockMetadata.getEndDocId());
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
    this.startDocId= startDocId;
    blockValSetBlockDocIdIterator.setStartDocId(startDocId);
  }

  /**
   * After setting the endDocId, next call will return Constants.EOF after currentDocId exceeds endDocId
   * @param endDocId
   */
  @Override
  public void setEndDocId(int endDocId) {
    this.endDocId = endDocId;
    blockValSetBlockDocIdIterator.setEndDocId(endDocId);
  }

  @Override
  public long getNumEntriesScannedInFilter() {
    return blockValSetBlockDocIdIterator.getNumEntriesScanned();
  }

  @Override
  public ScanBasedDocIdIterator iterator() {
    return blockValSetBlockDocIdIterator;
  }

  @Override
  public <T> T getRaw() {
    throw new UnsupportedOperationException("getRaw not supported for ScanBasedDocIdSet");
  }

}
