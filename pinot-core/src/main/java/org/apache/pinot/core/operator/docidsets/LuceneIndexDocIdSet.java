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
package org.apache.pinot.core.operator.docidsets;

import org.apache.pinot.core.common.BlockDocIdIterator;
import org.apache.pinot.core.operator.dociditerators.LuceneIndexScanDocIdIterator;
import org.apache.pinot.core.segment.index.readers.LuceneTextIndexReader;
import org.apache.pinot.core.segment.index.readers.TextIndexReader;

public class LuceneIndexDocIdSet implements FilterBlockDocIdSet {

  private int _startDocId;
  private int _endDocId;
  private final LuceneIndexScanDocIdIterator _luceneIndexScanDocIdIterator;

  public LuceneIndexDocIdSet(LuceneTextIndexReader.LuceneSearchResult luceneSearchResult, int startDocId, int endDocId, TextIndexReader textIndexReader) {
    _startDocId = startDocId;
    _endDocId = endDocId;
    _luceneIndexScanDocIdIterator = new LuceneIndexScanDocIdIterator(luceneSearchResult, textIndexReader);
    _luceneIndexScanDocIdIterator.setStartDocId(startDocId);
    _luceneIndexScanDocIdIterator.setEndDocId(endDocId);
  }

  @Override
  public int getMinDocId() {
    return _startDocId;
  }

  @Override
  public int getMaxDocId() {
    return _endDocId;
  }

  /**
   * After setting the startDocId, next calls will always return from &gt;=startDocId
   * @param startDocId
   */
  @Override
  public void setStartDocId(int startDocId) {
    _startDocId = startDocId;
    _luceneIndexScanDocIdIterator.setStartDocId(startDocId);
  }

  /**
   * After setting the endDocId, next call will return Constants.EOF after currentDocId exceeds endDocId
   * @param endDocId
   */
  @Override
  public void setEndDocId(int endDocId) {
    _endDocId = endDocId;
    _luceneIndexScanDocIdIterator.setEndDocId(endDocId);
  }

  @Override
  public long getNumEntriesScannedInFilter() {
    return 0L;
  }

  @Override
  public BlockDocIdIterator iterator() {
    return _luceneIndexScanDocIdIterator;
  }

  @Override
  public <T> T getRaw() {
    throw new UnsupportedOperationException("getRaw not supported for ScanBasedDocIdSet");
  }
}
