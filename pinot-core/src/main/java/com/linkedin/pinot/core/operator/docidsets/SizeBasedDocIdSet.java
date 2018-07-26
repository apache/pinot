/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
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
import com.linkedin.pinot.core.operator.dociditerators.SizeBasedDocIdIterator;


public final class SizeBasedDocIdSet implements FilterBlockDocIdSet {
  private final int _maxDocId;

  public SizeBasedDocIdSet(int maxDocId) {
    _maxDocId = maxDocId;
  }

  @Override
  public int getMinDocId() {
    return 0;
  }

  @Override
  public int getMaxDocId() {
    return _maxDocId;
  }

  @Override
  public void setStartDocId(int startDocId) {
  }

  @Override
  public void setEndDocId(int endDocId) {
  }

  @Override
  public long getNumEntriesScannedInFilter() {
    return 0L;
  }

  @Override
  public BlockDocIdIterator iterator() {
    return new SizeBasedDocIdIterator(_maxDocId);
  }

  @Override
  public <T> T getRaw() {
    throw new UnsupportedOperationException();
  }
}
