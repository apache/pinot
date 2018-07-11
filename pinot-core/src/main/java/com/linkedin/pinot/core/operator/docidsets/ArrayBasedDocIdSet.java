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
import com.linkedin.pinot.core.common.BlockDocIdSet;
import com.linkedin.pinot.core.operator.dociditerators.ArrayBasedDocIdIterator;


public final class ArrayBasedDocIdSet implements BlockDocIdSet {

  private final int[] _docIdArray;
  private final int _searchableLength;

  public ArrayBasedDocIdSet(int[] docIdArray, int searchableLength) {
    _docIdArray = docIdArray;
    _searchableLength = searchableLength;
  }

  @Override
  public BlockDocIdIterator iterator() {
    return new ArrayBasedDocIdIterator(_docIdArray, _searchableLength);
  }

  @Override
  public int[] getRaw() {
    return _docIdArray;
  }

  public int size() {
    return _searchableLength;
  }
}
