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
package org.apache.pinot.core.operator.dociditerators;

import org.apache.pinot.core.common.BlockDocIdIterator;
import org.apache.pinot.segment.spi.Constants;


/**
 * The {@code ArrayBasedDocIdIterator} is the iterator for ArrayBasedDocIdSet. It iterates on an array of matching
 * document ids.
 */
public final class ArrayBasedDocIdIterator implements BlockDocIdIterator {
  private final int[] _docIds;
  private final int _searchableLength;

  private int _nextIndex = 0;

  public ArrayBasedDocIdIterator(int[] docIds, int searchableLength) {
    _docIds = docIds;
    _searchableLength = searchableLength;
  }

  @Override
  public int next() {
    if (_nextIndex < _searchableLength) {
      return _docIds[_nextIndex++];
    } else {
      return Constants.EOF;
    }
  }

  @Override
  public int advance(int targetDocId) {
    throw new UnsupportedOperationException();
  }
}
