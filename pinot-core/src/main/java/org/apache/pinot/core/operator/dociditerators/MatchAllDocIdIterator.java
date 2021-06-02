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
 * The {@code MatchAllDocIdIterator} is the iterator for MatchAllDocIdSet where all documents are matching.
 */
public final class MatchAllDocIdIterator implements BlockDocIdIterator {
  private final int _numDocs;

  private int _nextDocId = 0;

  public MatchAllDocIdIterator(int numDocs) {
    _numDocs = numDocs;
  }

  @Override
  public int next() {
    if (_nextDocId < _numDocs) {
      return _nextDocId++;
    } else {
      return Constants.EOF;
    }
  }

  @Override
  public int advance(int targetDocId) {
    _nextDocId = targetDocId;
    return next();
  }
}
