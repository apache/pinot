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
public abstract class MatchAllDocIdIterator implements BlockDocIdIterator {
  protected final int _numDocs;

  public MatchAllDocIdIterator(int numDocs) {
    _numDocs = numDocs;
  }

  public static MatchAllDocIdIterator create(int numDocs, boolean ascending) {
    return ascending ? new Asc(numDocs) : new Desc(numDocs);
  }

  private static class Asc extends MatchAllDocIdIterator {
    protected int _nextDocId = 0;

    private Asc(int numDocs) {
      super(numDocs);
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

  private static class Desc extends MatchAllDocIdIterator {
    private int _lastDocId;

    private Desc(int numDocs) {
      super(numDocs);
      _lastDocId = numDocs;
    }

    @Override
    public int next() {
      if (_lastDocId > 0) {
        return --_lastDocId;
      } else {
        return Constants.EOF;
      }
    }

    @Override
    public int advance(int targetDocId) {
      if (_lastDocId <= targetDocId) {
        throw new IllegalArgumentException("Target docId: " + targetDocId + " must be smaller than last returned "
            + "docId: " + _lastDocId);
      }
      _lastDocId = targetDocId + 1;
      return next();
    }
  }
}
