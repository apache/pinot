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
 * The {@code AndDocIdIterator} is the iterator for AndDocIdSet to perform AND on all child BlockDocIdIterators.
 * <p>It keeps calling {@link BlockDocIdIterator#advance(int)} to gather the common matching document ids from all child
 * BlockDocIdIterators until one of them hits the end.
 */
public class AndDocIdIterator implements BlockDocIdIterator {
  public final BlockDocIdIterator[] _docIdIterators;

  protected int _nextDocId;

  private AndDocIdIterator(BlockDocIdIterator[] docIdIterators) {
    _docIdIterators = docIdIterators;
  }

  public static AndDocIdIterator create(BlockDocIdIterator[] docIdIterators, boolean asc) {
    return asc ? new Asc(docIdIterators) : new Desc(docIdIterators);
  }

  @Override
  public int advance(int targetDocId) {
    _nextDocId = targetDocId;
    return next();
  }

  @Override
  public void close() {
    for (BlockDocIdIterator it : _docIdIterators) {
      it.close();
    }
  }

  /// Starting from [#_nextDocId], returns the next common matching document id from all child BlockDocIdIterators or
  /// Constants.EOF if there is no more common matching document id.
  ///
  /// Although the state of [#_nextDocId] is not updated, iterators will be advanced, so this method have side effects.
  protected int findNextCommonMatch() {
    // CandidateId is the next did to try.
    // It is initialized to the current _nextDocId and updated using advance() calls on the child iterators.
    int candidateId = _nextDocId;
    // The index in _docIdIterators that returned the candidateId from advance() call.
    // Initialized to -1 to indicate that no iterator has returned the candidateId yet.
    int candidateIdIndex = -1;
    int numDocIdIterators = _docIdIterators.length;
    // The index of the next iterator to test. It can be reset to 0 when a new candidateId is found.
    int index = 0;
    while (index < numDocIdIterators) {
      if (index == candidateIdIndex) {
        // Skip the index with the max document id
        index++;
        continue;
      }
      // docId is the next docId from the current iterator that matches
      int docId = _docIdIterators[index].advance(candidateId);
      if (docId == Constants.EOF) { // this iterator is exhausted. Given we are an and we know there are no more matches
        close();
        return Constants.EOF;
      }
      if (docId == candidateId) { // The current iterator contains the candidateId, move to the next iterator
        index++;
      } else {
        // The current iterator does not contain the candidateId, update candidateId
        // and reset index to advance other iterators
        candidateId = docId;
        candidateIdIndex = index;
        index = 0;
      }
    }
    return candidateId;
  }

  public static class Asc extends AndDocIdIterator {
    public Asc(BlockDocIdIterator[] docIdIterators) {
      super(docIdIterators);
      _nextDocId = 0;
    }

    @Override
    public int next() {
      _nextDocId = findNextCommonMatch();
      return _nextDocId++;
    }
  }

  public static class Desc extends AndDocIdIterator {
    public Desc(BlockDocIdIterator[] docIdIterators) {
      super(docIdIterators);
      _nextDocId = Integer.MAX_VALUE;
    }
  }

  @Override
  public int next() {
    _nextDocId = findNextCommonMatch();
    return _nextDocId--;
  }
}
