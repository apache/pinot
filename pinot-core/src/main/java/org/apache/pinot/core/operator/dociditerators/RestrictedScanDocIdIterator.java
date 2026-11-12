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

import java.util.List;
import org.apache.pinot.core.common.BlockDocIdIterator;
import org.apache.pinot.core.plan.DocIdSetPlanNode;
import org.apache.pinot.segment.spi.Constants;
import org.apache.pinot.spi.query.QueryThreadContext;
import org.roaringbitmap.PeekableIntIterator;


/// Evaluates scan-based predicates only on a set of candidate document ids, one chunk at a time.
///
/// This is the streaming counterpart of [ScanBasedDocIdIterator#applyAnd]: the restriction still reaches the scans, so
/// a predicate is never evaluated outside the candidate set, but the result is produced lazily instead of being
/// materialized into a bitmap. A consumer that stops early -- a selection query that has filled its LIMIT -- only
/// pays for the chunks it actually pulled.
///
/// The scans are applied to each chunk in order: the first scan sees the whole chunk, and each next scan only sees
/// the documents that the previous scans matched, as in the eager path. A chunk that no document survives is not
/// passed to the next scans.
///
/// The candidates are always a bitmap iterator. [#advance] can be called with a target behind the position of the
/// candidate stream, which only a bitmap iterator handles, and it peeks at the next candidate to know whether it skips
/// any. It never drives a scan with [BlockDocIdIterator#advance]: `advance(target)` on a scan iterator scans *forward
/// from* the target until it finds a match, so it can run far past the candidate set.
///
/// The chunk starts at [#MIN_CHUNK_SIZE] documents and doubles with every chunk up to
/// [DocIdSetPlanNode#MAX_DOC_PER_CALL], so a consumer that stops early pays for a small chunk and a consumer that
/// drains reaches the batch size of the eager path. An [#advance] that skips candidates resets the chunk to
/// [#MIN_CHUNK_SIZE]: after a skip, the consumer only needs the documents near the target.
///
/// The iterator closes the scans when it reaches the end of the candidates, like the scans do on their own. Not
/// thread-safe.
public final class RestrictedScanDocIdIterator implements BlockDocIdIterator {
  public static final int MIN_CHUNK_SIZE = BlockDocIdIterator.OPTIMAL_ITERATOR_BATCH_SIZE;
  public static final int MAX_CHUNK_SIZE = DocIdSetPlanNode.MAX_DOC_PER_CALL;

  private final PeekableIntIterator _candidateDocIdIterator;
  private final ScanBasedDocIdIterator[] _scanDocIdIterators;
  private final int[] _buffer;

  private int _chunkSize = MIN_CHUNK_SIZE;
  private int _cursor;
  private int _limit;
  private boolean _closed;

  /// @param candidateDocIdIterator the candidate document ids, in ascending order
  /// @param scanDocIdIterators the scans to apply to every chunk, in order. Not empty.
  public RestrictedScanDocIdIterator(PeekableIntIterator candidateDocIdIterator,
      List<ScanBasedDocIdIterator> scanDocIdIterators) {
    assert !scanDocIdIterators.isEmpty();
    _candidateDocIdIterator = candidateDocIdIterator;
    _scanDocIdIterators = scanDocIdIterators.toArray(new ScanBasedDocIdIterator[0]);
    _buffer = new int[MAX_CHUNK_SIZE];
  }

  @Override
  public int next() {
    while (_cursor == _limit) {
      if (!fillNextChunk()) {
        return Constants.EOF;
      }
    }
    return _buffer[_cursor++];
  }

  @Override
  public int advance(int targetDocId) {
    // Skip over the matches below the target that are already evaluated, before evaluating anything new
    while (_cursor < _limit) {
      int docId = _buffer[_cursor++];
      if (docId >= targetDocId) {
        return docId;
      }
    }
    // Move the candidate stream itself to the target, so that the scans are never asked about documents below it
    if (_candidateDocIdIterator.hasNext() && _candidateDocIdIterator.peekNext() < targetDocId) {
      _candidateDocIdIterator.advanceIfNeeded(targetDocId);
      _chunkSize = MIN_CHUNK_SIZE;
    }
    return next();
  }

  /// Evaluates the scans on the next chunk of candidates. Skips the chunks that nothing matches. Returns `false`, and
  /// closes the scans, when there is no candidate left.
  private boolean fillNextChunk() {
    while (true) {
      int numCandidates = 0;
      int chunkSize = _chunkSize;
      while (numCandidates < chunkSize && _candidateDocIdIterator.hasNext()) {
        _buffer[numCandidates++] = _candidateDocIdIterator.next();
      }
      if (numCandidates == 0) {
        close();
        return false;
      }
      _chunkSize = Math.min(chunkSize << 1, MAX_CHUNK_SIZE);
      int numMatchingDocIds = numCandidates;
      for (ScanBasedDocIdIterator scanDocIdIterator : _scanDocIdIterators) {
        numMatchingDocIds = scanDocIdIterator.matchDocIds(_buffer, numMatchingDocIds);
        if (numMatchingDocIds == 0) {
          break;
        }
      }
      // A run of chunks without any match can take long, so let the query be killed in between
      QueryThreadContext.checkTerminationAndSampleUsage("RestrictedScanDocIdIterator");
      if (numMatchingDocIds > 0) {
        _cursor = 0;
        _limit = numMatchingDocIds;
        return true;
      }
    }
  }

  /// Closes every scan, also when one of them fails to close. Idempotent.
  @Override
  public void close() {
    if (_closed) {
      return;
    }
    _closed = true;
    _cursor = 0;
    _limit = 0;
    RuntimeException exception = null;
    for (ScanBasedDocIdIterator scanDocIdIterator : _scanDocIdIterators) {
      try {
        scanDocIdIterator.close();
      } catch (RuntimeException e) {
        if (exception == null) {
          exception = e;
        } else {
          exception.addSuppressed(e);
        }
      }
    }
    if (exception != null) {
      throw exception;
    }
  }
}
