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


/// Evaluates a scan-based predicate only on the document ids produced by an upstream iterator, one chunk at a time.
///
/// This is the streaming counterpart of [ScanBasedDocIdIterator#applyAnd]: the restriction still reaches the scan, so
/// the predicate is never evaluated outside the candidate set, but the result is produced lazily instead of being
/// materialized into a bitmap. A consumer that stops early -- a selection query that has filled its LIMIT -- only
/// pays for the chunks it actually pulled.
///
/// It also avoids the trap of driving a scan through [BlockDocIdIterator#advance]: `advance(target)` on a scan
/// iterator scans *forward from* the target until it finds a match, so it can run far past the candidate set.
public final class RestrictedScanDocIdIterator implements BlockDocIdIterator {
  private final BlockDocIdIterator _candidateDocIdIterator;
  private final ScanBasedDocIdIterator _scanDocIdIterator;
  private final int[] _buffer;

  private int _cursor;
  private int _limit;
  private boolean _candidatesExhausted;

  public RestrictedScanDocIdIterator(BlockDocIdIterator candidateDocIdIterator,
      ScanBasedDocIdIterator scanDocIdIterator, int chunkSize) {
    _candidateDocIdIterator = candidateDocIdIterator;
    _scanDocIdIterator = scanDocIdIterator;
    _buffer = new int[chunkSize];
  }

  @Override
  public int next() {
    while (true) {
      if (_cursor < _limit) {
        return _buffer[_cursor++];
      }
      if (_candidatesExhausted) {
        return Constants.EOF;
      }
      if (!fillNextChunk()) {
        return Constants.EOF;
      }
    }
  }

  @Override
  public int advance(int targetDocId) {
    // Skip over the buffered matches below the target before pulling anything new
    while (_cursor < _limit) {
      int docId = _buffer[_cursor++];
      if (docId >= targetDocId) {
        return docId;
      }
    }
    if (_candidatesExhausted) {
      return Constants.EOF;
    }
    // Move the candidate stream itself to the target, so the scan is never asked about documents below it
    int candidateDocId = _candidateDocIdIterator.advance(targetDocId);
    if (candidateDocId == Constants.EOF) {
      _candidatesExhausted = true;
      return Constants.EOF;
    }
    _cursor = 0;
    _limit = 0;
    _buffer[0] = candidateDocId;
    if (!fillNextChunk(1)) {
      return Constants.EOF;
    }
    return next();
  }

  private boolean fillNextChunk() {
    return fillNextChunk(0);
  }

  /// Pulls candidates until the buffer is full or the candidate stream ends, then evaluates the predicate on the
  /// whole chunk at once. Returns `false` when there is nothing left to produce.
  private boolean fillNextChunk(int numBuffered) {
    while (true) {
      int numCandidates = numBuffered;
      while (numCandidates < _buffer.length) {
        int docId = _candidateDocIdIterator.next();
        if (docId == Constants.EOF) {
          _candidatesExhausted = true;
          break;
        }
        _buffer[numCandidates++] = docId;
      }
      if (numCandidates == 0) {
        return false;
      }
      _cursor = 0;
      _limit = _scanDocIdIterator.matchDocIds(_buffer, numCandidates);
      if (_limit > 0) {
        return true;
      }
      // The whole chunk was filtered out; keep pulling rather than reporting EOF
      if (_candidatesExhausted) {
        return false;
      }
      numBuffered = 0;
    }
  }

  @Override
  public void close() {
    _candidateDocIdIterator.close();
    _scanDocIdIterator.close();
  }
}
