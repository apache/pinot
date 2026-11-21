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

import com.google.common.base.Preconditions;
import java.util.Objects;
import org.apache.pinot.core.common.BlockDocIdIterator;
import org.apache.pinot.segment.spi.Constants;


/// Evaluates a scan only on doc ids pulled from an ascending candidate iterator, one bounded batch at a time.
/// Owns both input iterators and closes them on exhaustion, failure, or explicit close. Not thread-safe.
public final class RestrictedScanDocIdIterator implements BlockDocIdIterator {
  private final BlockDocIdIterator _candidateDocIdIterator;
  private final ScanBasedDocIdIterator _scanDocIdIterator;
  private final int[] _buffer;
  private final Runnable _afterBatch;

  private int _cursor;
  private int _limit;
  private int _lastCandidateDocId = -1;
  private boolean _candidatesExhausted;
  private boolean _closed;

  public RestrictedScanDocIdIterator(BlockDocIdIterator candidateDocIdIterator,
      ScanBasedDocIdIterator scanDocIdIterator, int chunkSize) {
    this(candidateDocIdIterator, scanDocIdIterator, chunkSize, () -> {
    });
  }

  /// Runs `afterBatch` after each evaluated batch, including batches with no matches.
  public RestrictedScanDocIdIterator(BlockDocIdIterator candidateDocIdIterator,
      ScanBasedDocIdIterator scanDocIdIterator, int chunkSize, Runnable afterBatch) {
    Preconditions.checkArgument(chunkSize > 0, "Chunk size must be positive");
    int scanBatchSize = scanDocIdIterator.getRestrictedScanBatchSize();
    Preconditions.checkArgument(scanBatchSize > 0, "Scan does not support restricted batches");
    _candidateDocIdIterator = candidateDocIdIterator;
    _scanDocIdIterator = scanDocIdIterator;
    _buffer = new int[Math.min(chunkSize, scanBatchSize)];
    _afterBatch = Objects.requireNonNull(afterBatch);
  }

  @Override
  public int next() {
    return _cursor < _limit ? _buffer[_cursor++] : pullNextMatch(-1);
  }

  @Override
  public int advance(int targetDocId) {
    while (_cursor < _limit) {
      int docId = _buffer[_cursor++];
      if (docId >= targetDocId) {
        return docId;
      }
    }
    return pullNextMatch(targetDocId);
  }

  private int pullNextMatch(int targetDocId) {
    if (_closed) {
      return Constants.EOF;
    }
    try {
      while (!_candidatesExhausted) {
        // Reading a batch can move the candidate iterator beyond the last match returned to our caller.
        int candidate = targetDocId > _lastCandidateDocId
            ? _candidateDocIdIterator.advance(targetDocId)
            : _candidateDocIdIterator.next();
        int numCandidates = 0;
        while (candidate != Constants.EOF) {
          _buffer[numCandidates++] = candidate;
          _lastCandidateDocId = candidate;
          if (numCandidates == _buffer.length) {
            break;
          }
          candidate = _candidateDocIdIterator.next();
        }
        _candidatesExhausted = candidate == Constants.EOF;
        if (numCandidates != 0) {
          _cursor = 0;
          _limit = _scanDocIdIterator.matchDocIds(_buffer, numCandidates);
          _afterBatch.run();
          if (_limit > 0) {
            return _buffer[_cursor++];
          }
        }
      }
      close();
      return Constants.EOF;
    } catch (RuntimeException | Error e) {
      try {
        close();
      } catch (RuntimeException | Error closeFailure) {
        if (closeFailure != e) {
          e.addSuppressed(closeFailure);
        }
      }
      throw e;
    }
  }

  public long getNumEntriesScanned() {
    return _scanDocIdIterator.getNumEntriesScanned();
  }

  @Override
  public void close() {
    if (!_closed) {
      _closed = true;
      _limit = 0;
      try (_scanDocIdIterator) {
        _candidateDocIdIterator.close();
      }
    }
  }
}
