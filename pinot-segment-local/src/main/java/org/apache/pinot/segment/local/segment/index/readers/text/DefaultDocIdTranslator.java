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
package org.apache.pinot.segment.local.segment.index.readers.text;

import java.io.IOException;
import java.util.Arrays;
import org.apache.pinot.segment.spi.memory.PinotDataBuffer;
import org.apache.pinot.spi.query.QueryThreadContext;


/// Lucene docIDs are not same as pinot docIDs. The internal implementation
/// of Lucene can change the docIds and they are not guaranteed to be the
/// same as how we expect -- strictly increasing docIDs as the documents
/// are ingested during segment/index creation.
/// This class is used to map the luceneDocId (returned by the search query
/// to the collector) to corresponding pinotDocId.
class DefaultDocIdTranslator implements DocIdTranslator {
  final PinotDataBuffer _buffer;

  DefaultDocIdTranslator(PinotDataBuffer buffer) {
    _buffer = buffer;
  }

  @Override
  public int getPinotDocId(int luceneDocId) {
    return _buffer.getInt((long) luceneDocId * Integer.BYTES);
  }

  /// Returns the number of Lucene document ID entries backed by this mapping buffer.
  int size() {
    return (int) (_buffer.size() / Integer.BYTES);
  }

  /// Scans Lucene docIds in `[0, min(maxLuceneDoc, size()))` and records every `(luceneDocId, targetIdx)` pair
  /// whose mapped Pinot docId appears in the strictly ascending `sortedUniqueTargets[0 .. numTargets - 1]`.
  ///
  /// The recorded `luceneDocIdsOut` entries are naturally in strictly ascending Lucene docId order.
  /// Stops early once `numTargets` matches are found.
  ///
  /// @return number of matches written to `luceneDocIdsOut` and `targetIndicesOut`
  int findLuceneDocIds(int maxLuceneDoc, int[] sortedUniqueTargets, int numTargets, int[] luceneDocIdsOut,
      int[] targetIndicesOut) {
    if (numTargets <= 0) {
      return 0;
    }
    int limit = Math.min(maxLuceneDoc, size());
    int minTarget = sortedUniqueTargets[0];
    int maxTarget = sortedUniqueTargets[numTargets - 1];
    int found = 0;
    for (int luceneDocId = 0; luceneDocId < limit; luceneDocId++) {
      QueryThreadContext.checkTerminationAndSampleUsagePeriodically(luceneDocId, "DefaultDocIdTranslator");
      int pinotDocId = _buffer.getInt((long) luceneDocId * Integer.BYTES);
      if (pinotDocId < minTarget || pinotDocId > maxTarget) {
        continue;
      }
      int idx = numTargets == 1 ? 0 : Arrays.binarySearch(sortedUniqueTargets, 0, numTargets, pinotDocId);
      if (idx >= 0) {
        luceneDocIdsOut[found] = luceneDocId;
        targetIndicesOut[found] = idx;
        if (++found == numTargets) {
          break;
        }
      }
    }
    return found;
  }

  @Override
  public void close()
      throws IOException {
    _buffer.close();
  }
}
