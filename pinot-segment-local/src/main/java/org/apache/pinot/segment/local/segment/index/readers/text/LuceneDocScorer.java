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

import it.unimi.dsi.fastutil.ints.Int2FloatMap;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.ReaderUtil;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.ScorerSupplier;
import org.apache.lucene.search.TwoPhaseIterator;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.Bits;
import org.apache.pinot.segment.spi.index.reader.TextIndexReader;
import org.apache.pinot.spi.exception.QueryException;
import org.apache.pinot.spi.query.QueryThreadContext;


/// Adaptive block-level [TextIndexReader.DocScorer] for immutable Lucene text indexes.
///
/// Rewrites the query and builds the Lucene [Weight] once on construction (preserving local and global-stats
/// validation semantics). When the segment uses the identity doc-id mapping ([NoOpDocIdTranslator]), ascending
/// blocks stream directly through Lucene's [Scorer] / [TwoPhaseIterator] with zero score-map heap allocation;
/// descending Block 1 (from `ReverseDocIdSetOperator`) reverse-scans into the output array, and out-of-order
/// Block 2+ or non-monotonic jumps backwards switch to eager collection.
///
/// When the segment uses a non-identity mapping ([DefaultDocIdTranslator]), selective single blocks either run a
/// filtered [LuceneDocScoreCollector] (when estimated matches `<= maxDoc / 4`) or reverse-scan the mapping buffer
/// to collect sorted Lucene docIds and advance the [Scorer] over only those `<= length` candidates (when estimated
/// matches `> maxDoc / 4`). Small match sets (`<= 1024`), full blocks, and multi-block scans fall back to eager
/// map collection so the index is scanned at most once.
///
/// Holds only pure-heap Lucene query/scorer state over an already-open [IndexSearcher] and zero closeable resources.
/// Not thread-safe: one instance serves one segment on a single thread.
class LuceneDocScorer implements TextIndexReader.DocScorer {
  /// Below this match estimate on a non-identity segment, eager map collection is tiny and avoids per-block
  /// translator scans.
  static final int NON_IDENTITY_EAGER_MATCH_THRESHOLD = 1024;
  /// Full block size used by Pinot's DocIdSetPlanNode (pinot-segment-local does not depend on pinot-core).
  static final int MAX_BLOCK_SIZE = 10_000;

  private final IndexSearcher _searcher;
  private final Query _rewrittenQuery;
  private final Weight _weight;
  private final DocIdTranslator _docIdTranslator;
  private final boolean _isIdentityMapping;
  private final List<LeafReaderContext> _leaves;
  private final int _maxDoc;
  private final long _estimatedMatches;
  private final boolean _noMatches;
  private final String _column;
  private final String _searchQuery;

  /// Initial per-leaf [ScorerSupplier] instances captured during cost estimation so Block 1 avoids computing them
  /// twice. Each entry is cleared once consumed.
  @Nullable
  private ScorerSupplier[] _initialSuppliers;

  private int _blocksScored = 0;

  // Forward per-leaf cursor state (used on the identity path and for single-block reverse-scan scoring)
  private int _leafIdx = -1;
  private int _curLeafDocBase = 0;
  private int _curLeafDocEnd = 0;
  @Nullable
  private Scorer _scorer;
  @Nullable
  private TwoPhaseIterator _twoPhase;
  @Nullable
  private DocIdSetIterator _approx;
  @Nullable
  private Bits _liveDocs;
  private int _curLocalDoc = -1;
  private long _currentLeadCost = Long.MAX_VALUE;
  private boolean _cursorStarted = false;
  private int _lastDocId = -1;
  private double _lastScore = 0.0;

  @Nullable
  private Int2FloatMap _eagerScores;

  LuceneDocScorer(IndexSearcher searcher, Query query, DocIdTranslator docIdTranslator, String column,
      String searchQuery)
      throws IOException {
    _searcher = searcher;
    _docIdTranslator = docIdTranslator;
    _isIdentityMapping = (docIdTranslator == NoOpDocIdTranslator.INSTANCE);
    _leaves = searcher.getIndexReader().leaves();
    _maxDoc = searcher.getIndexReader().maxDoc();
    _column = column;
    _searchQuery = searchQuery;

    _rewrittenQuery = searcher.rewrite(query);
    _weight = searcher.createWeight(_rewrittenQuery, ScoreMode.COMPLETE, 1f);

    int numLeaves = _leaves.size();
    ScorerSupplier[] initialSuppliers = numLeaves > 0 ? new ScorerSupplier[numLeaves] : null;
    long estimatedMatches = 0;
    boolean hasMatchingLeaf = false;
    for (int i = 0; i < numLeaves; i++) {
      ScorerSupplier supplier = _weight.scorerSupplier(_leaves.get(i));
      if (supplier != null) {
        hasMatchingLeaf = true;
        initialSuppliers[i] = supplier;
        long cost = supplier.cost();
        if (cost > 0) {
          estimatedMatches = Math.min(_maxDoc, estimatedMatches + Math.min(_maxDoc, cost));
        }
      }
    }
    _initialSuppliers = hasMatchingLeaf ? initialSuppliers : null;
    _estimatedMatches = hasMatchingLeaf ? estimatedMatches : 0;
    _noMatches = !hasMatchingLeaf;
  }

  @Override
  public void scoreDocs(int[] docIds, int length, double[] scores) {
    if (length <= 0) {
      return;
    }
    if (_noMatches) {
      Arrays.fill(scores, 0, length, 0.0);
      return;
    }
    if (_eagerScores != null) {
      scoreFromEagerMap(docIds, length, scores);
      return;
    }
    _blocksScored++;
    try {
      if (_isIdentityMapping) {
        scoreIdentityBlock(docIds, length, scores);
      } else {
        scoreNonIdentityBlock(docIds, length, scores);
      }
    } catch (QueryException e) {
      throw e;
    } catch (Exception e) {
      throw new RuntimeException(
          "Caught exception while scoring the text index for column:" + _column + " search query:" + _searchQuery, e);
    }
  }

  private void scoreIdentityBlock(int[] docIds, int length, double[] scores)
      throws IOException {
    _currentLeadCost = (_blocksScored == 1 && length < MAX_BLOCK_SIZE) ? length : Long.MAX_VALUE;

    if (isNonDecreasing(docIds, length)) {
      if (!_cursorStarted || docIds[0] >= _lastDocId) {
        scoreAscendingIdentity(docIds, length, scores);
        return;
      }
      // Monotonic ascending block that jumped backwards relative to the previous block: switch to eager.
      collectEagerScores();
      scoreFromEagerMap(docIds, length, scores);
      return;
    }

    if (_blocksScored == 1 && !_cursorStarted && isNonIncreasing(docIds, length)) {
      // ReverseDocIdSetOperator Block 1: reverse-scan into scores in ascending docId order with zero map heap.
      scoreDescendingIdentity(docIds, length, scores);
      return;
    }

    // Block 2+ descending or arbitrary unsorted block: switch to eager collection.
    collectEagerScores();
    scoreFromEagerMap(docIds, length, scores);
  }

  private void scoreAscendingIdentity(int[] docIds, int length, double[] scores)
      throws IOException {
    for (int i = 0; i < length; i++) {
      QueryThreadContext.checkTerminationAndSampleUsagePeriodically(i, "LuceneDocScorer");
      int target = docIds[i];
      if (_cursorStarted && target == _lastDocId) {
        scores[i] = _lastScore;
        continue;
      }
      double score = advanceAndScore(target);
      _cursorStarted = true;
      _lastDocId = target;
      _lastScore = score;
      scores[i] = score;
    }
  }

  private void scoreDescendingIdentity(int[] docIds, int length, double[] scores)
      throws IOException {
    int processed = 0;
    for (int i = length - 1; i >= 0; i--) {
      QueryThreadContext.checkTerminationAndSampleUsagePeriodically(processed++, "LuceneDocScorer");
      int target = docIds[i];
      if (_cursorStarted && target == _lastDocId) {
        scores[i] = _lastScore;
        continue;
      }
      double score = advanceAndScore(target);
      _cursorStarted = true;
      _lastDocId = target;
      _lastScore = score;
      scores[i] = score;
    }
  }

  private void scoreNonIdentityBlock(int[] docIds, int length, double[] scores)
      throws IOException {
    // Eager fallback for tiny match sets, full blocks, or multi-block queries so Lucene is scanned at most once.
    if (_estimatedMatches <= NON_IDENTITY_EAGER_MATCH_THRESHOLD || _blocksScored >= 2 || length >= MAX_BLOCK_SIZE) {
      collectEagerScores();
      scoreFromEagerMap(docIds, length, scores);
      return;
    }

    // Selective Block 1 (length < MAX_BLOCK_SIZE) with > 1024 estimated matches.
    if (_estimatedMatches <= _maxDoc / 4 || !(_docIdTranslator instanceof DefaultDocIdTranslator defaultTranslator)) {
      Int2FloatMap blockScores = new LuceneDocScoreCollector(_docIdTranslator::getPinotDocId,
          LuceneDocScoreCollector.allowedDocIdsPredicate(docIds, length)).collectScores(_searcher, _rewrittenQuery);
      for (int i = 0; i < length; i++) {
        scores[i] = blockScores.getOrDefault(docIds[i], 0f);
      }
      return;
    }

    scoreViaReverseTranslatorScan(defaultTranslator, docIds, length, scores);
  }

  private void scoreViaReverseTranslatorScan(DefaultDocIdTranslator translator, int[] docIds, int length,
      double[] scores)
      throws IOException {
    Arrays.fill(scores, 0, length, 0.0);
    boolean strictlyAscending = isStrictlyAscending(docIds, length);
    int[] sortedUniqueTargets;
    int numTargets;
    if (strictlyAscending) {
      sortedUniqueTargets = docIds;
      numTargets = length;
    } else {
      sortedUniqueTargets = Arrays.copyOf(docIds, length);
      Arrays.sort(sortedUniqueTargets);
      numTargets = deduplicateSorted(sortedUniqueTargets, length);
    }

    int[] matchedLuceneDocIds = new int[numTargets];
    int[] matchedTargetIndices = new int[numTargets];
    int found = translator.findLuceneDocIds(_maxDoc, sortedUniqueTargets, numTargets, matchedLuceneDocIds,
        matchedTargetIndices);
    if (found == 0) {
      return;
    }

    // matchedLuceneDocIds[0 .. found - 1] is strictly ascending; advance Scorer over only those <= length candidates.
    _currentLeadCost = found;
    double[] targetScores = strictlyAscending ? scores : new double[numTargets];
    for (int k = 0; k < found; k++) {
      QueryThreadContext.checkTerminationAndSampleUsagePeriodically(k, "LuceneDocScorer");
      targetScores[matchedTargetIndices[k]] = advanceAndScore(matchedLuceneDocIds[k]);
    }
    clearCursor();

    if (!strictlyAscending) {
      for (int i = 0; i < length; i++) {
        int uIdx = Arrays.binarySearch(sortedUniqueTargets, 0, numTargets, docIds[i]);
        scores[i] = uIdx >= 0 ? targetScores[uIdx] : 0.0;
      }
    }
  }

  private double advanceAndScore(int target)
      throws IOException {
    if (target < 0 || target >= _maxDoc) {
      return 0.0;
    }
    if (_leafIdx < 0 || target >= _curLeafDocEnd) {
      advanceToLeaf(target);
    }
    if (_approx == null) {
      return 0.0;
    }
    int localTarget = target - _curLeafDocBase;
    if (_curLocalDoc < localTarget) {
      _curLocalDoc = _approx.advance(localTarget);
    }
    if (_curLocalDoc != localTarget) {
      return 0.0;
    }
    if (_liveDocs != null && !_liveDocs.get(localTarget)) {
      return 0.0;
    }
    if (_twoPhase != null && !_twoPhase.matches()) {
      return 0.0;
    }
    return _scorer.score();
  }

  private void advanceToLeaf(int target)
      throws IOException {
    int idx;
    int nextIdx = _leafIdx + 1;
    if (nextIdx < _leaves.size()) {
      LeafReaderContext nextLeaf = _leaves.get(nextIdx);
      if (target >= nextLeaf.docBase && target < nextLeaf.docBase + nextLeaf.reader().maxDoc()) {
        idx = nextIdx;
      } else {
        idx = ReaderUtil.subIndex(target, _leaves);
      }
    } else {
      idx = ReaderUtil.subIndex(target, _leaves);
    }
    LeafReaderContext leaf = _leaves.get(idx);
    _leafIdx = idx;
    _curLeafDocBase = leaf.docBase;
    _curLeafDocEnd = leaf.docBase + leaf.reader().maxDoc();

    ScorerSupplier supplier = takeScorerSupplier(idx, leaf);
    if (supplier == null) {
      _scorer = null;
      _twoPhase = null;
      _approx = null;
      _liveDocs = null;
      _curLocalDoc = DocIdSetIterator.NO_MORE_DOCS;
      return;
    }
    _scorer = supplier.get(_currentLeadCost);
    if (_scorer == null) {
      _twoPhase = null;
      _approx = null;
      _liveDocs = null;
      _curLocalDoc = DocIdSetIterator.NO_MORE_DOCS;
      return;
    }
    _twoPhase = _scorer.twoPhaseIterator();
    _approx = _twoPhase != null ? _twoPhase.approximation() : _scorer.iterator();
    _liveDocs = leaf.reader().getLiveDocs();
    _curLocalDoc = -1;
  }

  @Nullable
  private ScorerSupplier takeScorerSupplier(int leafIdx, LeafReaderContext leaf)
      throws IOException {
    if (_initialSuppliers != null) {
      ScorerSupplier supplier = _initialSuppliers[leafIdx];
      _initialSuppliers[leafIdx] = null;
      return supplier;
    }
    return _weight.scorerSupplier(leaf);
  }

  private void collectEagerScores()
      throws IOException {
    if (_eagerScores == null) {
      _eagerScores = new LuceneDocScoreCollector(_docIdTranslator::getPinotDocId).collectScores(_searcher,
          _rewrittenQuery);
      clearCursor();
      _initialSuppliers = null;
    }
  }

  private void scoreFromEagerMap(int[] docIds, int length, double[] scores) {
    Int2FloatMap eagerScores = _eagerScores;
    for (int i = 0; i < length; i++) {
      scores[i] = eagerScores.getOrDefault(docIds[i], 0f);
    }
  }

  private void clearCursor() {
    _leafIdx = -1;
    _curLeafDocBase = 0;
    _curLeafDocEnd = 0;
    _scorer = null;
    _twoPhase = null;
    _approx = null;
    _liveDocs = null;
    _curLocalDoc = -1;
  }

  boolean isEagerFallbackActive() {
    return _eagerScores != null;
  }

  private static boolean isStrictlyAscending(int[] docIds, int length) {
    for (int i = 1; i < length; i++) {
      if (docIds[i] <= docIds[i - 1]) {
        return false;
      }
    }
    return true;
  }

  private static boolean isNonDecreasing(int[] docIds, int length) {
    for (int i = 1; i < length; i++) {
      if (docIds[i] < docIds[i - 1]) {
        return false;
      }
    }
    return true;
  }

  private static boolean isNonIncreasing(int[] docIds, int length) {
    for (int i = 1; i < length; i++) {
      if (docIds[i] > docIds[i - 1]) {
        return false;
      }
    }
    return true;
  }

  private static int deduplicateSorted(int[] sorted, int length) {
    if (length <= 1) {
      return length;
    }
    int write = 1;
    for (int read = 1; read < length; read++) {
      if (sorted[read] != sorted[write - 1]) {
        sorted[write++] = sorted[read];
      }
    }
    return write;
  }
}
