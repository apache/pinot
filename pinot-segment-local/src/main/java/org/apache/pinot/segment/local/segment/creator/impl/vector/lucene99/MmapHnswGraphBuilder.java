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
package org.apache.pinot.segment.local.segment.creator.impl.vector.lucene99;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.SplittableRandom;
import javax.annotation.Nullable;
import org.apache.lucene.codecs.KnnVectorsWriter.MergedVectorValues;
import org.apache.lucene.codecs.hnsw.HnswGraphProvider;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.MergeState;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.hnsw.CloseableRandomVectorScorerSupplier;
import org.apache.lucene.util.hnsw.HnswGraph;
import org.apache.lucene.util.hnsw.HnswGraphBuilder;
import org.apache.lucene.util.hnsw.IncrementalHnswGraphMerger;
import org.apache.lucene.util.hnsw.NeighborArray;
import org.apache.lucene.util.hnsw.NeighborQueue;
import org.apache.lucene.util.hnsw.RandomVectorScorer;
import org.apache.lucene.util.hnsw.RandomVectorScorerSupplier;


/// Serial Lucene 9.12 HNSW merge algorithm using mapped neighbor rows. Reuses Lucene's selection
/// of a compatible source graph and mapping of its vector ordinals into the merged segment.
final class MmapHnswGraphBuilder extends IncrementalHnswGraphMerger {
  private static final int MIN_CACHED_VECTOR_DIMENSION = 256;

  private final MmapHnswGraph _graph;
  private final SplittableRandom _random = new SplittableRandom(HnswGraphBuilder.randSeed);
  private final double _levelMultiplier;
  private final FixedBitSet _visited;
  private final NeighborQueue _frontier;
  private final NeighborQueue _entryCandidates = new NeighborQueue(1, false);
  private final NeighborQueue _beamCandidates;

  static MmapHnswGraph build(Path scratchDir, FieldInfo fieldInfo, CloseableRandomVectorScorerSupplier scorerSupplier,
      MergeState mergeState, int maxConn, int beamWidth)
      throws IOException {
    int count = scorerSupplier.totalVectorCount();
    MmapHnswGraph graph = MmapHnswGraph.open(scratchDir, count, maxConn);
    try {
      new MmapHnswGraphBuilder(graph, fieldInfo, scorerSupplier, maxConn, beamWidth).build(mergeState, count);
      return graph;
    } catch (IOException | RuntimeException | Error e) {
      try {
        graph.close();
      } catch (IOException | RuntimeException closeFailure) {
        e.addSuppressed(closeFailure);
      }
      throw e;
    }
  }

  private MmapHnswGraphBuilder(MmapHnswGraph graph, FieldInfo fieldInfo,
      CloseableRandomVectorScorerSupplier scorerSupplier, int maxConn, int beamWidth) {
    super(fieldInfo, fieldInfo.getVectorDimension() >= MIN_CACHED_VECTOR_DIMENSION
        ? new ScoreCachingSupplier(scorerSupplier) : scorerSupplier, maxConn, beamWidth);
    _graph = graph;
    _levelMultiplier = maxConn == 1 ? 1 : 1 / Math.log(maxConn);
    _visited = new FixedBitSet(graph.maxNodeId() + 1);
    _frontier = new NeighborQueue(beamWidth, true);
    _beamCandidates = new NeighborQueue(beamWidth, false);
  }

  private void build(MergeState mergeState, int count)
      throws IOException {
    for (int i = 0; i < mergeState.liveDocs.length; i++) {
      if (MergedVectorValues.hasVectorValues(mergeState.fieldInfos[i], fieldInfo.name)) {
        addReader(mergeState.knnVectorsReaders[i], mergeState.docMaps[i], mergeState.liveDocs[i]);
      }
    }
    if (initReader != null) {
      DocIdSetIterator mergedValues = switch (fieldInfo.getVectorEncoding()) {
        case BYTE -> MergedVectorValues.mergeByteVectorValues(fieldInfo, mergeState);
        case FLOAT32 -> MergedVectorValues.mergeFloatVectorValues(fieldInfo, mergeState);
      };
      initialize(((HnswGraphProvider) initReader).getGraph(fieldInfo.name),
          getNewOrdMapping(mergedValues, new FixedBitSet(count)));
    }
    for (int node = 0; node < count; node++) {
      if (!_graph.contains(node)) {
        insert(node);
      }
    }
    connectComponents();
  }

  private void initialize(HnswGraph source, int[] ordinalMap)
      throws IOException {
    for (int level = source.numLevels() - 1; level >= 0; level--) {
      HnswGraph.NodesIterator nodes = source.getNodesOnLevel(level);
      while (nodes.hasNext()) {
        int oldNode = nodes.nextInt();
        int node = ordinalMap[oldNode];
        if (!_graph.contains(node)) {
          _graph.addNode(node, level);
        }
        if (_graph.entryNode() == -1) {
          _graph.setEntryNode(node, level);
        }
        MmapHnswNeighborList neighbors = _graph.neighbors(level, node);
        source.seek(level, oldNode);
        for (int oldNeighbor = source.nextNeighbor(); oldNeighbor != DocIdSetIterator.NO_MORE_DOCS;
            oldNeighbor = source.nextNeighbor()) {
          // Scores from the reused graph are computed lazily if this row needs pruning.
          neighbors.appendAndPrune(ordinalMap[oldNeighbor], Float.NaN, node, scorerSupplier);
        }
      }
    }
  }

  private void insert(int node)
      throws IOException {
    int nodeLevel = nextLevel();
    _graph.addNode(node, nodeLevel);
    if (_graph.entryNode() == -1) {
      _graph.setEntryNode(node, nodeLevel);
      return;
    }

    RandomVectorScorer scorer = scorerSupplier.scorer(node);
    int highestLevel = _graph.numLevels() - 1;
    int[] entryPoints = {_graph.entryNode()};
    for (int level = highestLevel; level > nodeLevel; level--) {
      searchLevel(scorer, level, entryPoints, _entryCandidates, 1, null);
      entryPoints[0] = _entryCandidates.pop();
    }
    NeighborArray[] candidates = new NeighborArray[Math.min(highestLevel, nodeLevel) + 1];
    for (int level = candidates.length - 1; level >= 0; level--) {
      searchLevel(scorer, level, entryPoints, _beamCandidates, beamWidth, null);
      entryPoints = _beamCandidates.nodes();
      NeighborArray row = new NeighborArray(_beamCandidates.size(), false);
      while (_beamCandidates.size() > 0) {
        float score = _beamCandidates.topScore();
        row.addInOrder(_beamCandidates.pop(), score);
      }
      candidates[level] = row;
    }
    for (int level = 0; level < candidates.length; level++) {
      linkNeighbors(level, node, candidates[level]);
    }
    if (nodeLevel > highestLevel) {
      _graph.setEntryNode(node, nodeLevel);
    }
  }

  private int nextLevel() {
    double sample;
    do {
      sample = _random.nextDouble();
    } while (sample == 0);
    return (int) (-Math.log(sample) * _levelMultiplier);
  }

  private void linkNeighbors(int level, int node, NeighborArray candidates)
      throws IOException {
    MmapHnswNeighborList neighbors = _graph.neighbors(level, node);
    int limit = _graph.neighborLimit(level);
    int[] nodeIds = candidates.nodes();
    float[] scores = candidates.scores();
    // Add outgoing edges first. A selected neighbor must be closer to this node than to any
    // neighbor already selected.
    int[] chosen = new int[Math.min(limit, candidates.size())];
    int chosenCount = 0;
    for (int i = candidates.size() - 1; i >= 0 && chosenCount < chosen.length; i--) {
      if (isDiverse(nodeIds[i], scores[i], neighbors)) {
        neighbors.appendInOrder(nodeIds[i], scores[i]);
        chosen[chosenCount++] = i;
      }
    }
    // Traverse the selected candidates in the original worst-to-best order for backlinks.
    for (int i = chosenCount - 1; i >= 0; i--) {
      int candidate = chosen[i];
      int neighbor = nodeIds[candidate];
      _graph.neighbors(level, neighbor).appendAndPrune(node, scores[candidate], neighbor, scorerSupplier);
    }
  }

  private boolean isDiverse(int candidate, float score, MmapHnswNeighborList neighbors)
      throws IOException {
    RandomVectorScorer scorer = scorerSupplier.scorer(candidate);
    for (int i = 0; i < neighbors.size(); i++) {
      if (scorer.score(neighbors.node(i)) >= score) {
        return false;
      }
    }
    return true;
  }

  private void searchLevel(RandomVectorScorer scorer, int level, int[] entryPoints, NeighborQueue results, int topK,
      @Nullable FixedBitSet accept)
      throws IOException {
    _frontier.clear();
    _visited.clear();
    results.clear();
    for (int entry : entryPoints) {
      if (!_visited.getAndSet(entry)) {
        float score = scorer.score(entry);
        _frontier.add(entry, score);
        if (accept == null || accept.get(entry)) {
          results.insertWithOverflow(entry, score);
        }
      }
    }
    float minimum = results.size() == topK ? results.topScore() : Float.NEGATIVE_INFINITY;
    while (_frontier.size() > 0) {
      if (_frontier.topScore() < minimum) {
        break;
      }
      MmapHnswNeighborList neighbors = _graph.neighbors(level, _frontier.pop());
      for (int i = 0; i < neighbors.size(); i++) {
        int neighbor = neighbors.node(i);
        if (!_visited.getAndSet(neighbor)) {
          float score = scorer.score(neighbor);
          if (score > minimum) {
            _frontier.add(neighbor, score);
            if ((accept == null || accept.get(neighbor)) && results.insertWithOverflow(neighbor, score)
                && results.size() == topK) {
              minimum = results.topScore();
            }
          }
        }
      }
    }
  }

  private void connectComponents()
      throws IOException {
    int[] stack = new int[_graph.size()];
    FixedBitSet connected = new FixedBitSet(_graph.size());
    FixedBitSet spare = new FixedBitSet(_graph.size());
    NeighborQueue nearest = new NeighborQueue(2, false);
    for (int level = 0; level < _graph.numLevels(); level++) {
      connected.clear();
      spare.clear();
      List<Component> components = findComponents(level, connected, spare, stack);
      Component largest = components.getFirst();
      for (Component component : components) {
        if (component.size() > largest.size()) {
          largest = component;
        }
      }
      if (components.size() == 1 || largest.entry() == DocIdSetIterator.NO_MORE_DOCS) {
        continue;
      }
      int[] entryPoints = {largest.entry()};
      for (Component component : components) {
        if (component != largest && component.entry() != DocIdSetIterator.NO_MORE_DOCS) {
          int entry = component.entry();
          RandomVectorScorer scorer = scorerSupplier.scorer(entry);
          searchLevel(scorer, 0, entryPoints, nearest, 2, spare);
          while (nearest.size() > 0) {
            float score = nearest.topScore();
            int candidate = nearest.pop();
            if (candidate != entry && spare.get(candidate)) {
              addBridge(level, candidate, entry, score, spare);
            }
          }
        }
      }
    }
  }

  private List<Component> findComponents(int level, FixedBitSet connected, FixedBitSet spare, int[] stack) {
    // Nodes in the next higher level are entry points for this level's search.
    HnswGraph.NodesIterator entries = level == _graph.numLevels() - 1
        ? new HnswGraph.ArrayNodesIterator(new int[]{_graph.entryNode()}, 1) : _graph.getNodesOnLevel(level + 1);
    int rootedSize = 0;
    while (entries.hasNext()) {
      rootedSize += markReachable(level, entries.nextInt(), connected, spare, stack);
    }
    List<Component> components = new ArrayList<>();
    components.add(new Component(spare.nextSetBit(0), rootedSize));
    HnswGraph.NodesIterator nodes = _graph.getNodesOnLevel(level);
    while (nodes.hasNext()) {
      int node = nodes.nextInt();
      if (!connected.get(node)) {
        components.add(new Component(node, markReachable(level, node, connected, spare, stack)));
      }
    }
    return components;
  }

  private int markReachable(int level, int entry, FixedBitSet connected, FixedBitSet spare, int[] stack) {
    if (connected.getAndSet(entry)) {
      return 0;
    }
    stack[0] = entry;
    int pending = 1;
    int count = 0;
    int limit = _graph.neighborLimit(level);
    while (pending > 0) {
      int node = stack[--pending];
      count++;
      MmapHnswNeighborList neighbors = _graph.neighbors(level, node);
      if (neighbors.size() < limit) {
        spare.set(node);
      }
      for (int i = 0; i < neighbors.size(); i++) {
        int neighbor = neighbors.node(i);
        if (!connected.getAndSet(neighbor)) {
          stack[pending++] = neighbor;
        }
      }
    }
    return count;
  }

  private void addBridge(int level, int from, int to, float score, FixedBitSet spare) {
    int limit = _graph.neighborLimit(level);
    MmapHnswNeighborList forward = _graph.neighbors(level, from);
    forward.append(to, score);
    if (forward.size() == limit) {
      spare.clear(from);
    }
    MmapHnswNeighborList reverse = _graph.neighbors(level, to);
    if (reverse.size() < limit) {
      reverse.append(from, score);
      if (reverse.size() == limit) {
        spare.clear(to);
      }
    }
  }

  private record Component(int entry, int size) {
  }

  /// A bounded cache local to one merge. Searching multiple levels, linking neighbors, and pruning
  /// backlinks can all compare the same pair of vectors. Lucene's similarity functions are symmetric.
  private static final class ScoreCachingSupplier implements RandomVectorScorerSupplier {
    private static final int CACHE_BITS = 13;

    private final RandomVectorScorerSupplier _delegate;
    private final long[] _pairs = new long[1 << CACHE_BITS];
    private final float[] _scores = new float[_pairs.length];

    ScoreCachingSupplier(RandomVectorScorerSupplier delegate) {
      _delegate = delegate;
    }

    @Override
    public RandomVectorScorer scorer(int ord)
        throws IOException {
      RandomVectorScorer scorer = _delegate.scorer(ord);
      return new RandomVectorScorer() {
        @Override
        public float score(int node)
            throws IOException {
          long pair = (((long) Math.min(ord, node) << Integer.SIZE) | Math.max(ord, node)) + 1;
          int slot = (int) ((pair * 0x9E3779B97F4A7C15L) >>> (Long.SIZE - CACHE_BITS));
          if (_pairs[slot] == pair) {
            return _scores[slot];
          }
          float score = scorer.score(node);
          _pairs[slot] = pair;
          _scores[slot] = score;
          return score;
        }

        @Override
        public int maxOrd() {
          return scorer.maxOrd();
        }

        @Override
        public int ordToDoc(int ord) {
          return scorer.ordToDoc(ord);
        }

        @Override
        public Bits getAcceptOrds(Bits acceptDocs) {
          return scorer.getAcceptOrds(acceptDocs);
        }
      };
    }

    @Override
    public RandomVectorScorerSupplier copy()
        throws IOException {
      return new ScoreCachingSupplier(_delegate.copy());
    }
  }
}
