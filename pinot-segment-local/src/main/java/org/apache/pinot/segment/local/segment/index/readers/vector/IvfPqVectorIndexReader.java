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
package org.apache.pinot.segment.local.segment.index.readers.vector;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteOrder;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.HashMap;
import java.util.Map;
import java.util.PriorityQueue;
import javax.annotation.Nullable;
import org.apache.pinot.segment.local.segment.creator.impl.vector.pq.IvfPqIndexFormat;
import org.apache.pinot.segment.local.segment.creator.impl.vector.pq.IvfPqVectorIndexCreator;
import org.apache.pinot.segment.local.segment.creator.impl.vector.pq.KMeans;
import org.apache.pinot.segment.local.segment.creator.impl.vector.pq.ProductQuantizer;
import org.apache.pinot.segment.local.segment.creator.impl.vector.pq.VectorDistanceUtil;
import org.apache.pinot.segment.spi.index.reader.VectorIndexReader;
import org.apache.pinot.segment.spi.store.SegmentDirectoryPaths;
import org.apache.pinot.spi.utils.CommonConstants.Broker.Request.QueryOptionKey;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.roaringbitmap.buffer.MutableRoaringBitmap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Reader for IVF_PQ vector index. Loads the index file into memory-mapped buffers
 * and performs approximate nearest-neighbor search using IVF + Product Quantization.
 *
 * <p>Supports runtime query options:
 * <ul>
 *   <li>{@code vectorNprobe} - number of coarse centroids to probe (overrides config default)</li>
 *   <li>{@code vectorExactRerank} - whether to rerank candidates using exact distances (default: true)</li>
 * </ul>
 *
 * <p>Search flow:
 * <ol>
 *   <li>Find the nprobe closest coarse centroids to the query vector</li>
 *   <li>Score all candidates in probed lists using approximate PQ distances</li>
 *   <li>If exact rerank is enabled, rerank using original vectors</li>
 *   <li>Return top-K candidates as doc IDs</li>
 * </ol>
 */
public class IvfPqVectorIndexReader implements VectorIndexReader {
  private static final Logger LOGGER = LoggerFactory.getLogger(IvfPqVectorIndexReader.class);
  private static final int DEFAULT_NPROBE = 8;
  private static final boolean DEFAULT_EXACT_RERANK = true;
  private static final int RERANK_CANDIDATE_MULTIPLIER = 4;

  private final String _column;
  private final int _dimension;
  private final int _nlist;
  private final int _pqM;
  private final int _pqNbits;
  private final int _numVectors;
  private final int _defaultNprobe;
  private final int _distanceFunctionCode;

  private final float[][] _coarseCentroids;
  private final ProductQuantizer _pq;

  // Inverted lists
  private final int[][] _listDocIds;
  private final byte[][][] _listPqCodes;
  private final float[][][] _listOriginalVectors;

  private final Closeable _closeHandle;

  public IvfPqVectorIndexReader(String column, File segmentDir, int numDocs, int nprobe) {
    _column = column;
    _defaultNprobe = nprobe > 0 ? nprobe : DEFAULT_NPROBE;

    File indexFile = findIndexFile(segmentDir);
    if (indexFile == null) {
      throw new IllegalStateException("Failed to find IVF_PQ index file for column: " + column);
    }

    try {
      RandomAccessFile raf = new RandomAccessFile(indexFile, "r");
      MappedByteBuffer buffer = raf.getChannel().map(FileChannel.MapMode.READ_ONLY, 0, raf.length());
      buffer.order(ByteOrder.LITTLE_ENDIAN);
      _closeHandle = raf;

      // Read header
      int magic = buffer.getInt();
      if (magic != IvfPqIndexFormat.MAGIC) {
        throw new IOException("Invalid IVF_PQ index magic: " + Integer.toHexString(magic));
      }
      int version = buffer.getInt();
      if (version != IvfPqIndexFormat.VERSION) {
        throw new IOException("Unsupported IVF_PQ index version: " + version);
      }
      _dimension = buffer.getInt();
      _nlist = buffer.getInt();
      _pqM = buffer.getInt();
      _pqNbits = buffer.getInt();
      _distanceFunctionCode = buffer.getInt();
      _numVectors = buffer.getInt();

      int ksub = 1 << _pqNbits;
      int dsub = _pqM > 0 ? _dimension / _pqM : 1;

      if (_numVectors == 0 || _nlist == 0) {
        _coarseCentroids = new float[0][];
        int safeDim = Math.max(_dimension, 1);
        int safeM = Math.max(_pqM, 1);
        // Ensure safeDim is divisible by safeM for ProductQuantizer
        if (safeDim % safeM != 0) {
          safeM = 1;
        }
        _pq = new ProductQuantizer(safeDim, safeM, Math.max(_pqNbits, 1));
        _listDocIds = new int[0][];
        _listPqCodes = new byte[0][][];
        _listOriginalVectors = new float[0][][];
        return;
      }

      // Read coarse centroids
      _coarseCentroids = new float[_nlist][_dimension];
      for (int c = 0; c < _nlist; c++) {
        for (int d = 0; d < _dimension; d++) {
          _coarseCentroids[c][d] = buffer.getFloat();
        }
      }

      // Read PQ codebooks
      float[][][] codebooks = new float[_pqM][ksub][dsub];
      for (int sub = 0; sub < _pqM; sub++) {
        for (int code = 0; code < ksub; code++) {
          for (int d = 0; d < dsub; d++) {
            codebooks[sub][code][d] = buffer.getFloat();
          }
        }
      }
      _pq = new ProductQuantizer(_dimension, _pqM, _pqNbits);
      _pq.setCodebooks(codebooks);

      // Read inverted lists (docIds + PQ codes + original vectors)
      _listDocIds = new int[_nlist][];
      _listPqCodes = new byte[_nlist][][];
      _listOriginalVectors = new float[_nlist][][];
      for (int list = 0; list < _nlist; list++) {
        int listSize = buffer.getInt();
        _listDocIds[list] = new int[listSize];
        for (int i = 0; i < listSize; i++) {
          _listDocIds[list][i] = buffer.getInt();
        }
        _listPqCodes[list] = new byte[listSize][_pqM];
        for (int i = 0; i < listSize; i++) {
          buffer.get(_listPqCodes[list][i]);
        }
        _listOriginalVectors[list] = new float[listSize][_dimension];
        for (int i = 0; i < listSize; i++) {
          for (int d = 0; d < _dimension; d++) {
            _listOriginalVectors[list][i][d] = buffer.getFloat();
          }
        }
      }

      LOGGER.info("Loaded IVF_PQ index for column: {} with {} lists, {} vectors, dimension={}, pqM={}, pqNbits={}",
          column, _nlist, _numVectors, _dimension, _pqM, _pqNbits);
    } catch (IOException e) {
      throw new RuntimeException("Failed to load IVF_PQ index for column: " + column, e);
    }
  }

  @Override
  public MutableRoaringBitmap getDocIds(float[] queryVector, int topK) {
    return (MutableRoaringBitmap) getDocIds(queryVector, topK, null);
  }

  @Override
  public ImmutableRoaringBitmap getDocIds(float[] queryVector, int topK, @Nullable Map<String, String> searchParams) {
    if (_numVectors == 0 || _nlist == 0) {
      return new MutableRoaringBitmap();
    }

    // Resolve runtime parameters
    int effectiveNprobe = resolveNprobe(searchParams);
    boolean exactRerank = resolveExactRerank(searchParams);

    // For COSINE distance, normalize the query vector before probing/scoring.
    // The index was built on normalized vectors, so probing and PQ ADC tables
    // must operate in the same normalized space.
    float[] searchVector = _distanceFunctionCode == IvfPqIndexFormat.DIST_COSINE
        ? VectorDistanceUtil.normalize(queryVector) : queryVector;

    // For exact rerank, we generate more candidates to improve recall after reranking
    int candidateTopK = exactRerank ? topK * RERANK_CANDIDATE_MULTIPLIER : topK;

    // Step 1: Find closest coarse centroids
    int[] probedLists = KMeans.findKNearest(searchVector, _coarseCentroids, _dimension, effectiveNprobe);

    // Step 2: Score candidates using PQ distances
    PriorityQueue<ScoredDoc> topKHeap = new PriorityQueue<>(candidateTopK + 1,
        (a, b) -> Float.compare(b._score, a._score)); // max-heap

    for (int listIdx : probedLists) {
      if (listIdx < 0 || listIdx >= _nlist) {
        continue;
      }
      int listSize = _listDocIds[listIdx].length;
      if (listSize == 0) {
        continue;
      }

      // Build PQ distance tables for residual: searchVector - coarseCentroid
      float[] residualQuery = new float[_dimension];
      for (int d = 0; d < _dimension; d++) {
        residualQuery[d] = searchVector[d] - _coarseCentroids[listIdx][d];
      }
      float[][] distTables = _pq.buildDistanceTables(residualQuery);

      for (int i = 0; i < listSize; i++) {
        float approxDist = _pq.computeDistanceFromTables(distTables, _listPqCodes[listIdx][i]);
        if (topKHeap.size() < candidateTopK) {
          topKHeap.add(new ScoredDoc(_listDocIds[listIdx][i], approxDist, listIdx, i));
        } else if (approxDist < topKHeap.peek()._score) {
          topKHeap.poll();
          topKHeap.add(new ScoredDoc(_listDocIds[listIdx][i], approxDist, listIdx, i));
        }
      }
    }

    // Step 3: Exact rerank if enabled
    if (exactRerank && !topKHeap.isEmpty()) {
      return rerankAndCollect(queryVector, topKHeap, topK);
    }

    // Collect results into bitmap (no rerank, take top topK from candidates)
    MutableRoaringBitmap result = new MutableRoaringBitmap();
    if (topKHeap.size() <= topK) {
      for (ScoredDoc doc : topKHeap) {
        result.add(doc._docId);
      }
    } else {
      // Need to take only the topK best from candidateTopK (already in heap)
      // Drain heap into array, sort by score ascending, take topK
      ScoredDoc[] candidates = topKHeap.toArray(new ScoredDoc[0]);
      java.util.Arrays.sort(candidates, (a, b) -> Float.compare(a._score, b._score));
      for (int i = 0; i < Math.min(topK, candidates.length); i++) {
        result.add(candidates[i]._docId);
      }
    }
    return result;
  }

  private ImmutableRoaringBitmap rerankAndCollect(float[] queryVector, PriorityQueue<ScoredDoc> candidates, int topK) {
    // Rerank using exact L2 distances from original vectors
    PriorityQueue<ScoredDoc> rerankHeap = new PriorityQueue<>(topK + 1,
        (a, b) -> Float.compare(b._score, a._score)); // max-heap

    for (ScoredDoc candidate : candidates) {
      float[] originalVector = _listOriginalVectors[candidate._listIdx][candidate._posInList];
      float exactDist = VectorDistanceUtil.computeDistance(queryVector, originalVector, _dimension,
          _distanceFunctionCode);
      if (rerankHeap.size() < topK) {
        rerankHeap.add(new ScoredDoc(candidate._docId, exactDist, candidate._listIdx, candidate._posInList));
      } else if (exactDist < rerankHeap.peek()._score) {
        rerankHeap.poll();
        rerankHeap.add(new ScoredDoc(candidate._docId, exactDist, candidate._listIdx, candidate._posInList));
      }
    }

    MutableRoaringBitmap result = new MutableRoaringBitmap();
    for (ScoredDoc doc : rerankHeap) {
      result.add(doc._docId);
    }
    return result;
  }

  private int resolveNprobe(@Nullable Map<String, String> searchParams) {
    if (searchParams != null) {
      String nprobeStr = searchParams.get(QueryOptionKey.VECTOR_NPROBE);
      if (nprobeStr != null) {
        int nprobe = Integer.parseInt(nprobeStr);
        if (nprobe > 0) {
          return Math.min(nprobe, _nlist);
        }
      }
    }
    return Math.min(_defaultNprobe, _nlist);
  }

  private boolean resolveExactRerank(@Nullable Map<String, String> searchParams) {
    if (searchParams != null) {
      String rerankStr = searchParams.get(QueryOptionKey.VECTOR_EXACT_RERANK);
      if (rerankStr != null) {
        return Boolean.parseBoolean(rerankStr);
      }
    }
    return DEFAULT_EXACT_RERANK;
  }

  @Override
  public Map<String, String> getIndexDebugInfo() {
    Map<String, String> info = new HashMap<>();
    info.put("backend", "IVF_PQ");
    info.put("nlist", String.valueOf(_nlist));
    info.put("pqM", String.valueOf(_pqM));
    info.put("pqNbits", String.valueOf(_pqNbits));
    info.put("nprobe", String.valueOf(_defaultNprobe));
    info.put("numVectors", String.valueOf(_numVectors));
    info.put("dimension", String.valueOf(_dimension));
    info.put("distanceFunction", distanceFunctionName(_distanceFunctionCode));
    info.put("exactRerank", String.valueOf(DEFAULT_EXACT_RERANK));
    return info;
  }

  private static String distanceFunctionName(int code) {
    switch (code) {
      case IvfPqIndexFormat.DIST_COSINE:
        return "COSINE";
      case IvfPqIndexFormat.DIST_EUCLIDEAN:
        return "EUCLIDEAN";
      case IvfPqIndexFormat.DIST_INNER_PRODUCT:
        return "INNER_PRODUCT";
      default:
        return "UNKNOWN";
    }
  }

  private File findIndexFile(File segmentDir) {
    File indexFile = new File(segmentDir, _column + IvfPqVectorIndexCreator.FILE_EXTENSION);
    if (indexFile.exists()) {
      return indexFile;
    }
    File v3Dir = SegmentDirectoryPaths.findSegmentDirectory(segmentDir);
    if (v3Dir != null) {
      indexFile = new File(v3Dir, _column + IvfPqVectorIndexCreator.FILE_EXTENSION);
      if (indexFile.exists()) {
        return indexFile;
      }
    }
    return null;
  }

  @Override
  public void close()
      throws IOException {
    if (_closeHandle != null) {
      _closeHandle.close();
    }
  }

  private static class ScoredDoc {
    final int _docId;
    final float _score;
    final int _listIdx;
    final int _posInList;

    ScoredDoc(int docId, float score, int listIdx, int posInList) {
      _docId = docId;
      _score = score;
      _listIdx = listIdx;
      _posInList = posInList;
    }
  }
}
