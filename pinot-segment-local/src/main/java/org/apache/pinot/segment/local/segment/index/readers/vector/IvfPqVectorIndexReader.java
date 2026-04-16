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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.PriorityQueue;
import org.apache.pinot.segment.local.segment.index.vector.IvfPqIndexFormat;
import org.apache.pinot.segment.local.segment.index.vector.IvfPqVectorIndexCreator;
import org.apache.pinot.segment.local.segment.index.vector.ProductQuantizer;
import org.apache.pinot.segment.local.segment.index.vector.VectorQuantizationUtils;
import org.apache.pinot.segment.spi.index.creator.VectorIndexConfig;
import org.apache.pinot.segment.spi.index.reader.ApproximateRadiusVectorIndexReader;
import org.apache.pinot.segment.spi.index.reader.FilterAwareVectorIndexReader;
import org.apache.pinot.segment.spi.index.reader.NprobeAware;
import org.apache.pinot.segment.spi.store.SegmentDirectoryPaths;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.roaringbitmap.buffer.MutableRoaringBitmap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Reader for IVF_PQ (Inverted File with residual Product Quantization) index.
 *
 * <p>The reader loads the entire index into memory and performs ANN search by probing the
 * nearest coarse centroids, then scoring candidates with either asymmetric L2 lookup tables
 * or full reconstructed-vector distance evaluation for non-L2 metrics.</p>
 */
public class IvfPqVectorIndexReader
    implements FilterAwareVectorIndexReader, ApproximateRadiusVectorIndexReader, NprobeAware {
  private static final Logger LOGGER = LoggerFactory.getLogger(IvfPqVectorIndexReader.class);

  /** Default nprobe value when not explicitly configured. */
  static final int DEFAULT_NPROBE = 4;

  private final int _dimension;
  private final int _numVectors;
  private final int _nlist;
  private final int _pqM;
  private final int _pqNbits;
  private final int _trainSampleSize;
  private final long _trainingSeed;
  private final VectorIndexConfig.VectorDistanceFunction _distanceFunction;
  private final float[][] _centroids;
  private final float[] _centroidNormSquares;
  private final float[][][] _codebooks;
  private final float[][] _codebookNormSquares;
  private final int[][] _listDocIds;
  private final byte[][][] _listCodes;
  private final int[] _subvectorLengths;
  private final int[] _subvectorOffsets;
  private final String _column;
  private final int _defaultNprobe;
  private final ThreadLocal<Integer> _nprobeOverride = new ThreadLocal<>();

  /**
   * Opens and loads an IVF_PQ index from disk.
   *
   * @param column the column name
   * @param indexDir the segment index directory
   * @param config vector index configuration
   */
  public IvfPqVectorIndexReader(String column, File indexDir, VectorIndexConfig config) {
    _column = column;

    File indexFile = SegmentDirectoryPaths.findVectorIndexIndexFile(indexDir, column, config);
    if (indexFile == null || !indexFile.exists()) {
      throw new IllegalStateException(
          "Failed to find IVF_PQ index file for column: " + column + " in dir: " + indexDir
              + ". Expected file: " + column + IvfPqVectorIndexCreator.INDEX_FILE_EXTENSION);
    }

    try {
      IvfPqIndexFormat.IndexData indexData = IvfPqIndexFormat.read(indexFile);
      _dimension = indexData.getDimension();
      _numVectors = indexData.getNumVectors();
      _nlist = indexData.getNlist();
      _pqM = indexData.getPqM();
      _pqNbits = indexData.getPqNbits();
      _trainSampleSize = indexData.getTrainSampleSize();
      _trainingSeed = indexData.getTrainingSeed();
      _distanceFunction = indexData.getDistanceFunction();
      _subvectorLengths = indexData.getSubvectorLengths();
      _subvectorOffsets = VectorQuantizationUtils.computeSubvectorOffsets(_subvectorLengths);

      _centroids = indexData.getCentroids();
      _centroidNormSquares = new float[_nlist];
      for (int c = 0; c < _nlist; c++) {
        float centroidNormSquare = 0.0f;
        for (int d = 0; d < _dimension; d++) {
          float value = _centroids[c][d];
          centroidNormSquare += value * value;
        }
        _centroidNormSquares[c] = centroidNormSquare;
      }

      _codebooks = indexData.getCodebooks();
      _codebookNormSquares = new float[_pqM][];
      for (int m = 0; m < _pqM; m++) {
        _codebookNormSquares[m] = new float[_codebooks[m].length];
        for (int code = 0; code < _codebooks[m].length; code++) {
          float codeNormSquare = 0.0f;
          for (float value : _codebooks[m][code]) {
            codeNormSquare += value * value;
          }
          _codebookNormSquares[m][code] = codeNormSquare;
        }
      }
      _listDocIds = indexData.getListDocIds();
      _listCodes = indexData.getListCodes();

      _defaultNprobe = Math.min(DEFAULT_NPROBE, Math.max(_nlist, 1));
      LOGGER.info("Loaded IVF_PQ index for column: {}: {} vectors, {} centroids, dim={}, pqM={}, pqNbits={}, "
              + "nprobe={}, distance={}", column, _numVectors, _nlist, _dimension, _pqM, _pqNbits, _defaultNprobe,
          _distanceFunction);
    } catch (IOException e) {
      throw new RuntimeException("Failed to load IVF_PQ index for column: " + column + " from file: " + indexFile, e);
    }
  }

  @Override
  public MutableRoaringBitmap getDocIds(float[] searchQuery, int topK) {
    Preconditions.checkArgument(searchQuery.length == _dimension,
        "Query dimension mismatch: expected %s, got %s", _dimension, searchQuery.length);
    Preconditions.checkArgument(topK > 0, "topK must be positive, got: %s", topK);

    if (_numVectors == 0 || _nlist == 0) {
      return new MutableRoaringBitmap();
    }

    float[] query = VectorQuantizationUtils.transformForDistance(searchQuery, _distanceFunction);
    int effectiveNprobe = Math.min(getNprobe(), _nlist);
    int[] probeCentroids = findClosestCentroids(query, effectiveNprobe);
    int effectiveTopK = Math.min(topK, _numVectors);
    PriorityQueue<ScoredDoc> heap = new PriorityQueue<>(effectiveTopK,
        (a, b) -> Float.compare(b._distance, a._distance));

    for (int probe : probeCentroids) {
      float[] centroid = _centroids[probe];
      float[] queryResidual = VectorQuantizationUtils.subtractVectors(query, centroid);
      byte[][] listCodes = _listCodes[probe];
      int[] listDocIds = _listDocIds[probe];

      if (_distanceFunction == VectorIndexConfig.VectorDistanceFunction.EUCLIDEAN
          || _distanceFunction == VectorIndexConfig.VectorDistanceFunction.L2) {
        float[][] tables = ProductQuantizer.buildL2DistanceTables(queryResidual, _codebooks, _subvectorLengths);
        for (int i = 0; i < listDocIds.length; i++) {
          float distance = 0.0f;
          byte[] codes = listCodes[i];
          for (int m = 0; m < _pqM; m++) {
            distance += tables[m][codes[m] & 0xFF];
          }
          offer(heap, listDocIds[i], distance, effectiveTopK);
        }
      } else if (_distanceFunction == VectorIndexConfig.VectorDistanceFunction.INNER_PRODUCT
          || _distanceFunction == VectorIndexConfig.VectorDistanceFunction.DOT_PRODUCT) {
        for (int i = 0; i < listDocIds.length; i++) {
          byte[] codes = listCodes[i];
          float distance = -computeApproximateDotProduct(query, centroid, codes);
          offer(heap, listDocIds[i], distance, effectiveTopK);
        }
      } else {
        float queryNormSquare = dotProduct(query, query);
        for (int i = 0; i < listDocIds.length; i++) {
          byte[] codes = listCodes[i];
          float dotProduct = computeApproximateDotProduct(query, centroid, codes);
          float normSquare = computeApproximateNormSquare(probe, centroid, codes);
          float distance = computeCosineDistanceFromDotProduct(dotProduct, queryNormSquare, normSquare);
          offer(heap, listDocIds[i], distance, effectiveTopK);
        }
      }
    }

    MutableRoaringBitmap result = new MutableRoaringBitmap();
    for (ScoredDoc scoredDoc : heap) {
      result.add(scoredDoc._docId);
    }
    return result;
  }

  @Override
  public ImmutableRoaringBitmap getDocIds(float[] searchQuery, int topK, ImmutableRoaringBitmap preFilterBitmap) {
    Preconditions.checkArgument(searchQuery.length == _dimension,
        "Query dimension mismatch: expected %s, got %s", _dimension, searchQuery.length);
    Preconditions.checkArgument(topK > 0, "topK must be positive, got: %s", topK);

    if (_numVectors == 0 || _nlist == 0 || preFilterBitmap.isEmpty()) {
      return new MutableRoaringBitmap();
    }

    float[] query = VectorQuantizationUtils.transformForDistance(searchQuery, _distanceFunction);
    int effectiveNprobe = Math.min(getNprobe(), _nlist);
    int[] probeCentroids = findClosestCentroids(query, effectiveNprobe);
    int effectiveTopK = Math.min(topK, _numVectors);
    PriorityQueue<ScoredDoc> heap = new PriorityQueue<>(effectiveTopK,
        (a, b) -> Float.compare(b._distance, a._distance));

    for (int probe : probeCentroids) {
      float[] centroid = _centroids[probe];
      float[] queryResidual = VectorQuantizationUtils.subtractVectors(query, centroid);
      byte[][] listCodes = _listCodes[probe];
      int[] listDocIds = _listDocIds[probe];

      if (_distanceFunction == VectorIndexConfig.VectorDistanceFunction.EUCLIDEAN
          || _distanceFunction == VectorIndexConfig.VectorDistanceFunction.L2) {
        float[][] tables = ProductQuantizer.buildL2DistanceTables(queryResidual, _codebooks, _subvectorLengths);
        for (int i = 0; i < listDocIds.length; i++) {
          if (!preFilterBitmap.contains(listDocIds[i])) {
            continue;
          }
          float distance = 0.0f;
          byte[] codes = listCodes[i];
          for (int m = 0; m < _pqM; m++) {
            distance += tables[m][codes[m] & 0xFF];
          }
          offer(heap, listDocIds[i], distance, effectiveTopK);
        }
      } else if (_distanceFunction == VectorIndexConfig.VectorDistanceFunction.INNER_PRODUCT
          || _distanceFunction == VectorIndexConfig.VectorDistanceFunction.DOT_PRODUCT) {
        for (int i = 0; i < listDocIds.length; i++) {
          if (!preFilterBitmap.contains(listDocIds[i])) {
            continue;
          }
          byte[] codes = listCodes[i];
          float distance = -computeApproximateDotProduct(query, centroid, codes);
          offer(heap, listDocIds[i], distance, effectiveTopK);
        }
      } else {
        float queryNormSquare = dotProduct(query, query);
        for (int i = 0; i < listDocIds.length; i++) {
          if (!preFilterBitmap.contains(listDocIds[i])) {
            continue;
          }
          byte[] codes = listCodes[i];
          float dotProd = computeApproximateDotProduct(query, centroid, codes);
          float normSquare = computeApproximateNormSquare(probe, centroid, codes);
          float distance = computeCosineDistanceFromDotProduct(dotProd, queryNormSquare, normSquare);
          offer(heap, listDocIds[i], distance, effectiveTopK);
        }
      }
    }

    MutableRoaringBitmap result = new MutableRoaringBitmap();
    for (ScoredDoc scoredDoc : heap) {
      result.add(scoredDoc._docId);
    }
    return result;
  }

  @Override
  public ImmutableRoaringBitmap getDocIdsWithinApproximateRadius(float[] searchQuery, float threshold,
      int maxCandidates) {
    Preconditions.checkArgument(searchQuery.length == _dimension,
        "Query dimension mismatch: expected %s, got %s", _dimension, searchQuery.length);
    Preconditions.checkArgument(maxCandidates > 0, "maxCandidates must be positive, got: %s", maxCandidates);

    if (_numVectors == 0 || _nlist == 0) {
      return new MutableRoaringBitmap();
    }

    float[] query = VectorQuantizationUtils.transformForDistance(searchQuery, _distanceFunction);
    int effectiveNprobe = Math.min(getNprobe(), _nlist);
    int[] probeCentroids = findClosestCentroids(query, effectiveNprobe);
    int effectiveMaxCandidates = Math.min(maxCandidates, _numVectors);
    PriorityQueue<ScoredDoc> heap = new PriorityQueue<>(effectiveMaxCandidates,
        (a, b) -> Float.compare(b._distance, a._distance));

    for (int probe : probeCentroids) {
      float[] centroid = _centroids[probe];
      float[] queryResidual = VectorQuantizationUtils.subtractVectors(query, centroid);
      byte[][] listCodes = _listCodes[probe];
      int[] listDocIds = _listDocIds[probe];

      if (_distanceFunction == VectorIndexConfig.VectorDistanceFunction.EUCLIDEAN
          || _distanceFunction == VectorIndexConfig.VectorDistanceFunction.L2) {
        float[][] tables = ProductQuantizer.buildL2DistanceTables(queryResidual, _codebooks, _subvectorLengths);
        for (int i = 0; i < listDocIds.length; i++) {
          float distance = 0.0f;
          byte[] codes = listCodes[i];
          for (int m = 0; m < _pqM; m++) {
            distance += tables[m][codes[m] & 0xFF];
          }
          if (distance <= threshold) {
            offer(heap, listDocIds[i], distance, effectiveMaxCandidates);
          }
        }
      } else if (_distanceFunction == VectorIndexConfig.VectorDistanceFunction.INNER_PRODUCT
          || _distanceFunction == VectorIndexConfig.VectorDistanceFunction.DOT_PRODUCT) {
        for (int i = 0; i < listDocIds.length; i++) {
          byte[] codes = listCodes[i];
          float distance = -computeApproximateDotProduct(query, centroid, codes);
          if (distance <= threshold) {
            offer(heap, listDocIds[i], distance, effectiveMaxCandidates);
          }
        }
      } else {
        float queryNormSquare = dotProduct(query, query);
        for (int i = 0; i < listDocIds.length; i++) {
          byte[] codes = listCodes[i];
          float dotProduct = computeApproximateDotProduct(query, centroid, codes);
          float normSquare = computeApproximateNormSquare(probe, centroid, codes);
          float distance = computeCosineDistanceFromDotProduct(dotProduct, queryNormSquare, normSquare);
          if (distance <= threshold) {
            offer(heap, listDocIds[i], distance, effectiveMaxCandidates);
          }
        }
      }
    }

    MutableRoaringBitmap result = new MutableRoaringBitmap();
    for (ScoredDoc scoredDoc : heap) {
      result.add(scoredDoc._docId);
    }
    return result;
  }

  @Override
  public void setNprobe(int nprobe) {
    if (nprobe < 1) {
      throw new IllegalArgumentException("nprobe must be >= 1, got: " + nprobe);
    }
    _nprobeOverride.set(Math.min(nprobe, Math.max(_nlist, 1)));
  }

  @Override
  public void clearNprobe() {
    _nprobeOverride.remove();
  }

  public int getNprobe() {
    Integer nprobeOverride = _nprobeOverride.get();
    return nprobeOverride != null ? nprobeOverride : _defaultNprobe;
  }

  @Override
  public void close()
      throws IOException {
    clearNprobe();
  }

  private int[] findClosestCentroids(float[] query, int n) {
    if (n <= 0) {
      return new int[0];
    }

    int[] bestIndices = new int[n];
    float[] bestDistances = new float[n];
    Arrays.fill(bestDistances, Float.POSITIVE_INFINITY);

    for (int c = 0; c < _nlist; c++) {
      float distance = VectorQuantizationUtils.computeDistance(query, _centroids[c], _distanceFunction);
      if (distance >= bestDistances[n - 1]) {
        continue;
      }
      int insertPos = n - 1;
      while (insertPos > 0 && distance < bestDistances[insertPos - 1]) {
        bestDistances[insertPos] = bestDistances[insertPos - 1];
        bestIndices[insertPos] = bestIndices[insertPos - 1];
        insertPos--;
      }
      bestDistances[insertPos] = distance;
      bestIndices[insertPos] = c;
    }

    return bestIndices;
  }

  private void offer(PriorityQueue<ScoredDoc> heap, int docId, float distance, int effectiveTopK) {
    if (heap.size() < effectiveTopK) {
      heap.offer(new ScoredDoc(docId, distance));
    } else if (distance < heap.peek()._distance) {
      heap.poll();
      heap.offer(new ScoredDoc(docId, distance));
    }
  }

  private float computeApproximateDotProduct(float[] query, float[] centroid, byte[] codes) {
    float dotProduct = dotProduct(query, centroid);
    int offset = 0;
    for (int m = 0; m < _pqM; m++) {
      int code = codes[m] & 0xFF;
      float[] codeword = _codebooks[m][code];
      for (int d = 0; d < _subvectorLengths[m]; d++) {
        dotProduct += query[offset + d] * codeword[d];
      }
      offset += _subvectorLengths[m];
    }
    return dotProduct;
  }

  private float computeApproximateNormSquare(int probe, float[] centroid, byte[] codes) {
    float normSquare = _centroidNormSquares[probe];
    int offset = 0;
    for (int m = 0; m < _pqM; m++) {
      int code = codes[m] & 0xFF;
      float[] codeword = _codebooks[m][code];
      float centroidDotCodeword = 0.0f;
      for (int d = 0; d < _subvectorLengths[m]; d++) {
        centroidDotCodeword += centroid[offset + d] * codeword[d];
      }
      normSquare += 2.0f * centroidDotCodeword + _codebookNormSquares[m][code];
      offset += _subvectorLengths[m];
    }
    return normSquare;
  }

  private static float dotProduct(float[] left, float[] right) {
    float dotProduct = 0.0f;
    for (int i = 0; i < left.length; i++) {
      dotProduct += left[i] * right[i];
    }
    return dotProduct;
  }

  private static float computeCosineDistanceFromDotProduct(float dotProduct, float queryNormSquare,
      float candidateNormSquare) {
    if (queryNormSquare <= 0.0f || candidateNormSquare <= 0.0f) {
      return 1.0f;
    }
    return (float) (1.0d - (dotProduct / (Math.sqrt(queryNormSquare) * Math.sqrt(candidateNormSquare))));
  }

  @Override
  public Map<String, Object> getIndexDebugInfo() {
    Map<String, Object> info = new LinkedHashMap<>();
    info.put("backend", "IVF_PQ");
    info.put("column", _column);
    info.put("dimension", _dimension);
    info.put("numVectors", _numVectors);
    info.put("nlist", _nlist);
    info.put("pqM", _pqM);
    info.put("pqNbits", _pqNbits);
    info.put("distanceFunction", _distanceFunction.name());
    info.put("requestedTrainSampleSize", _trainSampleSize);
    info.put("effectiveTrainSampleSize", Math.min(_trainSampleSize, _numVectors));
    info.put("trainingSeed", _trainingSeed);
    info.put("effectiveNprobe", _nlist > 0 ? getNprobe() : 0);
    info.put("codebookSize", _numVectors > 0 ? (1 << _pqNbits) : 0);
    info.put("subvectorLengths", Arrays.toString(_subvectorLengths));

    // Per-list cardinality stats
    if (_nlist > 0) {
      int minListSize = Integer.MAX_VALUE;
      int maxListSize = 0;
      int emptyLists = 0;
      for (int[] docIds : _listDocIds) {
        int size = docIds.length;
        if (size == 0) {
          emptyLists++;
        }
        minListSize = Math.min(minListSize, size);
        maxListSize = Math.max(maxListSize, size);
      }
      info.put("avgDocsPerList", (double) _numVectors / _nlist);
      info.put("minListSize", minListSize == Integer.MAX_VALUE ? 0 : minListSize);
      info.put("maxListSize", maxListSize);
      info.put("emptyLists", emptyLists);
    }
    return info;
  }

  @VisibleForTesting
  public int getDimension() {
    return _dimension;
  }

  @VisibleForTesting
  public int getNumVectors() {
    return _numVectors;
  }

  @VisibleForTesting
  public int getNlist() {
    return _nlist;
  }

  @VisibleForTesting
  public int getPqM() {
    return _pqM;
  }

  @VisibleForTesting
  public int getPqNbits() {
    return _pqNbits;
  }

  @VisibleForTesting
  public int getTrainSampleSize() {
    return _trainSampleSize;
  }

  @VisibleForTesting
  public long getTrainingSeed() {
    return _trainingSeed;
  }

  @VisibleForTesting
  public VectorIndexConfig.VectorDistanceFunction getDistanceFunction() {
    return _distanceFunction;
  }

  @VisibleForTesting
  public float[][] getCentroids() {
    return _centroids;
  }

  @VisibleForTesting
  public float[][][] getCodebooks() {
    return _codebooks;
  }

  @VisibleForTesting
  public int[][] getListDocIds() {
    return _listDocIds;
  }

  @VisibleForTesting
  public byte[][][] getListCodes() {
    return _listCodes;
  }

  @VisibleForTesting
  public int[] getSubvectorLengths() {
    return _subvectorLengths;
  }

  @VisibleForTesting
  public int[] getSubvectorOffsets() {
    return _subvectorOffsets;
  }

  private static final class ScoredDoc {
    private final int _docId;
    private final float _distance;

    private ScoredDoc(int docId, float distance) {
      _docId = docId;
      _distance = distance;
    }
  }
}
