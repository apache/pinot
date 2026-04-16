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
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.PriorityQueue;
import org.apache.pinot.segment.local.segment.index.vector.IvfFlatVectorIndexCreator;
import org.apache.pinot.segment.local.segment.index.vector.VectorQuantizationUtils;
import org.apache.pinot.segment.spi.V1Constants;
import org.apache.pinot.segment.spi.index.creator.VectorIndexConfig;
import org.apache.pinot.segment.spi.index.creator.VectorQuantizerType;
import org.apache.pinot.segment.spi.index.reader.ApproximateRadiusVectorIndexReader;
import org.apache.pinot.segment.spi.index.reader.FilterAwareVectorIndexReader;
import org.apache.pinot.segment.spi.index.reader.NprobeAware;
import org.apache.pinot.segment.spi.index.reader.VectorQuantizer;
import org.apache.pinot.segment.spi.store.SegmentDirectoryPaths;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.roaringbitmap.buffer.MutableRoaringBitmap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Reader for IVF_FLAT (Inverted File with flat vectors) index.
 *
 * <p>Loads the entire index into memory at construction time for fast search.
 * The search algorithm:
 * <ol>
 *   <li>Computes distance from the query to all centroids.</li>
 *   <li>Selects the {@code nprobe} closest centroids.</li>
 *   <li>Scans all vectors in those centroids' inverted lists.</li>
 *   <li>Returns the top-K doc IDs as a bitmap.</li>
 * </ol>
 *
 * <h3>Thread safety</h3>
 * <p>This class is thread-safe for concurrent reads. The loaded index data is immutable
 * after construction. Query-scoped {@code nprobe} overrides are stored in a {@link ThreadLocal}
 * so concurrent queries cannot overwrite each other's search parameters.</p>
 */
public class IvfFlatVectorIndexReader
    implements FilterAwareVectorIndexReader, ApproximateRadiusVectorIndexReader, NprobeAware {
  private static final Logger LOGGER = LoggerFactory.getLogger(IvfFlatVectorIndexReader.class);

  /** Default nprobe value when not explicitly set. */
  static final int DEFAULT_NPROBE = 4;

  // Index data loaded from file
  private final int _dimension;
  private final int _numVectors;
  private final int _nlist;
  private final VectorIndexConfig.VectorDistanceFunction _distanceFunction;
  private final int _indexFormatVersion;
  private final VectorQuantizerType _quantizerType;
  private final VectorQuantizer _quantizer;
  private final float[][] _centroids;
  private final int[][] _listDocIds;
  private final byte[][][] _listEncodedVectors;
  private final String _column;
  private final int _defaultNprobe;

  /** Query-scoped nprobe override. Falls back to {@link #_defaultNprobe} when unset. */
  private final ThreadLocal<Integer> _nprobeOverride = new ThreadLocal<>();

  /**
   * Opens and loads an IVF_FLAT index from disk.
   *
   * @param column    the column name
   * @param indexDir  the segment index directory
   * @param config    the vector index configuration
   * @throws RuntimeException if the index file cannot be read or is corrupt
   */
  public IvfFlatVectorIndexReader(String column, File indexDir, VectorIndexConfig config) {
    _column = column;

    // Initialize nprobe to the default; query-time tuning should use NprobeAware#setNprobe.
    int configuredNprobe = DEFAULT_NPROBE;

    File indexFile = SegmentDirectoryPaths.findVectorIndexIndexFile(indexDir, column, config);
    if (indexFile == null || !indexFile.exists()) {
      throw new IllegalStateException(
          "Failed to find IVF_FLAT index file for column: " + column + " in dir: " + indexDir
              + ". Expected file: " + column + V1Constants.Indexes.VECTOR_IVF_FLAT_INDEX_FILE_EXTENSION);
    }

    try (DataInputStream in = new DataInputStream(new FileInputStream(indexFile))) {
      // --- Header ---
      int magic = in.readInt();
      Preconditions.checkState(magic == IvfFlatVectorIndexCreator.MAGIC,
          "Invalid IVF_FLAT magic: 0x%s, expected 0x%s",
          Integer.toHexString(magic), Integer.toHexString(IvfFlatVectorIndexCreator.MAGIC));

      int version = in.readInt();
      Preconditions.checkState(version == IvfFlatVectorIndexCreator.FORMAT_VERSION,
          "Unsupported IVF_FLAT format version: %s, expected: %s",
          version, IvfFlatVectorIndexCreator.FORMAT_VERSION);
      _indexFormatVersion = version;

      _dimension = in.readInt();
      _numVectors = in.readInt();
      _nlist = in.readInt();
      int distanceFunctionOrdinal = in.readInt();
      VectorIndexConfig.VectorDistanceFunction[] allFunctions = VectorIndexConfig.VectorDistanceFunction.values();
      Preconditions.checkState(distanceFunctionOrdinal >= 0 && distanceFunctionOrdinal < allFunctions.length,
          "Invalid distance function ordinal %s in IVF_FLAT index for column: %s (valid range: 0-%s)",
          distanceFunctionOrdinal, column, allFunctions.length - 1);
      _distanceFunction = allFunctions[distanceFunctionOrdinal];

      int quantizerTypeOrdinal = in.readInt();
      VectorQuantizerType[] allQuantizerTypes = VectorQuantizerType.values();
      Preconditions.checkState(quantizerTypeOrdinal >= 0 && quantizerTypeOrdinal < allQuantizerTypes.length,
          "Invalid quantizer type ordinal %s in IVF_FLAT index for column: %s (valid range: 0-%s)",
          quantizerTypeOrdinal, column, allQuantizerTypes.length - 1);
      _quantizerType = allQuantizerTypes[quantizerTypeOrdinal];

      int quantizerParamsLength = in.readInt();
      Preconditions.checkState(quantizerParamsLength >= 0,
          "Invalid quantizer params length %s in IVF_FLAT index for column: %s",
          quantizerParamsLength, column);
      byte[] quantizerParams = new byte[quantizerParamsLength];
      if (quantizerParamsLength > 0) {
        in.readFully(quantizerParams);
      }
      _quantizer = VectorQuantizationUtils.createReadQuantizer(_quantizerType, _dimension, quantizerParams);

      // Clamp nprobe to valid range
      _defaultNprobe = Math.min(configuredNprobe, _nlist);

      // --- Centroids ---
      _centroids = new float[_nlist][_dimension];
      for (int c = 0; c < _nlist; c++) {
        for (int d = 0; d < _dimension; d++) {
          _centroids[c][d] = in.readFloat();
        }
      }

      // --- Inverted Lists ---
      _listDocIds = new int[_nlist][];
      _listEncodedVectors = new byte[_nlist][][];

      for (int c = 0; c < _nlist; c++) {
        int listSize = in.readInt();
        _listDocIds[c] = new int[listSize];
        for (int i = 0; i < listSize; i++) {
          _listDocIds[c][i] = in.readInt();
        }
        int encodedBytesPerVector = _quantizer.getEncodedBytesPerVector();
        _listEncodedVectors[c] = new byte[listSize][encodedBytesPerVector];
        for (int i = 0; i < listSize; i++) {
          in.readFully(_listEncodedVectors[c][i]);
        }
      }

      // We skip reading the offset table and footer since we read sequentially

      LOGGER.info("Loaded IVF_FLAT index for column: {}: {} vectors, {} centroids, dim={}, nprobe={}, distance={}, "
              + "formatVersion={}, quantizer={}",
          column, _numVectors, _nlist, _dimension, getNprobe(), _distanceFunction, _indexFormatVersion,
          _quantizerType);
    } catch (IOException e) {
      throw new RuntimeException(
          "Failed to load IVF_FLAT index for column: " + column + " from file: " + indexFile, e);
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

    int effectiveNprobe = Math.min(getNprobe(), _nlist);

    // Step 1: Find the nprobe closest centroids
    int[] probeCentroids = findClosestCentroids(searchQuery, effectiveNprobe);

    // Step 2: Scan all vectors in the selected inverted lists, maintaining a max-heap of size topK
    // Max-heap: the largest distance is at the top, so we can efficiently evict the worst candidate.
    int effectiveTopK = Math.min(topK, _numVectors);
    PriorityQueue<ScoredDoc> maxHeap = new PriorityQueue<>(effectiveTopK,
        (a, b) -> Float.compare(b._distance, a._distance));

    for (int probeIdx : probeCentroids) {
      int[] docIds = _listDocIds[probeIdx];

      for (int i = 0; i < docIds.length; i++) {
        float dist = getDistanceFromList(probeIdx, i, searchQuery);
        if (maxHeap.size() < effectiveTopK) {
          maxHeap.offer(new ScoredDoc(docIds[i], dist));
        } else if (dist < maxHeap.peek()._distance) {
          maxHeap.poll();
          maxHeap.offer(new ScoredDoc(docIds[i], dist));
        }
      }
    }

    // Step 3: Collect results into a bitmap
    MutableRoaringBitmap result = new MutableRoaringBitmap();
    for (ScoredDoc doc : maxHeap) {
      result.add(doc._docId);
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

    int effectiveNprobe = Math.min(getNprobe(), _nlist);

    // Step 1: Find the nprobe closest centroids (same as unfiltered)
    int[] probeCentroids = findClosestCentroids(searchQuery, effectiveNprobe);

    // Step 2: Scan selected inverted lists, but only consider docs present in preFilterBitmap
    int effectiveTopK = Math.min(topK, _numVectors);
    PriorityQueue<ScoredDoc> maxHeap = new PriorityQueue<>(effectiveTopK,
        (a, b) -> Float.compare(b._distance, a._distance));

    for (int probeIdx : probeCentroids) {
      int[] docIds = _listDocIds[probeIdx];

      for (int i = 0; i < docIds.length; i++) {
        // Only consider documents that pass the pre-filter
        if (!preFilterBitmap.contains(docIds[i])) {
          continue;
        }
        float dist = getDistanceFromList(probeIdx, i, searchQuery);
        if (maxHeap.size() < effectiveTopK) {
          maxHeap.offer(new ScoredDoc(docIds[i], dist));
        } else if (dist < maxHeap.peek()._distance) {
          maxHeap.poll();
          maxHeap.offer(new ScoredDoc(docIds[i], dist));
        }
      }
    }

    // Step 3: Collect results into a bitmap
    MutableRoaringBitmap result = new MutableRoaringBitmap();
    for (ScoredDoc doc : maxHeap) {
      result.add(doc._docId);
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

    int effectiveNprobe = Math.min(getNprobe(), _nlist);
    int[] probeCentroids = findClosestCentroids(searchQuery, effectiveNprobe);
    int effectiveMaxCandidates = Math.min(maxCandidates, _numVectors);
    PriorityQueue<ScoredDoc> maxHeap = new PriorityQueue<>(effectiveMaxCandidates,
        (a, b) -> Float.compare(b._distance, a._distance));

    for (int probeIdx : probeCentroids) {
      int[] docIds = _listDocIds[probeIdx];
      for (int i = 0; i < docIds.length; i++) {
        float distance = getDistanceFromList(probeIdx, i, searchQuery);
        if (distance <= threshold) {
          offer(maxHeap, docIds[i], distance, effectiveMaxCandidates);
        }
      }
    }

    MutableRoaringBitmap result = new MutableRoaringBitmap();
    for (ScoredDoc doc : maxHeap) {
      result.add(doc._docId);
    }
    return result;
  }

  /**
   * Sets the number of centroids to probe during search.
   * This allows query-time tuning of the recall/speed tradeoff.
   *
   * @param nprobe number of centroids to probe (clamped to [1, nlist])
   *
   * <p><b>Thread-safety note:</b> The override is stored in a thread-local so concurrent queries can
   * tune nprobe independently on the same reader instance.</p>
   */
  @Override
  public void setNprobe(int nprobe) {
    if (nprobe < 1) {
      throw new IllegalArgumentException("nprobe must be >= 1, got: " + nprobe);
    }
    _nprobeOverride.set(Math.min(nprobe, _nlist));
  }

  @Override
  public void clearNprobe() {
    _nprobeOverride.remove();
  }

  /**
   * Returns the current nprobe setting.
   */
  public int getNprobe() {
    Integer nprobeOverride = _nprobeOverride.get();
    return nprobeOverride != null ? nprobeOverride
        : (_defaultNprobe > 0 ? _defaultNprobe : Math.min(DEFAULT_NPROBE, _nlist));
  }

  @Override
  public void close()
      throws IOException {
    clearNprobe();
  }

  // -----------------------------------------------------------------------
  // Internal helpers
  // -----------------------------------------------------------------------

  /**
   * Computes distance between two vectors using the configured distance function.
   * Internally uses L2 for EUCLIDEAN/L2, cosine for COSINE, negative dot for INNER_PRODUCT/DOT_PRODUCT.
   */
  private float computeDistance(float[] a, float[] b) {
    return VectorQuantizationUtils.computeDistance(a, b, _distanceFunction);
  }

  private float getDistanceFromList(int probeIdx, int listOffset, float[] query) {
    return _quantizer.computeDistance(query, _listEncodedVectors[probeIdx][listOffset], _distanceFunction);
  }

  /**
   * Finds the n closest centroids to the given query vector.
   *
   * @param query  the query vector
   * @param n      number of centroids to return
   * @return array of centroid indices sorted by increasing distance
   */
  private int[] findClosestCentroids(float[] query, int n) {
    int[] bestIndices = new int[n];
    if (n == 0) {
      return bestIndices;
    }

    // Maintain a sorted top-n array (ascending by distance) using insertion sort.
    // This avoids boxing Integer[] and O(nlist log nlist) full sort — we only need nprobe smallest.
    float[] bestDistances = new float[n];
    Arrays.fill(bestDistances, Float.POSITIVE_INFINITY);

    for (int c = 0; c < _nlist; c++) {
      float distance = computeDistance(query, _centroids[c]);
      if (distance >= bestDistances[n - 1]) {
        continue;
      }
      // Insert into sorted position by shifting larger entries right
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

  private static void offer(PriorityQueue<ScoredDoc> heap, int docId, float distance, int maxCandidates) {
    if (heap.size() < maxCandidates) {
      heap.offer(new ScoredDoc(docId, distance));
    } else if (distance < heap.peek()._distance) {
      heap.poll();
      heap.offer(new ScoredDoc(docId, distance));
    }
  }

  // -----------------------------------------------------------------------
  // Accessors for testing and introspection
  // -----------------------------------------------------------------------

  @Override
  public Map<String, Object> getIndexDebugInfo() {
    Map<String, Object> info = new LinkedHashMap<>();
    info.put("backend", "IVF_FLAT");
    info.put("column", _column);
    info.put("dimension", _dimension);
    info.put("numVectors", _numVectors);
    info.put("nlist", _nlist);
    info.put("distanceFunction", _distanceFunction.name());
    info.put("effectiveNprobe", getNprobe());
    info.put("indexFormatVersion", _indexFormatVersion);
    info.put("quantizer", _quantizerType.name());
    info.put("encodedBytesPerVector", _quantizer.getEncodedBytesPerVector());

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
    if (_nlist > 0) {
      info.put("avgDocsPerList", _numVectors > 0 ? (double) _numVectors / _nlist : 0.0);
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
  public VectorIndexConfig.VectorDistanceFunction getDistanceFunction() {
    return _distanceFunction;
  }

  @VisibleForTesting
  public float[][] getCentroids() {
    return _centroids;
  }

  @VisibleForTesting
  public int[][] getListDocIds() {
    return _listDocIds;
  }

  @VisibleForTesting
  public VectorQuantizerType getQuantizerType() {
    return _quantizerType;
  }

  /**
   * A document with its computed distance, used in the max-heap during search.
   */
  private static final class ScoredDoc {
    final int _docId;
    final float _distance;

    ScoredDoc(int docId, float distance) {
      _docId = docId;
      _distance = distance;
    }
  }
}
