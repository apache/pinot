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
package org.apache.pinot.segment.local.segment.index.vector;

import com.google.common.base.Preconditions;
import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Random;
import javax.annotation.Nullable;
import org.apache.pinot.common.function.scalar.VectorFunctions;
import org.apache.pinot.segment.spi.V1Constants;
import org.apache.pinot.segment.spi.index.creator.VectorIndexConfig;
import org.apache.pinot.segment.spi.index.creator.VectorIndexCreator;
import org.apache.pinot.segment.spi.index.creator.VectorQuantizerType;
import org.apache.pinot.segment.spi.index.reader.VectorQuantizer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Creates an IVF_FLAT (Inverted File with flat vectors) index for immutable segments.
 *
 * <p>The creator buffers all vectors in memory during {@link #add(float[])} calls, then
 * trains k-means centroids, assigns vectors to their nearest centroids, and serializes
 * the complete index to a single {@code .ivfflat.index} file during {@link #seal()}.</p>
 *
 * <h3>Thread safety</h3>
 * <p>This class is NOT thread-safe. It is designed for single-threaded segment creation.</p>
 *
 * <h3>File format (version 1)</h3>
 * <pre>
 * [Header]
 *   magic:                  4 bytes (0x49564646 = "IVFF")
 *   version:                4 bytes (1)
 *   dimension:              4 bytes
 *   numVectors:             4 bytes
 *   nlist:                  4 bytes
 *   distanceFunctionOrd:    4 bytes
 *   quantizerTypeOrd:       4 bytes
 *   quantizerParamsLen:     4 bytes
 *   quantizerParams:        quantizerParamsLen bytes
 *
 * [Centroids Section]
 *   nlist x dimension x 4 bytes (float32)
 *
 * [Inverted Lists Section]
 *   For each centroid i (0..nlist-1):
 *     listSize_i:           4 bytes
 *     docIds_i:             listSize_i x 4 bytes (int32)
 *     encodedVectors_i:     listSize_i x encodedBytesPerVector bytes
 *
 * [Inverted List Offsets]
 *   nlist x 8 bytes (long offset to start of each inverted list)
 *
 * [Footer]
 *   offsetToOffsets:        8 bytes (position of the offsets section)
 * </pre>
 *
 * <p>Header fields, centroid float32 values, list sizes/doc IDs, offsets, and footer metadata are written in
 * big-endian order (Java {@link DataOutputStream} default). {@code encodedVectors} payload bytes are emitted
 * verbatim by the selected quantizer and therefore use quantizer-defined encoding semantics; the
 * {@link FlatQuantizer} currently stores float32 payloads in little-endian order.</p>
 */
public class IvfFlatVectorIndexCreator implements VectorIndexCreator {
  private static final Logger LOGGER = LoggerFactory.getLogger(IvfFlatVectorIndexCreator.class);

  /** Magic bytes identifying an IVF_FLAT index file: ASCII "IVFF". */
  public static final int MAGIC = 0x49564646;

  /** Current file format version (quantizer-aware encoded vectors). */
  public static final int FORMAT_VERSION = 1;

  /** Default number of Voronoi cells (centroids). */
  public static final int DEFAULT_NLIST = 128;

  /** Maximum number of k-means iterations. */
  static final int MAX_KMEANS_ITERATIONS = 50;

  /** Convergence threshold: stop when centroid movement is below this fraction. */
  static final float CONVERGENCE_THRESHOLD = 1e-5f;

  /** Default training sample size multiplier relative to nlist. */
  static final int DEFAULT_TRAIN_SAMPLE_MULTIPLIER = 40;

  /** Minimum training sample size. */
  static final int DEFAULT_MIN_TRAIN_SAMPLE_SIZE = 10000;

  private final String _column;
  private final File _indexDir;
  private final int _dimension;
  private final int _nlist;
  private final int _trainSampleSize;
  private final long _trainingSeed;
  private final VectorIndexConfig.VectorDistanceFunction _distanceFunction;
  private final VectorQuantizerType _quantizerType;

  /** All vectors collected during add(), indexed by docId (ordinal). */
  private final List<float[]> _vectors = new ArrayList<>();

  private boolean _sealed = false;

  /**
   * Creates a new IVF_FLAT index creator.
   *
   * @param column     the column name
   * @param indexDir   the segment index directory
   * @param config     the vector index configuration
   */
  public IvfFlatVectorIndexCreator(String column, File indexDir, VectorIndexConfig config) {
    _column = column;
    _indexDir = indexDir;
    _dimension = config.getVectorDimension();
    _distanceFunction = config.getVectorDistanceFunction();

    Map<String, String> properties = config.getProperties();
    _nlist = properties != null && properties.containsKey("nlist")
        ? Integer.parseInt(properties.get("nlist"))
        : DEFAULT_NLIST;
    _trainSampleSize = properties != null && properties.containsKey("trainSampleSize")
        ? Integer.parseInt(properties.get("trainSampleSize"))
        : Math.max(_nlist * DEFAULT_TRAIN_SAMPLE_MULTIPLIER, DEFAULT_MIN_TRAIN_SAMPLE_SIZE);
    _trainingSeed = properties != null && properties.containsKey("trainingSeed")
        ? Long.parseLong(properties.get("trainingSeed"))
        : System.nanoTime();
    _quantizerType = VectorQuantizationUtils.resolveQuantizerType(properties);

    Preconditions.checkArgument(_dimension > 0, "Vector dimension must be positive, got: %s", _dimension);
    Preconditions.checkArgument(_nlist > 0, "nlist must be positive, got: %s", _nlist);

    LOGGER.info("Creating IVF_FLAT index for column: {} in dir: {}, dimension={}, nlist={}, distance={}, "
            + "quantizer={}",
        column, indexDir.getAbsolutePath(), _dimension, _nlist, _distanceFunction, _quantizerType);
  }

  @Override
  public void add(Object[] values, @Nullable int[] dictIds) {
    // The segment builder calls this overload for multi-value columns.
    // Convert Object[] (boxed Floats) to float[] and delegate to add(float[]).
    Preconditions.checkArgument(values.length == _dimension,
        "Vector dimension mismatch: expected %s, got %s", _dimension, values.length);
    float[] floatValues = new float[_dimension];
    for (int i = 0; i < _dimension; i++) {
      floatValues[i] = (Float) values[i];
    }
    add(floatValues);
  }

  @Override
  public void add(float[] document) {
    Preconditions.checkState(!_sealed, "Cannot add documents after seal()");
    Preconditions.checkArgument(document.length == _dimension,
        "Vector dimension mismatch: expected %s, got %s", _dimension, document.length);
    _vectors.add(document.clone());
  }

  @Override
  public void seal()
      throws IOException {
    Preconditions.checkState(!_sealed, "seal() already called");
    _sealed = true;

    int numVectors = _vectors.size();
    if (numVectors == 0) {
      LOGGER.warn("No vectors to index for column: {}. Writing empty index.", _column);
      VectorQuantizer quantizer =
          VectorQuantizationUtils.createWriteQuantizer(_quantizerType, _dimension, new float[0][]);
      writeIndex(new float[0][0], new int[0], new List[0], 0, quantizer);
      return;
    }

    // Determine effective nlist (cannot have more centroids than vectors)
    int effectiveNlist = Math.min(_nlist, numVectors);
    LOGGER.info("IVF_FLAT seal: column={}, numVectors={}, effectiveNlist={}", _column, numVectors, effectiveNlist);

    // Collect training samples
    float[][] trainingSamples = collectTrainingSamples(numVectors, effectiveNlist);
    VectorQuantizer quantizer =
        VectorQuantizationUtils.createWriteQuantizer(_quantizerType, _dimension, trainingSamples);

    // Train centroids using k-means
    float[][] centroids = trainKMeans(trainingSamples, effectiveNlist);

    // Assign all vectors to their nearest centroids
    int[] assignments = assignVectors(centroids);

    // Build inverted lists
    @SuppressWarnings("unchecked")
    List<Integer>[] invertedLists = new List[effectiveNlist];
    for (int i = 0; i < effectiveNlist; i++) {
      invertedLists[i] = new ArrayList<>();
    }
    for (int docId = 0; docId < numVectors; docId++) {
      invertedLists[assignments[docId]].add(docId);
    }

    // Write the index file
    writeIndex(centroids, assignments, invertedLists, effectiveNlist, quantizer);

    LOGGER.info("IVF_FLAT index sealed for column: {}. {} vectors across {} centroids. quantizer={}",
        _column, numVectors, effectiveNlist, _quantizerType);
  }

  @Override
  public void close()
      throws IOException {
    // Release references to allow GC
    _vectors.clear();
  }

  // -----------------------------------------------------------------------
  // Training
  // -----------------------------------------------------------------------

  /**
   * Collects a subsample of vectors for k-means training.
   */
  float[][] collectTrainingSamples(int numVectors, int effectiveNlist) {
    int sampleSize = Math.min(_trainSampleSize, numVectors);
    if (sampleSize >= numVectors) {
      // Use all vectors for training
      return _vectors.toArray(new float[0][]);
    }

    Random rng = new Random(_trainingSeed);
    // Fisher-Yates partial shuffle to select sampleSize unique indices
    int[] indices = new int[numVectors];
    for (int i = 0; i < numVectors; i++) {
      indices[i] = i;
    }
    for (int i = 0; i < sampleSize; i++) {
      int j = i + rng.nextInt(numVectors - i);
      int tmp = indices[i];
      indices[i] = indices[j];
      indices[j] = tmp;
    }

    float[][] samples = new float[sampleSize][];
    for (int i = 0; i < sampleSize; i++) {
      samples[i] = _vectors.get(indices[i]);
    }
    return samples;
  }

  /**
   * Trains centroids using k-means++ initialization followed by Lloyd's algorithm.
   *
   * @param samples       the training vectors
   * @param numCentroids  the number of centroids to train
   * @return the trained centroids
   */
  float[][] trainKMeans(float[][] samples, int numCentroids) {
    int numSamples = samples.length;
    if (numCentroids >= numSamples) {
      // Use each sample as its own centroid
      float[][] centroids = new float[numSamples][];
      for (int i = 0; i < numSamples; i++) {
        centroids[i] = samples[i].clone();
      }
      return centroids;
    }

    // k-means++ initialization
    float[][] centroids = kMeansPlusPlusInit(samples, numCentroids);

    // Lloyd's iterations
    int[] assignments = new int[numSamples];
    for (int iter = 0; iter < MAX_KMEANS_ITERATIONS; iter++) {
      // Assign each sample to the nearest centroid
      for (int i = 0; i < numSamples; i++) {
        assignments[i] = findNearestCentroid(samples[i], centroids);
      }

      // Recompute centroids
      float[][] newCentroids = new float[numCentroids][_dimension];
      int[] counts = new int[numCentroids];
      for (int i = 0; i < numSamples; i++) {
        int cluster = assignments[i];
        counts[cluster]++;
        for (int d = 0; d < _dimension; d++) {
          newCentroids[cluster][d] += samples[i][d];
        }
      }

      // Finalize centroids (divide by count), handle empty clusters
      float maxMovement = 0.0f;
      for (int c = 0; c < numCentroids; c++) {
        if (counts[c] == 0) {
          // Empty cluster: keep old centroid
          newCentroids[c] = centroids[c].clone();
        } else {
          for (int d = 0; d < _dimension; d++) {
            newCentroids[c][d] /= counts[c];
          }
        }
        // Track maximum centroid movement for convergence check
        float movement = (float) VectorFunctions.euclideanDistance(centroids[c], newCentroids[c]);
        maxMovement = Math.max(maxMovement, movement);
      }

      centroids = newCentroids;

      if (maxMovement < CONVERGENCE_THRESHOLD) {
        LOGGER.debug("K-means converged at iteration {} with maxMovement={}", iter, maxMovement);
        break;
      }
    }

    return centroids;
  }

  /**
   * K-means++ initialization: selects initial centroids with probability proportional
   * to the squared distance from the nearest existing centroid.
   */
  private float[][] kMeansPlusPlusInit(float[][] samples, int numCentroids) {
    int numSamples = samples.length;
    Random rng = new Random(_trainingSeed);

    float[][] centroids = new float[numCentroids][];
    // Pick first centroid uniformly at random
    centroids[0] = samples[rng.nextInt(numSamples)].clone();

    // Distances from each sample to the nearest chosen centroid
    float[] minDistances = new float[numSamples];
    Arrays.fill(minDistances, Float.MAX_VALUE);

    for (int c = 1; c < numCentroids; c++) {
      // Update minimum distances with the most recently added centroid
      float totalWeight = 0.0f;
      for (int i = 0; i < numSamples; i++) {
        float dist = computeTrainingDistance(samples[i], centroids[c - 1]);
        if (dist < minDistances[i]) {
          minDistances[i] = dist;
        }
        totalWeight += minDistances[i];
      }

      // Weighted random selection
      float target = rng.nextFloat() * totalWeight;
      float cumulative = 0.0f;
      int selected = numSamples - 1; // fallback
      for (int i = 0; i < numSamples; i++) {
        cumulative += minDistances[i];
        if (cumulative >= target) {
          selected = i;
          break;
        }
      }
      centroids[c] = samples[selected].clone();
    }

    return centroids;
  }

  /**
   * Computes distance used for training. Always uses L2 squared distance for k-means
   * training regardless of the configured distance function, because k-means minimizes
   * squared Euclidean distance by construction.
   *
   * <p>For COSINE distance, we normalize vectors before computing L2, which is equivalent
   * to using angular distance for clustering.</p>
   */
  private float computeTrainingDistance(float[] a, float[] b) {
    // For cosine distance, use L2 on normalized vectors which groups by angular similarity
    if (_distanceFunction == VectorIndexConfig.VectorDistanceFunction.COSINE) {
      return (float) VectorFunctions.euclideanDistance(normalizeVector(a), normalizeVector(b));
    }
    return (float) VectorFunctions.euclideanDistance(a, b);
  }

  // -----------------------------------------------------------------------
  // Assignment
  // -----------------------------------------------------------------------

  /**
   * Assigns each vector to its nearest centroid using the configured distance function.
   */
  private int[] assignVectors(float[][] centroids) {
    int numVectors = _vectors.size();
    int[] assignments = new int[numVectors];
    for (int i = 0; i < numVectors; i++) {
      assignments[i] = findNearestCentroidForSearch(_vectors.get(i), centroids);
    }
    return assignments;
  }

  /**
   * Finds the index of the nearest centroid to the given vector using L2 distance
   * (used during k-means training).
   */
  private int findNearestCentroid(float[] vector, float[][] centroids) {
    int nearest = 0;
    float nearestDist = Float.MAX_VALUE;
    for (int c = 0; c < centroids.length; c++) {
      float dist = computeTrainingDistance(vector, centroids[c]);
      if (dist < nearestDist) {
        nearestDist = dist;
        nearest = c;
      }
    }
    return nearest;
  }

  /**
   * Finds the index of the nearest centroid to the given vector using the configured
   * distance function (used during vector assignment after training).
   */
  private int findNearestCentroidForSearch(float[] vector, float[][] centroids) {
    int nearest = 0;
    float nearestDist = Float.MAX_VALUE;
    for (int c = 0; c < centroids.length; c++) {
      float dist = computeDistance(vector, centroids[c]);
      if (dist < nearestDist) {
        nearestDist = dist;
        nearest = c;
      }
    }
    return nearest;
  }

  // -----------------------------------------------------------------------
  // Distance computation helpers (delegates to VectorFunctions)
  // -----------------------------------------------------------------------

  /**
   * Computes distance between two vectors using the configured distance function.
   * Internally uses L2 for EUCLIDEAN/L2, cosine for COSINE, negative dot for INNER_PRODUCT/DOT_PRODUCT.
   */
  private float computeDistance(float[] a, float[] b) {
    switch (_distanceFunction) {
      case EUCLIDEAN:
      case L2:
        return (float) VectorFunctions.euclideanDistance(a, b);
      case COSINE:
        return (float) VectorFunctions.cosineDistance(a, b);
      case INNER_PRODUCT:
      case DOT_PRODUCT:
        return (float) -VectorFunctions.dotProduct(a, b);
      default:
        throw new IllegalArgumentException("Unsupported distance function: " + _distanceFunction);
    }
  }

  /**
   * Returns a new unit-length copy of the given vector.
   * If the vector has zero magnitude, a zero vector of the same length is returned.
   */
  private static float[] normalizeVector(float[] vector) {
    float norm = 0.0f;
    for (float v : vector) {
      norm += v * v;
    }
    norm = (float) Math.sqrt(norm);
    float[] result = new float[vector.length];
    if (norm > 0.0f) {
      for (int i = 0; i < vector.length; i++) {
        result[i] = vector[i] / norm;
      }
    }
    return result;
  }

  // -----------------------------------------------------------------------
  // Serialization
  // -----------------------------------------------------------------------

  /**
   * Writes the complete IVF_FLAT index to disk.
   */
  private void writeIndex(float[][] centroids, int[] assignments, List<Integer>[] invertedLists, int effectiveNlist,
      VectorQuantizer quantizer)
      throws IOException {
    File indexFile = new File(_indexDir, _column + V1Constants.Indexes.VECTOR_IVF_FLAT_INDEX_FILE_EXTENSION);
    int numVectors = _vectors.size();
    byte[] quantizerParams = VectorQuantizationUtils.serializeQuantizerParams(quantizer);
    int encodedBytesPerVector = quantizer.getEncodedBytesPerVector();
    Preconditions.checkArgument(encodedBytesPerVector > 0,
        "Encoded bytes per vector must be > 0 for quantizer=%s", quantizer.getType());

    try (DataOutputStream out = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(indexFile)))) {
      // --- Header ---
      out.writeInt(MAGIC);
      out.writeInt(FORMAT_VERSION);
      out.writeInt(_dimension);
      out.writeInt(numVectors);
      out.writeInt(effectiveNlist);
      out.writeInt(_distanceFunction.ordinal());
      out.writeInt(quantizer.getType().ordinal());
      out.writeInt(quantizerParams.length);
      if (quantizerParams.length > 0) {
        out.write(quantizerParams);
      }

      // --- Centroids Section ---
      for (int c = 0; c < effectiveNlist; c++) {
        for (int d = 0; d < _dimension; d++) {
          out.writeFloat(centroids[c][d]);
        }
      }

      // --- Inverted Lists Section ---
      // Track offsets for each inverted list
      // We compute offsets from the start of the file
      // Header size:
      // base 6 ints + quantizerType + quantizerParamsLen + quantizerParams bytes
      // Centroids size: effectiveNlist * dimension * 4 bytes
      long currentOffset = 32L + quantizerParams.length + (long) effectiveNlist * _dimension * 4;
      long[] listOffsets = new long[effectiveNlist];

      for (int c = 0; c < effectiveNlist; c++) {
        listOffsets[c] = currentOffset;
        List<Integer> list = invertedLists[c];
        int listSize = list.size();

        out.writeInt(listSize);
        currentOffset += 4; // listSize

        // Write doc IDs
        for (int docId : list) {
          out.writeInt(docId);
        }
        currentOffset += (long) listSize * 4;

        // Write quantized vectors for this list
        for (int docId : list) {
          float[] vector = _vectors.get(docId);
          out.write(quantizer.encode(vector));
        }
        currentOffset += (long) listSize * encodedBytesPerVector;
      }

      // --- Inverted List Offsets ---
      long offsetToOffsets = currentOffset;
      for (int c = 0; c < effectiveNlist; c++) {
        out.writeLong(listOffsets[c]);
      }

      // --- Footer ---
      out.writeLong(offsetToOffsets);
    }

    LOGGER.info("IVF_FLAT index file written: {} ({} bytes)", indexFile.getAbsolutePath(), indexFile.length());
  }

  // -----------------------------------------------------------------------
  // Package-private accessors for testing
  // -----------------------------------------------------------------------

  int getNumVectors() {
    return _vectors.size();
  }

  int getDimension() {
    return _dimension;
  }

  int getNlist() {
    return _nlist;
  }

  VectorIndexConfig.VectorDistanceFunction getDistanceFunction() {
    return _distanceFunction;
  }
}
