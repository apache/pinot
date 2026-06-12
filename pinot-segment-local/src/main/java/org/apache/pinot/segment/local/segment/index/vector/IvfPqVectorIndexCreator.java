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
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Map;
import java.util.Random;
import javax.annotation.Nullable;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.segment.spi.index.creator.VectorIndexConfig;
import org.apache.pinot.segment.spi.index.creator.VectorIndexCreator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Creates an IVF_PQ (Inverted File with residual Product Quantization) index for immutable segments.
 *
 * <p>Uses a two-pass design to bound memory usage. During {@link #add}, vectors are spilled to a
 * temporary file while a reservoir sample is kept in memory for training. During {@link #seal},
 * centroids and PQ codebooks are trained from the sample, then all vectors are streamed from
 * the spill file for assignment and encoding.</p>
 *
 * <p>Peak heap usage is O(trainSampleSize * dimension) instead of O(numVectors * dimension).</p>
 *
 * <h3>Thread safety</h3>
 * <p>This class is not thread-safe. It is intended for single-threaded offline segment creation.</p>
 */
public class IvfPqVectorIndexCreator implements VectorIndexCreator {
  private static final Logger LOGGER = LoggerFactory.getLogger(IvfPqVectorIndexCreator.class);

  /** Magic bytes identifying an IVF_PQ index file: ASCII "IVPQ". */
  public static final int MAGIC = IvfPqIndexFormat.MAGIC;

  /** Current file format version. */
  public static final int FORMAT_VERSION = IvfPqIndexFormat.FORMAT_VERSION;

  /** On-disk file extension for the IVF_PQ index. */
  public static final String INDEX_FILE_EXTENSION = IvfPqIndexFormat.INDEX_FILE_EXTENSION;

  private final String _column;
  private final File _indexDir;
  private final int _dimension;
  private final int _nlist;
  private final int _pqM;
  private final int _pqNbits;
  private final int _trainSampleSize;
  private final long _trainingSeed;
  private final VectorIndexConfig.VectorDistanceFunction _distanceFunction;

  // Spill file for raw vectors: sequential float arrays, _dimension floats per vector.
  private final File _spillFile;
  private final DataOutputStream _spillOut;

  // Reservoir sample for training. Kept in memory; bounded by _trainSampleSize.
  private final float[][] _trainingSample;
  private final Random _reservoirRng;
  private int _numVectors;
  private boolean _sealed;
  private boolean _spillOutClosed;

  /**
   * Creates a new IVF_PQ creator.
   *
   * @param column the column name
   * @param indexDir the index directory
   * @param config vector index configuration
   */
  public IvfPqVectorIndexCreator(String column, File indexDir, VectorIndexConfig config)
      throws IOException {
    _column = column;
    _indexDir = indexDir;
    _dimension = config.getVectorDimension();
    _distanceFunction = config.getVectorDistanceFunction();

    Map<String, String> properties =
        Preconditions.checkNotNull(config.getProperties(), "IVF_PQ properties are required");
    _nlist = parseRequiredPositiveInt(properties, "nlist");
    _pqM = parseRequiredPositiveInt(properties, "pqM");
    _pqNbits = parseRequiredPositiveInt(properties, "pqNbits");
    _trainSampleSize = parseRequiredPositiveInt(properties, "trainSampleSize");
    _trainingSeed = parseLong(properties, "trainingSeed", System.nanoTime());

    Preconditions.checkArgument(_dimension > 0, "Vector dimension must be positive, got: %s", _dimension);
    Preconditions.checkArgument(_nlist > 0, "nlist must be positive, got: %s", _nlist);
    Preconditions.checkArgument(_pqM > 0, "pqM must be positive, got: %s", _pqM);
    Preconditions.checkArgument(_pqM <= _dimension, "pqM must be <= dimension, got pqM=%s dimension=%s", _pqM,
        _dimension);
    Preconditions.checkArgument(_dimension % _pqM == 0,
        "IVF_PQ pqM (%s) must evenly divide vectorDimension (%s)", _pqM, _dimension);
    Preconditions.checkArgument(_pqNbits == 4 || _pqNbits == 6 || _pqNbits == 8,
        "IVF_PQ pqNbits must be one of [4, 6, 8], got: %s", _pqNbits);
    Preconditions.checkArgument(_trainSampleSize > 0, "trainSampleSize must be positive, got: %s", _trainSampleSize);
    Preconditions.checkArgument(_trainSampleSize >= _nlist,
        "IVF_PQ trainSampleSize (%s) must be >= nlist (%s)", _trainSampleSize, _nlist);

    _trainingSample = new float[_trainSampleSize][];
    _reservoirRng = new Random(_trainingSeed);
    _numVectors = 0;
    _spillOutClosed = false;

    _spillFile = new File(indexDir, column + INDEX_FILE_EXTENSION + ".spill");
    _spillOut = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(_spillFile), 1 << 16));

    LOGGER.info("Creating IVF_PQ index for column: {} in dir: {}, dimension={}, nlist={}, pqM={}, pqNbits={}, "
            + "distance={}", column, indexDir.getAbsolutePath(), _dimension, _nlist, _pqM, _pqNbits,
        _distanceFunction);
  }

  @Override
  public void add(Object[] values, @Nullable int[] dictIds) {
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

    // Spill to disk
    try {
      for (int d = 0; d < _dimension; d++) {
        _spillOut.writeFloat(document[d]);
      }
    } catch (IOException e) {
      throw new RuntimeException("Failed to spill vector to disk for column: " + _column, e);
    }

    // Reservoir sampling: keep up to _trainSampleSize vectors in memory
    if (_numVectors < _trainSampleSize) {
      _trainingSample[_numVectors] = document.clone();
    } else {
      // Replace with decreasing probability to maintain uniform sampling
      int j = _reservoirRng.nextInt(_numVectors + 1);
      if (j < _trainSampleSize) {
        _trainingSample[j] = document.clone();
      }
    }
    _numVectors++;
  }

  @Override
  public void seal()
      throws IOException {
    Preconditions.checkState(!_sealed, "seal() already called");
    _sealed = true;
    boolean success = false;
    try {
      // Close the spill file for writing
      closeSpillOutput();

      if (_numVectors == 0) {
        int[] subvectorLengths = VectorQuantizationUtils.computeSubvectorLengths(_dimension, _pqM);
        float[][][] emptyCodebooks = new float[_pqM][][];
        for (int m = 0; m < _pqM; m++) {
          emptyCodebooks[m] = new float[0][subvectorLengths[m]];
        }
        IvfPqIndexFormat.write(new File(_indexDir, _column + INDEX_FILE_EXTENSION), _dimension, 0, _pqM, _pqNbits,
            _trainSampleSize, _trainingSeed, _distanceFunction, new float[0][0], emptyCodebooks, subvectorLengths,
            new int[0][], new byte[0][]);
        success = true;
        return;
      }

      int effectiveNlist = Math.min(_nlist, _numVectors);
      int[] subvectorLengths = VectorQuantizationUtils.computeSubvectorLengths(_dimension, _pqM);
      int effectiveSampleSize = Math.min(_trainSampleSize, _numVectors);

      // Phase 1: Train from the in-memory reservoir sample
      float[][] transformedSamples = new float[effectiveSampleSize][];
      for (int i = 0; i < effectiveSampleSize; i++) {
        transformedSamples[i] = VectorQuantizationUtils.transformForDistance(_trainingSample[i], _distanceFunction);
      }

      LOGGER.info("Training IVF_PQ for column: {}: {} vectors, {} training samples, {} centroids",
          _column, _numVectors, effectiveSampleSize, effectiveNlist);

      float[][] centroids = KMeansTrainer.train(transformedSamples, effectiveNlist, _trainingSeed,
          _distanceFunction);

      // Compute residuals from the training sample for PQ codebook training
      float[][] residualSamples = new float[effectiveSampleSize][_dimension];
      for (int i = 0; i < effectiveSampleSize; i++) {
        int centroid =
            VectorQuantizationUtils.findNearestCentroid(transformedSamples[i], centroids, _distanceFunction);
        residualSamples[i] = VectorQuantizationUtils.subtractVectors(transformedSamples[i], centroids[centroid]);
      }

      float[][][] codebooks = ProductQuantizer.train(residualSamples, _dimension, _pqM,
          _pqNbits, _trainingSeed);

      // Release training sample memory before pass 2
      for (int i = 0; i < _trainingSample.length; i++) {
        _trainingSample[i] = null;
      }

      // Phase 2a: Count list assignments so we can allocate primitive storage without per-doc boxing.
      int[] listSizes = new int[effectiveNlist];
      try (DataInputStream spillIn = new DataInputStream(
          new BufferedInputStream(new FileInputStream(_spillFile), 1 << 16))) {
        float[] vector = new float[_dimension];
        for (int docId = 0; docId < _numVectors; docId++) {
          for (int d = 0; d < _dimension; d++) {
            vector[d] = spillIn.readFloat();
          }
          float[] transformed = VectorQuantizationUtils.transformForDistance(vector, _distanceFunction);
          int centroid = VectorQuantizationUtils.findNearestCentroid(transformed, centroids, _distanceFunction);
          listSizes[centroid]++;
        }
      }

      int[][] listDocIds = new int[effectiveNlist][];
      byte[][] listCodes = new byte[effectiveNlist][];
      for (int i = 0; i < effectiveNlist; i++) {
        listDocIds[i] = new int[listSizes[i]];
        listCodes[i] = new byte[listSizes[i] * _pqM];
      }
      int[] listOffsets = new int[effectiveNlist];

      // Phase 2b: Stream through the spill file again, assign and encode into primitive arrays.
      try (DataInputStream spillIn = new DataInputStream(
          new BufferedInputStream(new FileInputStream(_spillFile), 1 << 16))) {
        float[] vector = new float[_dimension];
        for (int docId = 0; docId < _numVectors; docId++) {
          for (int d = 0; d < _dimension; d++) {
            vector[d] = spillIn.readFloat();
          }
          float[] transformed = VectorQuantizationUtils.transformForDistance(vector, _distanceFunction);
          int centroid = VectorQuantizationUtils.findNearestCentroid(transformed, centroids, _distanceFunction);
          float[] residual = VectorQuantizationUtils.subtractVectors(transformed, centroids[centroid]);
          byte[] codes = ProductQuantizer.encode(residual, codebooks, subvectorLengths);
          int listOffset = listOffsets[centroid]++;
          listDocIds[centroid][listOffset] = docId;
          System.arraycopy(codes, 0, listCodes[centroid], listOffset * _pqM, _pqM);
        }
      }

      IvfPqIndexFormat.write(new File(_indexDir, _column + INDEX_FILE_EXTENSION), _dimension, _numVectors, _pqM,
          _pqNbits, _trainSampleSize, _trainingSeed, _distanceFunction, centroids, codebooks, subvectorLengths,
          listDocIds, listCodes);
      LOGGER.info("IVF_PQ index sealed for column: {}. {} vectors across {} centroids.", _column, _numVectors,
          effectiveNlist);
      success = true;
    } finally {
      if (!success) {
        LOGGER.warn("Failed to seal IVF_PQ index for column: {}. Cleaning up spill file: {}", _column, _spillFile);
      }
      FileUtils.deleteQuietly(_spillFile);
    }
  }

  @Override
  public void close()
      throws IOException {
    try {
      closeSpillOutput();
    } finally {
      FileUtils.deleteQuietly(_spillFile);
    }
  }

  private void closeSpillOutput()
      throws IOException {
    if (!_spillOutClosed) {
      _spillOut.close();
      _spillOutClosed = true;
    }
  }

  private static int parseRequiredPositiveInt(Map<String, String> properties, String key) {
    String value = properties.get(key);
    if (value == null) {
      throw new IllegalArgumentException("IVF_PQ property '" + key + "' is required");
    }
    int parsedValue = Integer.parseInt(value);
    Preconditions.checkArgument(parsedValue > 0, "IVF_PQ %s must be positive, got: %s", key, parsedValue);
    return parsedValue;
  }

  private static long parseLong(@Nullable Map<String, String> properties, String key, long defaultValue) {
    if (properties == null || !properties.containsKey(key)) {
      return defaultValue;
    }
    return Long.parseLong(properties.get(key));
  }

  /**
   * Maps a distance function to a stable on-disk ID that is independent of enum ordinal order.
   * This ensures existing index files remain valid if the enum is reordered or extended.
   */
  public static int distanceFunctionToStableId(VectorIndexConfig.VectorDistanceFunction distanceFunction) {
    return IvfPqIndexFormat.distanceFunctionToStableId(distanceFunction);
  }

  /**
   * Maps a stable on-disk ID back to the corresponding distance function.
   */
  public static VectorIndexConfig.VectorDistanceFunction stableIdToDistanceFunction(int id) {
    return IvfPqIndexFormat.stableIdToDistanceFunction(id);
  }

  int getDimension() {
    return _dimension;
  }

  int getNlist() {
    return _nlist;
  }

  int getPqM() {
    return _pqM;
  }

  int getPqNbits() {
    return _pqNbits;
  }

  int getTrainSampleSize() {
    return _trainSampleSize;
  }

  long getTrainingSeed() {
    return _trainingSeed;
  }

  VectorIndexConfig.VectorDistanceFunction getDistanceFunction() {
    return _distanceFunction;
  }
}
