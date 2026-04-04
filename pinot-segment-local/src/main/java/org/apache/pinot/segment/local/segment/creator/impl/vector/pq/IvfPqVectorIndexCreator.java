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
package org.apache.pinot.segment.local.segment.creator.impl.vector.pq;

import com.google.common.base.Preconditions;
import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import javax.annotation.Nullable;
import org.apache.pinot.segment.spi.index.creator.VectorIndexConfig;
import org.apache.pinot.segment.spi.index.creator.VectorIndexCreator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Creates an IVF_PQ vector index for immutable/offline segments.
 *
 * <p>Build flow:
 * <ol>
 *   <li>Collect all vectors via {@link #add(float[])}</li>
 *   <li>On {@link #seal()}: sample training vectors, train IVF centroids, train PQ codebooks,
 *       assign all vectors to clusters, encode into PQ codes, and write the index file</li>
 * </ol>
 *
 * <p>This creator buffers all vectors in memory during segment creation, which is consistent
 * with how immutable segments are built (the full column data is available).
 */
public class IvfPqVectorIndexCreator implements VectorIndexCreator {
  private static final Logger LOGGER = LoggerFactory.getLogger(IvfPqVectorIndexCreator.class);

  public static final String FILE_EXTENSION = ".vector.ivfpq.index";

  private final String _column;
  private final File _indexFile;
  private final int _dimension;
  private final int _nlist;
  private final int _pqM;
  private final int _pqNbits;
  private final int _trainSampleSize;
  private final long _trainingSeed;
  private final int _distanceFunctionCode;

  // Accumulate vectors during creation
  private final List<float[]> _vectors = new ArrayList<>();
  private final List<Integer> _docIds = new ArrayList<>();
  private int _nextDocId = 0;

  public IvfPqVectorIndexCreator(String column, File segmentIndexDir, VectorIndexConfig config) {
    _column = column;
    _indexFile = new File(segmentIndexDir, column + FILE_EXTENSION);
    _dimension = config.getVectorDimension();

    Map<String, String> props = config.getProperties();
    _nlist = Integer.parseInt(props.getOrDefault("nlist", "256"));
    _pqM = Integer.parseInt(props.getOrDefault("pqM", "32"));
    _pqNbits = Integer.parseInt(props.getOrDefault("pqNbits", "8"));
    _trainSampleSize = Integer.parseInt(props.getOrDefault("trainSampleSize", "65536"));
    _trainingSeed = Long.parseLong(props.getOrDefault("trainingSeed", "42"));

    _distanceFunctionCode = distanceFunctionToCode(config.getVectorDistanceFunction());

    Preconditions.checkArgument(_dimension % _pqM == 0,
        "pqM (%s) must divide vectorDimension (%s) evenly", _pqM, _dimension);

    LOGGER.info("Creating IVF_PQ index for column: {} at path: {} with nlist={}, pqM={}, pqNbits={}, "
            + "trainSampleSize={}, dimension={}", column, _indexFile.getAbsolutePath(), _nlist, _pqM, _pqNbits,
        _trainSampleSize, _dimension);
  }

  @Override
  public void add(Object[] values, @Nullable int[] dictIds) {
    float[] floatValues = new float[_dimension];
    for (int i = 0; i < values.length; i++) {
      floatValues[i] = (Float) values[i];
    }
    add(floatValues);
  }

  @Override
  public void add(float[] document) {
    // Clone the input array because callers (e.g., VectorIndexHandler) may reuse
    // the same buffer across rows. Without cloning, all stored references would
    // point to the last row's values, corrupting the index.
    _vectors.add(document.clone());
    _docIds.add(_nextDocId++);
  }

  @Override
  public void seal()
      throws IOException {
    int numVectors = _vectors.size();
    LOGGER.info("Sealing IVF_PQ index for column: {} with {} vectors", _column, numVectors);

    if (numVectors == 0) {
      writeEmptyIndex();
      return;
    }

    Random random = new Random(_trainingSeed);

    // Step 0: For COSINE and INNER_PRODUCT distance, normalize all vectors before training/indexing.
    // L2 distance on unit-normalized vectors preserves ranking for both cosine and inner-product,
    // so L2-based centroid probing and PQ ADC tables produce correct ranking.
    boolean needsNormalization = _distanceFunctionCode == IvfPqIndexFormat.DIST_COSINE
        || _distanceFunctionCode == IvfPqIndexFormat.DIST_INNER_PRODUCT;
    float[][] indexVectors = new float[numVectors][];
    for (int i = 0; i < numVectors; i++) {
      indexVectors[i] = needsNormalization ? VectorDistanceUtil.normalize(_vectors.get(i)) : _vectors.get(i);
    }

    // Step 1: Sample training vectors
    float[][] trainingSample = sampleTrainingVectors(indexVectors, random);
    LOGGER.info("IVF_PQ training sample size: {}", trainingSample.length);

    // Step 2: Train coarse IVF centroids
    int effectiveNlist = Math.min(_nlist, numVectors);
    float[][] coarseCentroids = KMeans.train(trainingSample, _dimension, effectiveNlist, random);
    LOGGER.info("Trained {} coarse centroids", coarseCentroids.length);

    // Step 3: Assign all vectors to coarse centroids and compute residuals
    int[] assignments = new int[numVectors];
    float[][] residuals = new float[numVectors][_dimension];
    for (int i = 0; i < numVectors; i++) {
      float[] vec = indexVectors[i];
      assignments[i] = KMeans.findNearest(vec, coarseCentroids, _dimension);
      float[] centroid = coarseCentroids[assignments[i]];
      for (int d = 0; d < _dimension; d++) {
        residuals[i][d] = vec[d] - centroid[d];
      }
    }

    // Step 4: Sample residuals for PQ training
    float[][] pqTrainingSample = sampleFromArray(residuals, Math.min(_trainSampleSize, numVectors), random);

    // Step 5: Train PQ codebooks on residuals
    ProductQuantizer pq = new ProductQuantizer(_dimension, _pqM, _pqNbits);
    pq.train(pqTrainingSample, random);
    LOGGER.info("Trained PQ codebooks with M={}, nbits={}", _pqM, _pqNbits);

    // Step 6: Encode all residuals into PQ codes
    byte[][] allCodes = new byte[numVectors][];
    for (int i = 0; i < numVectors; i++) {
      allCodes[i] = pq.encode(residuals[i]);
    }

    // Step 7: Build inverted lists
    @SuppressWarnings("unchecked")
    List<Integer>[] invertedLists = new List[effectiveNlist];
    for (int i = 0; i < effectiveNlist; i++) {
      invertedLists[i] = new ArrayList<>();
    }
    for (int i = 0; i < numVectors; i++) {
      invertedLists[assignments[i]].add(i);
    }

    // Step 8: Write index file
    writeIndex(coarseCentroids, pq, invertedLists, allCodes, numVectors, effectiveNlist);
    LOGGER.info("IVF_PQ index written for column: {} ({} bytes)", _column, _indexFile.length());
  }

  private void writeIndex(float[][] coarseCentroids, ProductQuantizer pq, List<Integer>[] invertedLists,
      byte[][] allCodes, int numVectors, int effectiveNlist)
      throws IOException {
    try (DataOutputStream dos = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(_indexFile)))) {
      // Write header
      writeLittleEndianInt(dos, IvfPqIndexFormat.MAGIC);
      writeLittleEndianInt(dos, IvfPqIndexFormat.VERSION);
      writeLittleEndianInt(dos, _dimension);
      writeLittleEndianInt(dos, effectiveNlist);
      writeLittleEndianInt(dos, _pqM);
      writeLittleEndianInt(dos, _pqNbits);
      writeLittleEndianInt(dos, _distanceFunctionCode);
      writeLittleEndianInt(dos, numVectors);

      // Write coarse centroids
      for (int c = 0; c < effectiveNlist; c++) {
        for (int d = 0; d < _dimension; d++) {
          writeLittleEndianFloat(dos, coarseCentroids[c][d]);
        }
      }

      // Write PQ codebooks
      float[][][] codebooks = pq.getCodebooks();
      int ksub = pq.getKsub();
      int dsub = pq.getDsub();
      for (int sub = 0; sub < _pqM; sub++) {
        for (int code = 0; code < ksub; code++) {
          for (int d = 0; d < dsub; d++) {
            writeLittleEndianFloat(dos, codebooks[sub][code][d]);
          }
        }
      }

      // Write inverted lists
      for (int list = 0; list < effectiveNlist; list++) {
        List<Integer> members = invertedLists[list];
        int listSize = members.size();
        writeLittleEndianInt(dos, listSize);

        // Write doc IDs
        for (int idx : members) {
          writeLittleEndianInt(dos, _docIds.get(idx));
        }

        // Write PQ codes
        for (int idx : members) {
          dos.write(allCodes[idx]);
        }
      }
    }
  }

  private void writeEmptyIndex()
      throws IOException {
    try (DataOutputStream dos = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(_indexFile)))) {
      writeLittleEndianInt(dos, IvfPqIndexFormat.MAGIC);
      writeLittleEndianInt(dos, IvfPqIndexFormat.VERSION);
      writeLittleEndianInt(dos, _dimension);
      writeLittleEndianInt(dos, 0); // nlist
      writeLittleEndianInt(dos, _pqM);
      writeLittleEndianInt(dos, _pqNbits);
      writeLittleEndianInt(dos, _distanceFunctionCode);
      writeLittleEndianInt(dos, 0); // numVectors
    }
  }

  private float[][] sampleTrainingVectors(float[][] vectors, Random random) {
    int numVectors = vectors.length;
    int sampleSize = Math.min(_trainSampleSize, numVectors);
    if (sampleSize == numVectors) {
      return vectors;
    }

    // Fisher-Yates shuffle on indices, take first sampleSize
    int[] indices = new int[numVectors];
    for (int i = 0; i < numVectors; i++) {
      indices[i] = i;
    }
    for (int i = 0; i < sampleSize; i++) {
      int j = i + random.nextInt(numVectors - i);
      int tmp = indices[i];
      indices[i] = indices[j];
      indices[j] = tmp;
    }

    float[][] sample = new float[sampleSize][];
    for (int i = 0; i < sampleSize; i++) {
      sample[i] = vectors[indices[i]];
    }
    return sample;
  }

  private static float[][] sampleFromArray(float[][] vectors, int sampleSize, Random random) {
    int n = vectors.length;
    if (sampleSize >= n) {
      return vectors;
    }
    int[] indices = new int[n];
    for (int i = 0; i < n; i++) {
      indices[i] = i;
    }
    for (int i = 0; i < sampleSize; i++) {
      int j = i + random.nextInt(n - i);
      int tmp = indices[i];
      indices[i] = indices[j];
      indices[j] = tmp;
    }
    float[][] sample = new float[sampleSize][];
    for (int i = 0; i < sampleSize; i++) {
      sample[i] = vectors[indices[i]];
    }
    return sample;
  }

  private static int distanceFunctionToCode(VectorIndexConfig.VectorDistanceFunction df) {
    if (df == null) {
      return IvfPqIndexFormat.DIST_COSINE;
    }
    switch (df) {
      case COSINE:
        return IvfPqIndexFormat.DIST_COSINE;
      case EUCLIDEAN:
        return IvfPqIndexFormat.DIST_EUCLIDEAN;
      case INNER_PRODUCT:
      case DOT_PRODUCT:
        return IvfPqIndexFormat.DIST_INNER_PRODUCT;
      default:
        return IvfPqIndexFormat.DIST_COSINE;
    }
  }

  private static void writeLittleEndianInt(DataOutputStream dos, int value)
      throws IOException {
    dos.write(value & 0xFF);
    dos.write((value >> 8) & 0xFF);
    dos.write((value >> 16) & 0xFF);
    dos.write((value >> 24) & 0xFF);
  }

  private static void writeLittleEndianFloat(DataOutputStream dos, float value)
      throws IOException {
    writeLittleEndianInt(dos, Float.floatToIntBits(value));
  }

  @Override
  public void close()
      throws IOException {
    _vectors.clear();
    _docIds.clear();
  }
}
