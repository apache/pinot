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
import java.util.Random;


/**
 * Product Quantizer that splits vectors into M subspaces and quantizes each subspace independently
 * using k-means with 2^nbits centroids per subspace.
 *
 * <p>Thread-safe after training is complete (codebooks are immutable after {@link #train}).
 */
public class ProductQuantizer {
  private final int _dimension;
  private final int _m;
  private final int _nbits;
  private final int _dsub;
  private final int _ksub;

  // Codebooks: _codebooks[subspace][centroid][subDim]
  private float[][][] _codebooks;

  /**
   * Creates a ProductQuantizer.
   *
   * @param dimension full vector dimension
   * @param m number of subspaces (must divide dimension evenly)
   * @param nbits bits per sub-quantizer code (number of centroids = 2^nbits, max 8)
   */
  public ProductQuantizer(int dimension, int m, int nbits) {
    Preconditions.checkArgument(dimension > 0, "Dimension must be positive");
    Preconditions.checkArgument(m > 0 && dimension % m == 0,
        "M (%s) must divide dimension (%s) evenly", m, dimension);
    Preconditions.checkArgument(nbits >= 1 && nbits <= 8,
        "nbits must be between 1 and 8, got %s", nbits);
    _dimension = dimension;
    _m = m;
    _nbits = nbits;
    _dsub = dimension / m;
    _ksub = 1 << nbits;
  }

  /**
   * Train the PQ codebooks from training vectors.
   *
   * @param trainingVectors vectors to train on, each of length {@code dimension}
   * @param random random source for reproducibility
   */
  public void train(float[][] trainingVectors, Random random) {
    Preconditions.checkArgument(trainingVectors.length > 0, "Need at least 1 training vector");
    _codebooks = new float[_m][][];

    for (int sub = 0; sub < _m; sub++) {
      // Extract sub-vectors for this subspace
      float[][] subVectors = extractSubVectors(trainingVectors, sub);
      // Train KMeans on this subspace. KMeans caps k to n if needed,
      // so pad with zero vectors if fewer centroids than ksub are returned.
      float[][] trained = KMeans.train(subVectors, _dsub, _ksub, random);
      if (trained.length < _ksub) {
        float[][] padded = new float[_ksub][_dsub];
        System.arraycopy(trained, 0, padded, 0, trained.length);
        // Remaining entries stay as zero vectors
        _codebooks[sub] = padded;
      } else {
        _codebooks[sub] = trained;
      }
    }
  }

  /**
   * Encode a single vector into PQ codes.
   *
   * @param vector input vector of length {@code dimension}
   * @return PQ codes, one byte per subspace (length M)
   */
  public byte[] encode(float[] vector) {
    Preconditions.checkState(_codebooks != null, "ProductQuantizer must be trained before encoding");
    byte[] codes = new byte[_m];
    for (int sub = 0; sub < _m; sub++) {
      int offset = sub * _dsub;
      float[] subVector = new float[_dsub];
      System.arraycopy(vector, offset, subVector, 0, _dsub);
      int nearest = KMeans.findNearest(subVector, _codebooks[sub], _dsub);
      codes[sub] = (byte) nearest;
    }
    return codes;
  }

  /**
   * Build asymmetric distance lookup tables (ADC) for a query vector.
   * Returns a table of shape [M][ksub] where table[sub][code] = distance(querySub, codebook[sub][code]).
   *
   * @param queryVector the query vector
   * @return lookup table [M][ksub]
   */
  public float[][] buildDistanceTables(float[] queryVector) {
    Preconditions.checkState(_codebooks != null, "ProductQuantizer must be trained before building tables");
    float[][] tables = new float[_m][_ksub];
    for (int sub = 0; sub < _m; sub++) {
      int offset = sub * _dsub;
      for (int code = 0; code < _ksub; code++) {
        float dist = 0;
        for (int d = 0; d < _dsub; d++) {
          float diff = queryVector[offset + d] - _codebooks[sub][code][d];
          dist += diff * diff;
        }
        tables[sub][code] = dist;
      }
    }
    return tables;
  }

  /**
   * Compute approximate L2 squared distance from precomputed lookup tables.
   *
   * @param tables precomputed distance tables from {@link #buildDistanceTables}
   * @param codes PQ codes for the candidate vector
   * @return approximate L2 squared distance
   */
  public float computeDistanceFromTables(float[][] tables, byte[] codes) {
    float dist = 0;
    for (int sub = 0; sub < _m; sub++) {
      dist += tables[sub][Byte.toUnsignedInt(codes[sub])];
    }
    return dist;
  }

  /**
   * Extract sub-vectors for a given subspace from a set of vectors.
   */
  private float[][] extractSubVectors(float[][] vectors, int subspace) {
    int n = vectors.length;
    int offset = subspace * _dsub;
    float[][] subVectors = new float[n][_dsub];
    for (int i = 0; i < n; i++) {
      System.arraycopy(vectors[i], offset, subVectors[i], 0, _dsub);
    }
    return subVectors;
  }

  public int getDimension() {
    return _dimension;
  }

  public int getM() {
    return _m;
  }

  public int getNbits() {
    return _nbits;
  }

  public int getDsub() {
    return _dsub;
  }

  public int getKsub() {
    return _ksub;
  }

  public float[][][] getCodebooks() {
    return _codebooks;
  }

  public void setCodebooks(float[][][] codebooks) {
    _codebooks = codebooks;
  }
}
