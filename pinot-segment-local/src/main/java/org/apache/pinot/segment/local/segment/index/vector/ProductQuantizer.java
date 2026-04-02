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
import org.apache.pinot.segment.spi.index.creator.VectorIndexConfig;


/**
 * Product quantizer training and encoding helpers for IVF_PQ.
 *
 * <p>This class is stateless and thread-safe.</p>
 */
public final class ProductQuantizer {
  private ProductQuantizer() {
  }

  public static float[][][] train(float[][] residuals, int dimension, int pqM, int pqNbits, long seed) {
    Preconditions.checkArgument(pqM > 0, "pqM must be positive");
    Preconditions.checkArgument(pqNbits > 0, "pqNbits must be positive");
    Preconditions.checkArgument(pqNbits <= 8, "pqNbits must be <= 8 for the byte-coded on-disk format");
    Preconditions.checkArgument(dimension > 0, "dimension must be positive");
    Preconditions.checkArgument(pqM <= dimension, "pqM must be <= dimension");
    Preconditions.checkArgument(residuals.length > 0, "residuals must not be empty");

    int[] lengths = VectorQuantizationUtils.computeSubvectorLengths(dimension, pqM);
    int codebookSize = 1 << pqNbits;
    float[][][] codebooks = new float[pqM][][];
    for (int m = 0; m < pqM; m++) {
      int subDim = lengths[m];
      float[][] subSamples = new float[residuals.length][subDim];
      int offset = 0;
      for (int i = 0; i < m; i++) {
        offset += lengths[i];
      }
      for (int i = 0; i < residuals.length; i++) {
        for (int d = 0; d < subDim; d++) {
          subSamples[i][d] = residuals[i][offset + d];
        }
      }
      codebooks[m] = KMeansTrainer.train(subSamples, codebookSize, seed + (m * 31L) + 17L,
          VectorIndexConfig.VectorDistanceFunction.EUCLIDEAN);
    }
    return codebooks;
  }

  public static byte[] encode(float[] residual, float[][][] codebooks, int[] lengths) {
    Preconditions.checkArgument(codebooks.length == lengths.length,
        "codebooks/lengths size mismatch: codebooks=%s lengths=%s", codebooks.length, lengths.length);
    int totalDimension = 0;
    for (int length : lengths) {
      totalDimension += length;
    }
    Preconditions.checkArgument(residual.length >= totalDimension,
        "Residual dimension mismatch: expected at least %s, got %s", totalDimension, residual.length);

    byte[] codes = new byte[codebooks.length];
    int offset = 0;
    for (int m = 0; m < codebooks.length; m++) {
      int subDim = lengths[m];
      int best = 0;
      float bestDistance = Float.MAX_VALUE;
      for (int code = 0; code < codebooks[m].length; code++) {
        float distance = 0.0f;
        for (int d = 0; d < subDim; d++) {
          float diff = residual[offset + d] - codebooks[m][code][d];
          distance += diff * diff;
        }
        if (distance < bestDistance) {
          bestDistance = distance;
          best = code;
        }
      }
      codes[m] = (byte) best;
      offset += subDim;
    }
    return codes;
  }

  public static float[] decode(byte[] codes, float[][][] codebooks, int[] lengths) {
    int dimension = 0;
    for (int length : lengths) {
      dimension += length;
    }
    float[] residual = new float[dimension];
    int offset = 0;
    for (int m = 0; m < codebooks.length; m++) {
      float[] centroid = codebooks[m][codes[m] & 0xFF];
      int subDim = lengths[m];
      for (int d = 0; d < subDim; d++) {
        residual[offset + d] = centroid[d];
      }
      offset += subDim;
    }
    return residual;
  }

  public static float[][] buildL2DistanceTables(float[] queryResidual, float[][][] codebooks, int[] lengths) {
    float[][] tables = new float[codebooks.length][];
    int offset = 0;
    for (int m = 0; m < codebooks.length; m++) {
      int subDim = lengths[m];
      float[] table = new float[codebooks[m].length];
      for (int code = 0; code < codebooks[m].length; code++) {
        float distance = 0.0f;
        for (int d = 0; d < subDim; d++) {
          float diff = queryResidual[offset + d] - codebooks[m][code][d];
          distance += diff * diff;
        }
        table[code] = distance;
      }
      tables[m] = table;
      offset += subDim;
    }
    return tables;
  }
}
