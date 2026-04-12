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
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import org.apache.pinot.segment.spi.index.creator.VectorIndexConfig;
import org.apache.pinot.segment.spi.index.creator.VectorQuantizerType;
import org.apache.pinot.segment.spi.index.reader.VectorQuantizer;


/**
 * Identity (no-op) quantizer that stores raw float32 vectors without any compression.
 *
 * <p>This is the trivial quantizer: {@link #encode} serializes each float as 4 bytes in
 * little-endian order, and {@link #decode} deserializes them back. Distance computation
 * decodes first and then applies the quantizer distance semantics used across the ANN stack.</p>
 *
 * <p>This class is thread-safe and immutable after construction.</p>
 */
public final class FlatQuantizer implements VectorQuantizer {

  private final int _dimension;
  private final int _encodedBytesPerVector;

  /**
   * Creates a FlatQuantizer for vectors of the given dimension.
   *
   * @param dimension the vector dimension (must be positive)
   * @throws IllegalArgumentException if dimension is not positive
   */
  public FlatQuantizer(int dimension) {
    Preconditions.checkArgument(dimension > 0, "dimension must be positive, got: %s", dimension);
    _dimension = dimension;
    _encodedBytesPerVector = dimension * Float.BYTES;
  }

  @Override
  public VectorQuantizerType getType() {
    return VectorQuantizerType.FLAT;
  }

  @Override
  public int getDimension() {
    return _dimension;
  }

  @Override
  public int getEncodedBytesPerVector() {
    return _encodedBytesPerVector;
  }

  @Override
  public byte[] encode(float[] vector) {
    Preconditions.checkArgument(vector.length == _dimension,
        "Expected vector of dimension %s, got %s", _dimension, vector.length);
    ByteBuffer buffer = ByteBuffer.allocate(_encodedBytesPerVector).order(ByteOrder.LITTLE_ENDIAN);
    for (float v : vector) {
      buffer.putFloat(v);
    }
    return buffer.array();
  }

  @Override
  public float[] decode(byte[] encoded) {
    Preconditions.checkArgument(encoded.length == _encodedBytesPerVector,
        "Expected %s bytes, got %s", _encodedBytesPerVector, encoded.length);
    ByteBuffer buffer = ByteBuffer.wrap(encoded).order(ByteOrder.LITTLE_ENDIAN);
    float[] vector = new float[_dimension];
    for (int i = 0; i < _dimension; i++) {
      vector[i] = buffer.getFloat();
    }
    return vector;
  }

  @Override
  public float computeDistance(float[] query, byte[] encodedDoc,
      VectorIndexConfig.VectorDistanceFunction distanceFunction) {
    Preconditions.checkArgument(query.length == _dimension,
        "Expected query dimension %s, got %s", _dimension, query.length);
    Preconditions.checkArgument(encodedDoc.length == _encodedBytesPerVector,
        "Expected %s bytes, got %s", _encodedBytesPerVector, encodedDoc.length);

    switch (distanceFunction) {
      case EUCLIDEAN:
      case L2:
        return (float) computeEuclideanDistance(query, encodedDoc);
      case COSINE:
        return (float) computeCosineDistance(query, encodedDoc, 1.0d);
      case INNER_PRODUCT:
      case DOT_PRODUCT:
        return (float) computeNegativeDotProduct(query, encodedDoc);
      default:
        throw new IllegalArgumentException("Unsupported distance function: " + distanceFunction);
    }
  }

  private double computeEuclideanDistance(float[] query, byte[] encodedDoc) {
    double distance = 0.0d;
    for (int i = 0, offset = 0; i < _dimension; i++, offset += Float.BYTES) {
      float docValue = decodeLittleEndianFloat(encodedDoc, offset);
      double diff = query[i] - docValue;
      distance += diff * diff;
    }
    return distance;
  }

  private double computeCosineDistance(float[] query, byte[] encodedDoc, double defaultValue) {
    double dotProduct = 0.0d;
    double queryNorm = 0.0d;
    double docNorm = 0.0d;
    for (int i = 0, offset = 0; i < _dimension; i++, offset += Float.BYTES) {
      float docValue = decodeLittleEndianFloat(encodedDoc, offset);
      dotProduct += query[i] * docValue;
      queryNorm += query[i] * query[i];
      docNorm += docValue * docValue;
    }
    if (queryNorm == 0.0d || docNorm == 0.0d) {
      // Quantizer code paths use a finite cosine fallback to keep candidate ordering stable.
      return defaultValue;
    }
    return 1.0d - (dotProduct / (Math.sqrt(queryNorm) * Math.sqrt(docNorm)));
  }

  private double computeNegativeDotProduct(float[] query, byte[] encodedDoc) {
    double dotProduct = 0.0d;
    for (int i = 0, offset = 0; i < _dimension; i++, offset += Float.BYTES) {
      dotProduct += query[i] * decodeLittleEndianFloat(encodedDoc, offset);
    }
    return -dotProduct;
  }

  private static float decodeLittleEndianFloat(byte[] encodedDoc, int offset) {
    int bits = (encodedDoc[offset] & 0xFF)
        | ((encodedDoc[offset + 1] & 0xFF) << 8)
        | ((encodedDoc[offset + 2] & 0xFF) << 16)
        | ((encodedDoc[offset + 3] & 0xFF) << 24);
    return Float.intBitsToFloat(bits);
  }
}
