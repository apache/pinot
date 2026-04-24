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
import java.util.Arrays;
import org.apache.pinot.segment.spi.index.creator.VectorIndexConfig;
import org.apache.pinot.segment.spi.index.creator.VectorQuantizerType;
import org.apache.pinot.segment.spi.index.reader.VectorQuantizer;


/**
 * Scalar quantizer that compresses float32 vectors to fixed-width integer representations.
 *
 * <p>Supports two modes:</p>
 * <ul>
 *   <li><b>SQ8</b>: Maps each dimension from [min, max] to [0, 255] (1 byte per dim).
 *       Encoded size = dimension bytes.</li>
 *   <li><b>SQ4</b>: Maps each dimension from [min, max] to [0, 15] (4 bits per dim).
 *       Two dimensions are packed into one byte. Encoded size = ceil(dimension / 2) bytes.</li>
 * </ul>
 *
 * <p>The quantizer must be trained on representative vectors before encoding. Training computes
 * per-dimension min/max values which define the quantization grid.</p>
 *
 * <p>Thread safety: after training, the quantizer is immutable and thread-safe for concurrent
 * encode/decode/distance operations.</p>
 */
public class ScalarQuantizer implements VectorQuantizer {

  /** Quantizer bit width. */
  public enum BitWidth {
    /** 8-bit scalar quantization (SQ8). */
    SQ8(8, 255),
    /** 4-bit scalar quantization (SQ4). */
    SQ4(4, 15);

    private final int _bits;
    private final int _maxValue;

    BitWidth(int bits, int maxValue) {
      _bits = bits;
      _maxValue = maxValue;
    }

    public int getBits() {
      return _bits;
    }

    public int getMaxValue() {
      return _maxValue;
    }
  }

  private final int _dimension;
  private final BitWidth _bitWidth;
  private final float[] _minValues;
  private final float[] _maxValues;
  private final float[] _scales; // (max - min) / maxQuantizedValue per dimension
  private final boolean _trained;

  /**
   * Creates an untrained scalar quantizer. Use the static factory method
   * {@link #train(float[][], int, BitWidth)} to create a trained instance.
   *
   * @param dimension vector dimension
   * @param bitWidth quantization bit width (SQ8 or SQ4)
   */
  public ScalarQuantizer(int dimension, BitWidth bitWidth) {
    Preconditions.checkArgument(dimension > 0, "Dimension must be positive, got: %s", dimension);
    _dimension = dimension;
    _bitWidth = bitWidth;
    _minValues = new float[dimension];
    _maxValues = new float[dimension];
    _scales = new float[dimension];
    _trained = false;
    Arrays.fill(_minValues, Float.POSITIVE_INFINITY);
    Arrays.fill(_maxValues, Float.NEGATIVE_INFINITY);
  }

  /**
   * Creates a pre-trained scalar quantizer from saved min/max values.
   *
   * @param dimension vector dimension
   * @param bitWidth quantization bit width
   * @param minValues per-dimension minimum values
   * @param maxValues per-dimension maximum values
   */
  public ScalarQuantizer(int dimension, BitWidth bitWidth, float[] minValues, float[] maxValues) {
    Preconditions.checkArgument(dimension > 0, "Dimension must be positive, got: %s", dimension);
    Preconditions.checkArgument(minValues.length == dimension,
        "minValues length mismatch: expected %s, got %s", dimension, minValues.length);
    Preconditions.checkArgument(maxValues.length == dimension,
        "maxValues length mismatch: expected %s, got %s", dimension, maxValues.length);
    _dimension = dimension;
    _bitWidth = bitWidth;
    _minValues = minValues.clone();
    _maxValues = maxValues.clone();
    _scales = new float[dimension];
    computeScales();
    _trained = true;
  }

  /**
   * Trains the quantizer on a set of representative vectors.
   * Computes per-dimension min and max values.
   *
   * @param vectors training vectors (each must have length == dimension)
   * @return a new trained ScalarQuantizer
   */
  public static ScalarQuantizer train(float[][] vectors, int dimension, BitWidth bitWidth) {
    Preconditions.checkArgument(vectors.length > 0, "Training vectors must not be empty");

    float[] minValues = new float[dimension];
    float[] maxValues = new float[dimension];
    Arrays.fill(minValues, Float.POSITIVE_INFINITY);
    Arrays.fill(maxValues, Float.NEGATIVE_INFINITY);

    for (float[] vector : vectors) {
      Preconditions.checkArgument(vector.length == dimension,
          "Training vector dimension mismatch: expected %s, got %s", dimension, vector.length);
      for (int d = 0; d < dimension; d++) {
        if (vector[d] < minValues[d]) {
          minValues[d] = vector[d];
        }
        if (vector[d] > maxValues[d]) {
          maxValues[d] = vector[d];
        }
      }
    }

    // Add small epsilon to avoid zero range
    for (int d = 0; d < dimension; d++) {
      if (minValues[d] == maxValues[d]) {
        maxValues[d] = minValues[d] + 1e-7f;
      }
    }

    return new ScalarQuantizer(dimension, bitWidth, minValues, maxValues);
  }

  /**
   * Encodes a float32 vector to quantized bytes.
   *
   * @param vector input vector (length must equal dimension)
   * @return encoded bytes
   */
  public byte[] encode(float[] vector) {
    Preconditions.checkState(_trained, "Quantizer must be trained before encoding");
    Preconditions.checkArgument(vector.length == _dimension,
        "Vector dimension mismatch: expected %s, got %s", _dimension, vector.length);

    if (_bitWidth == BitWidth.SQ8) {
      return encodeSq8(vector);
    } else {
      return encodeSq4(vector);
    }
  }

  /**
   * Decodes quantized bytes back to an approximate float32 vector.
   *
   * @param encoded encoded bytes
   * @return decoded approximate vector
   */
  public float[] decode(byte[] encoded) {
    Preconditions.checkState(_trained, "Quantizer must be trained before decoding");

    if (_bitWidth == BitWidth.SQ8) {
      return decodeSq8(encoded);
    } else {
      return decodeSq4(encoded);
    }
  }

  /**
   * Computes approximate distance between a query vector and an encoded document vector.
   *
   * @param query the query vector (float32)
   * @param encodedDoc the encoded document vector
   * @param distanceFunction the distance function to use
   * @return approximate distance
   */
  public float computeDistance(float[] query, byte[] encodedDoc,
      VectorIndexConfig.VectorDistanceFunction distanceFunction) {
    Preconditions.checkState(_trained, "Quantizer must be trained before computing distance");
    Preconditions.checkArgument(query.length == _dimension,
        "Query dimension mismatch: expected %s, got %s", _dimension, query.length);
    switch (distanceFunction) {
      case EUCLIDEAN:
      case L2:
        return (float) computeEuclideanDistance(query, encodedDoc);
      case COSINE:
        return (float) computeCosineDistance(query, encodedDoc);
      case INNER_PRODUCT:
      case DOT_PRODUCT:
        return (float) computeNegativeDotProduct(query, encodedDoc);
      default:
        throw new IllegalArgumentException("Unsupported distance function: " + distanceFunction);
    }
  }

  /**
   * Returns the number of bytes per encoded vector.
   */
  public int getEncodedBytesPerVector() {
    if (_bitWidth == BitWidth.SQ8) {
      return _dimension;
    } else {
      return (_dimension + 1) / 2;
    }
  }

  /**
   * Serializes the quantizer parameters (min/max values) to bytes for storage.
   *
   * @return serialized bytes: [dimension(4)] [bitWidth ordinal(4)] [minValues(dim*4)] [maxValues(dim*4)]
   */
  public byte[] serialize() {
    Preconditions.checkState(_trained, "Quantizer must be trained before serializing");
    int size = 4 + 4 + _dimension * 4 * 2;
    ByteBuffer buffer = ByteBuffer.allocate(size);
    buffer.putInt(_dimension);
    buffer.putInt(_bitWidth.ordinal());
    for (int d = 0; d < _dimension; d++) {
      buffer.putFloat(_minValues[d]);
    }
    for (int d = 0; d < _dimension; d++) {
      buffer.putFloat(_maxValues[d]);
    }
    return buffer.array();
  }

  /** Maximum supported dimension to prevent unreasonable memory allocation during deserialization. */
  private static final int MAX_DIMENSION = 65536;

  /**
   * Deserializes a quantizer from saved bytes.
   *
   * @param data serialized bytes
   * @return the deserialized ScalarQuantizer
   * @throws IllegalArgumentException if the data is malformed or contains out-of-range values
   */
  public static ScalarQuantizer deserialize(byte[] data) {
    Preconditions.checkArgument(data != null && data.length >= 8,
        "Serialized data must be at least 8 bytes (header), got: %s",
        data == null ? "null" : data.length);

    ByteBuffer buffer = ByteBuffer.wrap(data);
    int dimension = buffer.getInt();
    Preconditions.checkArgument(dimension > 0 && dimension <= MAX_DIMENSION,
        "Deserialized dimension must be in [1, %s], got: %s", MAX_DIMENSION, dimension);

    int bitWidthOrdinal = buffer.getInt();
    BitWidth[] allBitWidths = BitWidth.values();
    Preconditions.checkArgument(bitWidthOrdinal >= 0 && bitWidthOrdinal < allBitWidths.length,
        "Invalid bitWidth ordinal: %s, must be in [0, %s)", bitWidthOrdinal, allBitWidths.length);
    BitWidth bitWidth = allBitWidths[bitWidthOrdinal];

    int expectedBytes = 8 + dimension * Float.BYTES * 2;
    Preconditions.checkArgument(data.length >= expectedBytes,
        "Serialized data too short: expected at least %s bytes for dimension=%s, got %s",
        expectedBytes, dimension, data.length);

    float[] minValues = new float[dimension];
    float[] maxValues = new float[dimension];
    for (int d = 0; d < dimension; d++) {
      minValues[d] = buffer.getFloat();
    }
    for (int d = 0; d < dimension; d++) {
      maxValues[d] = buffer.getFloat();
    }
    return new ScalarQuantizer(dimension, bitWidth, minValues, maxValues);
  }

  // -----------------------------------------------------------------------
  // SQ8 encoding/decoding
  // -----------------------------------------------------------------------

  private byte[] encodeSq8(float[] vector) {
    byte[] encoded = new byte[_dimension];
    for (int d = 0; d < _dimension; d++) {
      float normalized = (vector[d] - _minValues[d]) * _scales[d];
      int quantized = Math.round(normalized);
      quantized = Math.max(0, Math.min(255, quantized));
      encoded[d] = (byte) quantized;
    }
    return encoded;
  }

  private float[] decodeSq8(byte[] encoded) {
    Preconditions.checkArgument(encoded.length == _dimension,
        "Encoded length mismatch: expected %s, got %s", _dimension, encoded.length);
    float[] decoded = new float[_dimension];
    for (int d = 0; d < _dimension; d++) {
      int quantized = encoded[d] & 0xFF;
      decoded[d] = _minValues[d] + quantized / _scales[d];
    }
    return decoded;
  }

  // -----------------------------------------------------------------------
  // SQ4 encoding/decoding
  // -----------------------------------------------------------------------

  private byte[] encodeSq4(float[] vector) {
    int encodedLen = (_dimension + 1) / 2;
    byte[] encoded = new byte[encodedLen];
    for (int d = 0; d < _dimension; d++) {
      float normalized = (vector[d] - _minValues[d]) * _scales[d];
      int quantized = Math.round(normalized);
      quantized = Math.max(0, Math.min(15, quantized));
      int byteIdx = d / 2;
      if (d % 2 == 0) {
        encoded[byteIdx] = (byte) (quantized << 4);
      } else {
        encoded[byteIdx] |= (byte) (quantized & 0x0F);
      }
    }
    return encoded;
  }

  private float[] decodeSq4(byte[] encoded) {
    int expectedLen = (_dimension + 1) / 2;
    Preconditions.checkArgument(encoded.length == expectedLen,
        "Encoded length mismatch: expected %s, got %s", expectedLen, encoded.length);
    float[] decoded = new float[_dimension];
    for (int d = 0; d < _dimension; d++) {
      int byteIdx = d / 2;
      int quantized;
      if (d % 2 == 0) {
        quantized = (encoded[byteIdx] >> 4) & 0x0F;
      } else {
        quantized = encoded[byteIdx] & 0x0F;
      }
      decoded[d] = _minValues[d] + quantized / _scales[d];
    }
    return decoded;
  }

  // -----------------------------------------------------------------------
  // Internal
  // -----------------------------------------------------------------------

  private double computeEuclideanDistance(float[] query, byte[] encodedDoc) {
    validateEncodedLength(encodedDoc);
    double distance = 0.0d;
    for (int d = 0; d < _dimension; d++) {
      double decodedValue = decodeValue(encodedDoc, d);
      double diff = query[d] - decodedValue;
      distance += diff * diff;
    }
    return distance;
  }

  private double computeCosineDistance(float[] query, byte[] encodedDoc) {
    validateEncodedLength(encodedDoc);
    double dotProduct = 0.0d;
    double queryNorm = 0.0d;
    double docNorm = 0.0d;
    for (int d = 0; d < _dimension; d++) {
      double decodedValue = decodeValue(encodedDoc, d);
      dotProduct += query[d] * decodedValue;
      queryNorm += query[d] * query[d];
      docNorm += decodedValue * decodedValue;
    }
    if (queryNorm == 0.0d || docNorm == 0.0d) {
      return 1.0d;
    }
    return 1.0d - (dotProduct / (Math.sqrt(queryNorm) * Math.sqrt(docNorm)));
  }

  private double computeNegativeDotProduct(float[] query, byte[] encodedDoc) {
    validateEncodedLength(encodedDoc);
    double dotProduct = 0.0d;
    for (int d = 0; d < _dimension; d++) {
      dotProduct += query[d] * decodeValue(encodedDoc, d);
    }
    return -dotProduct;
  }

  private void validateEncodedLength(byte[] encodedDoc) {
    int expectedLen = getEncodedBytesPerVector();
    Preconditions.checkArgument(encodedDoc.length == expectedLen,
        "Encoded length mismatch: expected %s, got %s", expectedLen, encodedDoc.length);
  }

  private float decodeValue(byte[] encoded, int dimensionIndex) {
    if (_bitWidth == BitWidth.SQ8) {
      return decodeSq8Value(encoded, dimensionIndex);
    }
    return decodeSq4Value(encoded, dimensionIndex);
  }

  private float decodeSq8Value(byte[] encoded, int dimensionIndex) {
    int quantized = encoded[dimensionIndex] & 0xFF;
    return _minValues[dimensionIndex] + quantized / _scales[dimensionIndex];
  }

  private float decodeSq4Value(byte[] encoded, int dimensionIndex) {
    int encodedByte = encoded[dimensionIndex / 2] & 0xFF;
    int quantized = (dimensionIndex & 1) == 0 ? (encodedByte >>> 4) : (encodedByte & 0x0F);
    return _minValues[dimensionIndex] + quantized / _scales[dimensionIndex];
  }

  private void computeScales() {
    for (int d = 0; d < _dimension; d++) {
      float range = _maxValues[d] - _minValues[d];
      _scales[d] = range > 0 ? (float) _bitWidth.getMaxValue() / range : 0f;
    }
  }

  // -----------------------------------------------------------------------
  // Accessors
  // -----------------------------------------------------------------------

  @Override
  public VectorQuantizerType getType() {
    return _bitWidth == BitWidth.SQ8 ? VectorQuantizerType.SQ8 : VectorQuantizerType.SQ4;
  }

  @Override
  public int getDimension() {
    return _dimension;
  }

  public BitWidth getBitWidth() {
    return _bitWidth;
  }

  public float[] getMinValues() {
    return _minValues.clone();
  }

  public float[] getMaxValues() {
    return _maxValues.clone();
  }

  public boolean isTrained() {
    return _trained;
  }
}
