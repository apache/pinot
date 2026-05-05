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
package org.apache.pinot.segment.spi.index.creator;

/**
 * Enumerates the supported vector quantizer types.
 *
 * <p>Quantizers compress or transform raw float32 vectors into a more compact encoded form.
 * The quantizer type controls how vectors are stored on disk and how approximate distances
 * are computed during search.</p>
 *
 * <ul>
 *   <li>{@link #FLAT} - Identity quantizer; stores raw float32 vectors without compression.</li>
 *   <li>{@link #SQ8} - Scalar quantization to 8-bit unsigned integers per dimension.</li>
 *   <li>{@link #SQ4} - Scalar quantization to 4-bit unsigned integers per dimension.</li>
 *   <li>{@link #PQ} - Product quantization, splitting vectors into sub-vectors and encoding
 *       each with a codebook index.</li>
 * </ul>
 */
public enum VectorQuantizerType {

  /** No quantization. Vectors are stored as raw float32 values. */
  FLAT("No quantization (identity)"),

  /** Scalar quantization to 8-bit unsigned integers. Each dimension is linearly mapped to [0, 255]. */
  SQ8("Scalar quantization to 8-bit unsigned integers"),

  /** Scalar quantization to 4-bit unsigned integers. Each dimension is linearly mapped to [0, 15]. */
  SQ4("Scalar quantization to 4-bit unsigned integers"),

  /** Product quantization. Vectors are split into sub-vectors and each is encoded via a trained codebook. */
  PQ("Product quantization (existing, migrated)");

  private final String _description;

  VectorQuantizerType(String description) {
    _description = description;
  }

  /**
   * Returns a human-readable description of this quantizer type.
   */
  public String getDescription() {
    return _description;
  }

  /**
   * Returns the number of bytes needed to encode a single vector of the given dimension.
   *
   * <ul>
   *   <li>{@code FLAT}: 4 bytes per dimension (float32).</li>
   *   <li>{@code SQ8}: 1 byte per dimension.</li>
   *   <li>{@code SQ4}: ceil(dimension / 2) bytes (4 bits per dimension, packed).</li>
   *   <li>{@code PQ}: not deterministic from dimension alone; returns -1. PQ encoding size
   *       depends on pqM and pqNbits, which are not available at the enum level.</li>
   * </ul>
   *
   * @param dimension the vector dimension (must be positive)
   * @return encoded bytes per vector, or -1 if not deterministic from dimension alone
   */
  public int getEncodedBytesPerVector(int dimension) {
    switch (this) {
      case FLAT:
        return dimension * Float.BYTES;
      case SQ8:
        return dimension;
      case SQ4:
        return (dimension + 1) / 2;
      case PQ:
        // PQ encoding size depends on pqM and pqNbits; cannot be determined from dimension alone
        return -1;
      default:
        throw new IllegalStateException("Unknown quantizer type: " + this);
    }
  }

  /**
   * Parses a quantizer type string in a case-insensitive manner.
   *
   * @param value the string to parse (e.g., "FLAT", "sq8", "PQ")
   * @return the corresponding {@link VectorQuantizerType}
   * @throws IllegalArgumentException if the value does not match any known quantizer type
   */
  public static VectorQuantizerType fromString(String value) {
    if (value == null) {
      throw new IllegalArgumentException("Vector quantizer type must not be null");
    }
    try {
      return valueOf(value.toUpperCase());
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException(
          "Unknown vector quantizer type: '" + value + "'. Supported types: FLAT, SQ8, SQ4, PQ");
    }
  }

  /**
   * Returns true if the given string is a recognized quantizer type (case-insensitive).
   *
   * @param value the string to check
   * @return true if recognized, false otherwise
   */
  public static boolean isValid(String value) {
    if (value == null) {
      return false;
    }
    try {
      valueOf(value.toUpperCase());
      return true;
    } catch (IllegalArgumentException e) {
      return false;
    }
  }
}
