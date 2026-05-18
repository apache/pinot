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

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;


/**
 * Validates {@link VectorIndexConfig} for backend-specific correctness.
 *
 * <p>This validator ensures that:
 * <ul>
 *   <li>Required common fields (vectorDimension, vectorDistanceFunction) are present and valid.</li>
 *   <li>The vectorIndexType resolves to a known {@link VectorBackendType}.</li>
 *   <li>Backend-specific properties are valid for the resolved backend type.</li>
 *   <li>Properties belonging to a different backend are rejected with a clear error message.</li>
 * </ul>
 *
 * <p>Thread-safe: this class is stateless and all methods are static.</p>
 */
public final class VectorIndexConfigValidator {

  // HNSW-specific property keys
  static final Set<String> HNSW_PROPERTIES = Collections.unmodifiableSet(new HashSet<>(
      Arrays.asList("maxCon", "beamWidth", "maxDimensions", "maxBufferSizeMB",
          "useCompoundFile", "mode", "commit", "commitIntervalMs", "commitDocs")));

  // IVF_FLAT-specific property keys
  static final Set<String> IVF_FLAT_PROPERTIES = Collections.unmodifiableSet(new HashSet<>(
      Arrays.asList("nlist", "trainSampleSize", "trainingSeed", "minRowsForIndex")));

  // IVF_PQ-specific property keys
  static final Set<String> IVF_PQ_PROPERTIES = Collections.unmodifiableSet(new HashSet<>(
      Arrays.asList("nlist", "pqM", "pqNbits", "trainSampleSize", "trainingSeed")));

  // IVF_ON_DISK-specific property keys (same as IVF_FLAT; always uses FileChannel reads)
  static final Set<String> IVF_ON_DISK_PROPERTIES = Collections.unmodifiableSet(new HashSet<>(
      Arrays.asList("nlist", "trainSampleSize", "trainingSeed", "minRowsForIndex")));

  private static final Set<String> IVF_FLAT_EXCLUSIVE_PROPERTIES = Collections.unmodifiableSet(new HashSet<>(
      Collections.singletonList("minRowsForIndex")));

  private static final Set<String> IVF_PQ_EXCLUSIVE_PROPERTIES = Collections.unmodifiableSet(new HashSet<>(
      Arrays.asList("pqM", "pqNbits")));

  // Common property keys that appear in the properties map (legacy format stores common fields there too)
  private static final Set<String> COMMON_PROPERTIES = Collections.unmodifiableSet(new HashSet<>(
      Arrays.asList("vectorIndexType", "vectorDimension", "vectorDistanceFunction", "version", "quantizer")));

  private static final int DEFAULT_IVF_PQ_NLIST = 128;
  private static final int DEFAULT_IVF_PQ_PQ_NBITS = 8;
  private static final int DEFAULT_IVF_PQ_TRAIN_SAMPLE_MULTIPLIER = 40;
  private static final int DEFAULT_IVF_PQ_MIN_TRAIN_SAMPLE_SIZE = 10000;

  private VectorIndexConfigValidator() {
  }

  /**
   * Returns a properties map with recommended IVF_PQ defaults for the given dimension.
   * Users can override individual values. The returned map is mutable.
   *
   * <p>Recommended defaults:</p>
   * <ul>
   *   <li>{@code nlist}: 128 (good for 10K-1M vectors)</li>
   *   <li>{@code pqM}: dimension / 8, clamped to [1, dimension] and must divide dimension evenly</li>
   *   <li>{@code pqNbits}: 8 (256 codewords per sub-quantizer)</li>
   *   <li>{@code trainSampleSize}: max(nlist * 40, 10000)</li>
   * </ul>
   *
   * @param dimension the vector dimension
   * @param distanceFunction the distance function name (e.g., "EUCLIDEAN", "COSINE")
   * @return mutable properties map with recommended defaults
   */
  public static Map<String, String> recommendedIvfPqDefaults(int dimension, String distanceFunction) {
    int nlist = DEFAULT_IVF_PQ_NLIST;
    int pqM = findBestPqM(dimension);
    int pqNbits = DEFAULT_IVF_PQ_PQ_NBITS;
    int trainSampleSize = Math.max(nlist * DEFAULT_IVF_PQ_TRAIN_SAMPLE_MULTIPLIER,
        DEFAULT_IVF_PQ_MIN_TRAIN_SAMPLE_SIZE);

    Map<String, String> properties = new HashMap<>();
    properties.put("vectorIndexType", "IVF_PQ");
    properties.put("vectorDimension", String.valueOf(dimension));
    properties.put("vectorDistanceFunction", distanceFunction);
    properties.put("version", "1");
    properties.put("nlist", String.valueOf(nlist));
    properties.put("pqM", String.valueOf(pqM));
    properties.put("pqNbits", String.valueOf(pqNbits));
    properties.put("trainSampleSize", String.valueOf(trainSampleSize));
    return properties;
  }

  /**
   * Finds the largest pqM <= dimension/8 that evenly divides dimension.
   * Falls back to 1 if no better divisor exists.
   */
  static int findBestPqM(int dimension) {
    int target = Math.max(1, dimension / 8);
    for (int candidate = target; candidate >= 1; candidate--) {
      if (dimension % candidate == 0) {
        return candidate;
      }
    }
    return 1;
  }

  /**
   * Validates the given {@link VectorIndexConfig} for backend-specific correctness.
   *
   * @param config the config to validate
   * @throws IllegalArgumentException if validation fails
   */
  public static void validate(VectorIndexConfig config) {
    if (config.isDisabled()) {
      return;
    }

    VectorBackendType backendType = resolveBackendType(config);
    validateCommonFields(config);
    validateQuantizerProperty(config, backendType);
    validateBackendSpecificProperties(config, backendType);
  }

  /**
   * Resolves the {@link VectorBackendType} from the config. Defaults to HNSW if the
   * vectorIndexType field is null or empty, preserving backward compatibility.
   *
   * @param config the config to resolve from
   * @return the resolved backend type
   * @throws IllegalArgumentException if the vectorIndexType is not recognized
   */
  public static VectorBackendType resolveBackendType(VectorIndexConfig config) {
    String typeString = config.getVectorIndexType();
    if (typeString == null || typeString.isEmpty()) {
      return VectorBackendType.HNSW;
    }
    return VectorBackendType.fromString(typeString);
  }

  /**
   * Validates common fields shared across all backend types.
   */
  private static void validateCommonFields(VectorIndexConfig config) {
    if (config.getVectorDimension() <= 0) {
      throw new IllegalArgumentException(
          "vectorDimension must be a positive integer, got: " + config.getVectorDimension());
    }

    if (config.getVectorDistanceFunction() == null) {
      throw new IllegalArgumentException("vectorDistanceFunction is required");
    }
  }

  /**
   * Validates the optional "quantizer" property, if present, is a valid {@link VectorQuantizerType}.
   */
  private static void validateQuantizerProperty(VectorIndexConfig config, VectorBackendType backendType) {
    Map<String, String> properties = config.getProperties();
    if (properties == null) {
      return;
    }
    String quantizer = properties.get("quantizer");
    if (quantizer != null && !quantizer.isEmpty()) {
      if (!VectorQuantizerType.isValid(quantizer)) {
        throw new IllegalArgumentException(
            "Invalid quantizer type: '" + quantizer + "'. Supported types: FLAT, SQ8, SQ4, PQ");
      }
      VectorQuantizerType quantizerType = VectorQuantizerType.fromString(quantizer);
      switch (backendType) {
        case HNSW:
          if (quantizerType != VectorQuantizerType.FLAT) {
            throw new IllegalArgumentException(
                "vectorIndexType HNSW supports only quantizer='FLAT' (no quantization), got: " + quantizer);
          }
          break;
        case IVF_FLAT:
        case IVF_ON_DISK:
          if (quantizerType == VectorQuantizerType.PQ) {
            throw new IllegalArgumentException(
                "vectorIndexType " + backendType + " does not support quantizer='PQ'. "
                    + "Supported quantizers: FLAT, SQ8, SQ4");
          }
          break;
        case IVF_PQ:
          // Preserve backward compatibility: FLAT remains accepted as a no-op override.
          if (quantizerType != VectorQuantizerType.FLAT && quantizerType != VectorQuantizerType.PQ) {
            throw new IllegalArgumentException(
                "vectorIndexType IVF_PQ supports quantizer='PQ' (preferred) or 'FLAT' (legacy), got: " + quantizer);
          }
          break;
        default:
          throw new IllegalArgumentException("Unsupported vector backend type: " + backendType);
      }
    }
  }

  /**
   * Validates that the properties map only contains keys valid for the resolved backend type,
   * and that backend-specific property values are within acceptable ranges.
   */
  private static void validateBackendSpecificProperties(VectorIndexConfig config, VectorBackendType backendType) {
    Map<String, String> properties = config.getProperties();
    if (backendType == VectorBackendType.IVF_PQ) {
      validateRequiredProperties(properties, IVF_PQ_PROPERTIES, "IVF_PQ");
    }
    if (properties == null || properties.isEmpty()) {
      return;
    }

    switch (backendType) {
      case HNSW:
        validateNoForeignProperties(properties, union(IVF_FLAT_PROPERTIES, IVF_PQ_PROPERTIES), "HNSW",
            "IVF backends");
        validateHnswProperties(properties);
        break;
      case IVF_FLAT:
        validateNoForeignProperties(properties, union(HNSW_PROPERTIES, IVF_PQ_EXCLUSIVE_PROPERTIES), "IVF_FLAT",
            "other backends");
        validateIvfFlatProperties(properties);
        break;
      case IVF_PQ:
        validateNoForeignProperties(properties, union(HNSW_PROPERTIES, IVF_FLAT_EXCLUSIVE_PROPERTIES), "IVF_PQ",
            "other backends");
        validateIvfPqProperties(properties, config.getVectorDimension());
        break;
      case IVF_ON_DISK:
        validateNoForeignProperties(properties, union(HNSW_PROPERTIES, IVF_PQ_EXCLUSIVE_PROPERTIES), "IVF_ON_DISK",
            "other backends");
        validateIvfOnDiskProperties(properties);
        break;
      default:
        throw new IllegalArgumentException("Unsupported vector backend type: " + backendType);
    }
  }

  /**
   * Ensures that properties belonging to a foreign backend are not present.
   * Note: this only rejects known foreign-backend keys; arbitrary unknown keys are allowed
   * to support forward-compatible extensibility.
   */
  private static void validateNoForeignProperties(Map<String, String> properties,
      Set<String> foreignProperties, String ownType, String foreignType) {
    for (String key : properties.keySet()) {
      if (COMMON_PROPERTIES.contains(key)) {
        continue;
      }
      if (foreignProperties.contains(key)) {
        throw new IllegalArgumentException(
            "Property '" + key + "' is specific to " + foreignType
                + " and cannot be used with vectorIndexType " + ownType);
      }
    }
  }

  /**
   * Validates HNSW-specific property values.
   */
  private static void validateHnswProperties(Map<String, String> properties) {
    validatePositiveIntProperty(properties, "maxCon", "HNSW maxCon");
    validatePositiveIntProperty(properties, "beamWidth", "HNSW beamWidth");
    validatePositiveIntProperty(properties, "maxDimensions", "HNSW maxDimensions");
    validatePositiveDoubleProperty(properties, "maxBufferSizeMB", "HNSW maxBufferSizeMB");
  }

  /**
   * Validates IVF_FLAT-specific property values.
   */
  private static void validateIvfFlatProperties(Map<String, String> properties) {
    validatePositiveIntProperty(properties, "nlist", "IVF_FLAT nlist");
    validatePositiveIntProperty(properties, "trainSampleSize", "IVF_FLAT trainSampleSize");
    validatePositiveIntProperty(properties, "minRowsForIndex", "IVF_FLAT minRowsForIndex");

    // If both nlist and trainSampleSize are specified, trainSampleSize must be >= nlist
    if (properties.containsKey("nlist") && properties.containsKey("trainSampleSize")) {
      int nlist = Integer.parseInt(properties.get("nlist"));
      int trainSampleSize = Integer.parseInt(properties.get("trainSampleSize"));
      if (trainSampleSize < nlist) {
        throw new IllegalArgumentException(
            "IVF_FLAT trainSampleSize (" + trainSampleSize + ") must be >= nlist (" + nlist + ")");
      }
    }
  }

  /**
   * Validates IVF_ON_DISK-specific property values.
   */
  private static void validateIvfOnDiskProperties(Map<String, String> properties) {
    validatePositiveIntProperty(properties, "nlist", "IVF_ON_DISK nlist");
    validatePositiveIntProperty(properties, "trainSampleSize", "IVF_ON_DISK trainSampleSize");
    validatePositiveIntProperty(properties, "minRowsForIndex", "IVF_ON_DISK minRowsForIndex");

    // If both nlist and trainSampleSize are specified, trainSampleSize must be >= nlist
    if (properties.containsKey("nlist") && properties.containsKey("trainSampleSize")) {
      int nlist = Integer.parseInt(properties.get("nlist"));
      int trainSampleSize = Integer.parseInt(properties.get("trainSampleSize"));
      if (trainSampleSize < nlist) {
        throw new IllegalArgumentException(
            "IVF_ON_DISK trainSampleSize (" + trainSampleSize + ") must be >= nlist (" + nlist + ")");
      }
    }
  }

  private static void validateIvfPqProperties(Map<String, String> properties, int vectorDimension) {
    validatePositiveIntProperty(properties, "nlist", "IVF_PQ nlist");
    validatePositiveIntProperty(properties, "pqM", "IVF_PQ pqM");
    validatePositiveIntProperty(properties, "pqNbits", "IVF_PQ pqNbits");
    validatePositiveIntProperty(properties, "trainSampleSize", "IVF_PQ trainSampleSize");

    Integer pqM = parsePositiveIntProperty(properties, "pqM", "IVF_PQ pqM");
    Integer pqNbits = parsePositiveIntProperty(properties, "pqNbits", "IVF_PQ pqNbits");
    Integer nlist = parsePositiveIntProperty(properties, "nlist", "IVF_PQ nlist");
    Integer trainSampleSize = parsePositiveIntProperty(properties, "trainSampleSize", "IVF_PQ trainSampleSize");

    if (pqM != null && vectorDimension % pqM != 0) {
      throw new IllegalArgumentException(
          "IVF_PQ pqM (" + pqM + ") must evenly divide vectorDimension (" + vectorDimension + ")");
    }
    if (pqNbits != null && pqNbits != 4 && pqNbits != 6 && pqNbits != 8) {
      throw new IllegalArgumentException(
          "IVF_PQ pqNbits must be one of [4, 6, 8], got: " + pqNbits);
    }
    if (nlist != null && trainSampleSize != null && trainSampleSize < nlist) {
      throw new IllegalArgumentException(
          "IVF_PQ trainSampleSize (" + trainSampleSize + ") must be >= nlist (" + nlist + ")");
    }
  }

  /**
   * Validates that an optional property, if present, is a positive integer.
   */
  private static void validatePositiveIntProperty(Map<String, String> properties, String key, String displayName) {
    parsePositiveIntProperty(properties, key, displayName);
  }

  /**
   * Validates that an optional property, if present, is a positive double.
   */
  private static void validatePositiveDoubleProperty(Map<String, String> properties, String key, String displayName) {
    String value = properties.get(key);
    if (value == null) {
      return;
    }
    try {
      double doubleValue = Double.parseDouble(value);
      if (doubleValue <= 0) {
        throw new IllegalArgumentException(displayName + " must be a positive number, got: " + doubleValue);
      }
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException(displayName + " must be a valid number, got: '" + value + "'");
    }
  }

  private static Integer parsePositiveIntProperty(Map<String, String> properties, String key, String displayName) {
    String value = properties.get(key);
    if (value == null) {
      return null;
    }
    try {
      int intValue = Integer.parseInt(value);
      if (intValue <= 0) {
        throw new IllegalArgumentException(displayName + " must be a positive integer, got: " + intValue);
      }
      return intValue;
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException(displayName + " must be a valid integer, got: '" + value + "'");
    }
  }

  private static Set<String> union(Set<String> left, Set<String> right) {
    HashSet<String> union = new HashSet<>(left);
    union.addAll(right);
    return union;
  }

  private static void validateRequiredProperties(Map<String, String> properties, Set<String> requiredProperties,
      String backendType) {
    if (properties == null) {
      throw new IllegalArgumentException(backendType + " properties are required");
    }
    for (String requiredProperty : requiredProperties) {
      if ("trainingSeed".equals(requiredProperty)) {
        continue;
      }
      if (!properties.containsKey(requiredProperty)) {
        throw new IllegalArgumentException(
            backendType + " property '" + requiredProperty + "' is required");
      }
    }
  }
}
